package gateway

import (
	"context"
	"fmt"
	"log"
	"time"
)

// Request 是进入网关的请求。
type Request struct {
	Token    string
	TenantID string // 目标资源所属租户
	Path     string
	// MaxDuration 是单个请求的最长处理时间；<=0 表示不限制。
	// 超时后网关返回 DEADLINE_EXCEEDED（可重试），并立即归还并发额度。
	MaxDuration time.Duration
	// MaxQueueWait 是配额满时允许排队等待的最长时间；<=0 表示立即拒绝。
	MaxQueueWait time.Duration
}

// Response 是网关成功时的响应。
type Response struct {
	TenantID string
	Body     string
}

// Handler 是下游业务处理器。
type Handler func(ctx context.Context, req Request) (string, error)

// Gateway 组合鉴权、限流与审计。
type Gateway struct {
	validator *TokenValidator
	quota     *QuotaManager
	audit     AuditSink
	handler   Handler
	logger    *log.Logger
	now       func() time.Time
}

func NewGateway(v *TokenValidator, q *QuotaManager, a AuditSink, h Handler, logger *log.Logger) *Gateway {
	if logger == nil {
		logger = log.Default()
	}
	return &Gateway{validator: v, quota: q, audit: a, handler: h, logger: logger, now: time.Now}
}

// writeAudit 写入审计记录并输出判定日志；写入失败只记录日志，不改变主流程结论。
func (g *Gateway) writeAudit(rec AuditRecord) {
	g.logger.Printf("DECISION tenant=%s subject=%s path=%s decision=%s reason=%s quota=%d/%d",
		rec.TenantID, rec.Subject, rec.Path, rec.Decision, rec.Reason,
		rec.Quota.InFlight, rec.Quota.MaxConcurrent)
	if g.audit == nil {
		return
	}
	rec.Timestamp = g.now()
	if err := g.audit.Write(rec); err != nil {
		g.logger.Printf("AUDIT_WRITE_FAILED tenant=%s path=%s decision=%s err=%v",
			rec.TenantID, rec.Path, rec.Decision, Normalize(err))
	}
}

// safeCall 调用下游处理器并隔离 panic：下游不可恢复的异常被转换为
// 稳定的 UPSTREAM_FAILURE 结论，不会拖垮进程或其它租户。
func (g *Gateway) safeCall(ctx context.Context, req Request) (body string, err error) {
	defer func() {
		if r := recover(); r != nil {
			g.logger.Printf("UPSTREAM_PANIC tenant=%s path=%s panic=%v", req.TenantID, req.Path, r)
			body = ""
			err = newError(ErrCodeUpstreamFailure, "upstream handler failed",
				fmt.Errorf("panic: %v", r))
		}
	}()
	return g.handler(ctx, req)
}

// invoke 在 MaxDuration 约束下调用下游。超时返回可重试的 DEADLINE_EXCEEDED；
// 超时后下游 goroutine 可能仍在运行，但其结果会被丢弃，调用方拿到的是稳定结论。
func (g *Gateway) invoke(ctx context.Context, req Request) (string, error) {
	if req.MaxDuration <= 0 {
		return g.safeCall(ctx, req)
	}
	type result struct {
		body string
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		body, err := g.safeCall(ctx, req)
		resCh <- result{body, err}
	}()
	timer := time.NewTimer(req.MaxDuration)
	defer timer.Stop()
	select {
	case r := <-resCh:
		return r.body, r.err
	case <-timer.C:
		err := newError(ErrCodeDeadlineExceeded, "request exceeded max processing time", nil)
		err.Retryable = true
		return "", err
	case <-ctx.Done():
		return "", newError(ErrCodeRequestCancelled, "request cancelled while processing", ctx.Err())
	}
}

// Handle 是请求入口：鉴权 -> 租户隔离 -> 限流 -> 下游 -> 审计。
func (g *Gateway) Handle(ctx context.Context, req Request) (Response, error) {
	claims, err := g.validator.Validate(req.Token)
	if err != nil {
		ge := Normalize(err)
		g.writeAudit(AuditRecord{
			TenantID: req.TenantID, Path: req.Path,
			Decision: "DENY", Reason: string(ge.Code),
			Quota: g.quota.Snapshot(req.TenantID),
		})
		return Response{}, ge
	}

	if claims.TenantID != req.TenantID {
		ge := newError(ErrCodeCrossTenant, "token tenant does not match target tenant", nil)
		g.writeAudit(AuditRecord{
			TenantID: req.TenantID, Subject: claims.Subject, Path: req.Path,
			Decision: "DENY", Reason: string(ge.Code),
			Quota: g.quota.Snapshot(req.TenantID),
		})
		return Response{}, ge
	}

	release, snap, err := g.quota.AcquireWait(ctx, req.TenantID, req.MaxQueueWait)
	if err != nil {
		ge := Normalize(err)
		rec := AuditRecord{
			TenantID: req.TenantID, Subject: claims.Subject, Path: req.Path,
			Decision: "DENY", Reason: string(ge.Code),
			Quota: g.quota.Snapshot(req.TenantID),
		}
		if snap != nil {
			rec.Quota = *snap
		}
		g.writeAudit(rec)
		return Response{}, ge
	}
	defer release()

	body, herr := g.invoke(ctx, req)
	if herr != nil {
		ge := Normalize(herr)
		g.writeAudit(AuditRecord{
			TenantID: req.TenantID, Subject: claims.Subject, Path: req.Path,
			Decision: "DENY", Reason: string(ge.Code), Quota: *snap,
		})
		return Response{}, ge
	}

	g.writeAudit(AuditRecord{
		TenantID: req.TenantID, Subject: claims.Subject, Path: req.Path,
		Decision: "ALLOW", Reason: "OK", Quota: *snap,
	})
	return Response{TenantID: req.TenantID, Body: body}, nil
}
