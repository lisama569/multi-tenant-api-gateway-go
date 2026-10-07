package gateway

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"time"
)

// Request 是进入网关的请求。
type Request struct {
	Token       string
	TenantID    string // 目标资源所属租户
	Path        string
	Timeout     time.Duration // 单次请求最长处理时间；<=0 表示不设上限
	WaitTimeout time.Duration // 配额满时愿意排队的最长时间；<=0 表示立即拒绝
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

// writeAudit 写入审计记录；写入失败只记录日志，不改变主流程结论。
func (g *Gateway) writeAudit(rec AuditRecord) {
	if g.audit == nil {
		return
	}
	rec.Timestamp = g.now()
	if err := g.audit.Write(rec); err != nil {
		g.logger.Printf("AUDIT_WRITE_FAILED tenant=%s path=%s decision=%s err=%v",
			rec.TenantID, rec.Path, rec.Decision, Normalize(err))
	}
}

// logConclusion 输出结论日志：租户、主体、判定时刻配额占用、最终结论。
func (g *Gateway) logConclusion(tenant, subject, decision, reason string) {
	snap := g.quota.Snapshot(tenant)
	g.logger.Printf("CONCLUSION tenant=%s subject=%s decision=%s reason=%s quota={max:%d inFlight:%d waiting:%d}",
		tenant, subject, decision, reason, snap.MaxConcurrent, snap.InFlight, snap.Waiting)
}

// downstreamResult 承载 goroutine 内的下游执行结果。
type downstreamResult struct {
	body     string
	err      error
	panicked any
}

// invokeDownstream 在独立 goroutine 中执行下游，panic 在本请求边界内被恢复，
// 不会逃出 Handle、不会拖垮进程或其它租户。返回的 channel 在下游结束
// （含 panic 恢复）后关闭；超时放弃等待时，goroutine 可能仍在运行，
// 但结果会被丢弃，不影响本请求的对外结论。
func (g *Gateway) invokeDownstream(ctx context.Context, req Request) <-chan downstreamResult {
	done := make(chan downstreamResult, 1)
	go func() {
		var res downstreamResult
		defer func() {
			if p := recover(); p != nil {
				res.panicked = p
				g.logger.Printf("DOWNSTREAM_PANIC tenant=%s path=%s panic=%v stack=%s",
					req.TenantID, req.Path, p, debug.Stack())
			}
			done <- res
		}()
		res.body, res.err = g.handler(ctx, req)
	}()
	return done
}

// Handle 是请求入口：鉴权 -> 租户隔离 -> 有界限流 -> 下游(超时/崩溃隔离) -> 审计。
func (g *Gateway) Handle(ctx context.Context, req Request) (resp Response, err error) {
	claims, verr := g.validator.Validate(req.Token)
	if verr != nil {
		ge := Normalize(verr)
		g.writeAudit(AuditRecord{
			TenantID: req.TenantID, Path: req.Path,
			Decision: "DENY", Reason: string(ge.Code),
			Quota: g.quota.Snapshot(req.TenantID),
		})
		g.logConclusion(req.TenantID, "", "DENY", string(ge.Code))
		return Response{}, ge
	}
	subject := claims.Subject

	if claims.TenantID != req.TenantID {
		ge := newError(ErrCodeCrossTenant, "token tenant does not match target tenant", nil)
		g.writeAudit(AuditRecord{
			TenantID: req.TenantID, Subject: subject, Path: req.Path,
			Decision: "DENY", Reason: string(ge.Code),
			Quota: g.quota.Snapshot(req.TenantID),
		})
		g.logConclusion(req.TenantID, subject, "DENY", string(ge.Code))
		return Response{}, ge
	}

	release, snap, qerr := g.quota.AcquireWait(ctx, req.TenantID, req.WaitTimeout)
	if qerr != nil {
		ge := Normalize(qerr)
		rec := AuditRecord{
			TenantID: req.TenantID, Subject: subject, Path: req.Path,
			Decision: "DENY", Reason: string(ge.Code),
			Quota: g.quota.Snapshot(req.TenantID),
		}
		if snap != nil {
			rec.Quota = *snap
		}
		g.writeAudit(rec)
		g.logConclusion(req.TenantID, subject, "DENY", string(ge.Code))
		return Response{}, ge
	}
	// 无论超时、下游失败还是 panic，占用的并发槽位都在这里确定性归还。
	defer release()

	// 请求级最长处理时间：超时只中断本请求的下游上下文，不影响其它请求。
	downCtx := ctx
	var cancel context.CancelFunc
	if req.Timeout > 0 {
		downCtx, cancel = context.WithTimeout(ctx, req.Timeout)
	} else {
		downCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	body, herr, panicked := func() (string, error, any) {
		select {
		case res := <-g.invokeDownstream(downCtx, req):
			return res.body, res.err, res.panicked
		case <-downCtx.Done():
			// 超过请求级上限（或上游在下游执行期间取消）：立即给出结论，
			// 不等慢下游；deferred release 立即归还槽位。
			return "", downCtx.Err(), nil
		}
	}()

	decision := "ALLOW"
	reason := "OK"
	var ge *Error
	switch {
	case panicked != nil:
		decision, reason = "DENY", string(ErrCodeDownstreamUnavailable)
		ge = newError(ErrCodeDownstreamUnavailable, "downstream crashed while handling request",
			fmt.Errorf("downstream panic: %v", panicked))
		ge.Retryable = true
	case downCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil:
		// 请求自身超时（非上游取消）：稳定、可区分、可稍后重试的结论。
		decision, reason = "DENY", string(ErrCodeRequestTimeout)
		ge = newError(ErrCodeRequestTimeout, "request processing exceeded deadline", herr)
		ge.Retryable = true
		ge.RetryAfter = g.quota.retryAfter
	case herr != nil:
		decision = "DENY"
		ge = Normalize(herr)
		reason = string(ge.Code)
	default:
		resp = Response{TenantID: req.TenantID, Body: body}
	}

	g.writeAudit(AuditRecord{
		TenantID: req.TenantID, Subject: subject, Path: req.Path,
		Decision: decision, Reason: reason, Quota: *snap,
	})
	g.logConclusion(req.TenantID, subject, decision, reason)
	if ge != nil {
		return Response{}, ge
	}
	return resp, nil
}
