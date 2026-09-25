package gateway

import (
	"context"
	"log"
	"time"
)

// Request 是进入网关的请求。
type Request struct {
	Token    string
	TenantID string // 目标资源所属租户
	Path     string
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

	release, snap, err := g.quota.Acquire(req.TenantID)
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

	body, herr := g.handler(ctx, req)
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
