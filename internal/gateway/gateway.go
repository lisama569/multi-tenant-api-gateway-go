// Package gateway 编排鉴权、限流、审计与错误归一化，是请求入口。
package gateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/lisama569/multi-tenant-api-gateway-go/internal/audit"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/auth"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/gwerrors"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/quota"
)

// Request 是进入网关的请求。
type Request struct {
	Token        string
	TargetTenant string // 目标资源所属租户
	Payload      string
}

// Response 是网关响应。
type Response struct {
	Body string
}

// Handler 是下游业务处理器。
type Handler func(ctx context.Context, ac auth.Context, req Request) (string, error)

// Gateway 是请求入口。
type Gateway struct {
	auth    *auth.Authenticator
	limiter *quota.Limiter
	audit   *audit.Recorder
	handler Handler // 为 nil 时使用内置回显处理器
}

// New 创建网关。
func New(a *auth.Authenticator, l *quota.Limiter, rec *audit.Recorder) *Gateway {
	return &Gateway{auth: a, limiter: l, audit: rec}
}

// WithHandler 设置下游业务处理器，返回网关自身以便链式调用。
func (g *Gateway) WithHandler(h Handler) *Gateway {
	g.handler = h
	return g
}

// normalize 将任意错误归一化为稳定的对外错误类型。
// 下游/内部异常不保留 cause 链，确保内部细节无法经 errors.Is/As 透出。
func normalize(err error) *gwerrors.Error {
	var ge *gwerrors.Error
	if errors.As(err, &ge) {
		return ge
	}
	return gwerrors.New(gwerrors.CodeInternal, "internal error", true, nil)
}

// Handle 处理一个请求：鉴权 → 隔离校验 → 限流 → 下游 → 审计。
// 返回的错误一律为 *gwerrors.Error。
func (g *Gateway) Handle(ctx context.Context, req Request) (resp Response, err error) {
	ac, terr := g.auth.Authenticate(req.Token)
	tenant := ac.TenantID
	if terr != nil {
		ge := normalize(terr)
		g.audit.Log(audit.Record{TenantID: tenant, Decision: "rejected", Reason: string(ge.Code)})
		return Response{}, ge
	}

	if terr := auth.AuthorizeTenant(ac, req.TargetTenant); terr != nil {
		ge := normalize(terr)
		g.audit.Log(audit.Record{TenantID: tenant, Decision: "rejected", Reason: string(ge.Code)})
		return Response{}, ge
	}

	release, qerr := g.limiter.Acquire(tenant)
	if qerr != nil {
		ge := normalize(qerr)
		used, limit := g.limiter.Snapshot(tenant)
		g.audit.Log(audit.Record{
			TenantID: tenant, Decision: "rejected", Reason: string(ge.Code),
			Quota: fmt.Sprintf("%d/%d", used, limit),
		})
		return Response{}, ge
	}
	defer release()

	body, herr := callHandler(ctx, g.handlerOrDefault(), ac, req)
	used, limit := g.limiter.Snapshot(tenant)
	quotaSnap := fmt.Sprintf("%d/%d", used, limit)
	if herr != nil {
		ge := normalize(herr)
		g.audit.Log(audit.Record{TenantID: tenant, Decision: "rejected", Reason: string(ge.Code), Quota: quotaSnap})
		return Response{}, ge
	}

	g.audit.Log(audit.Record{TenantID: tenant, Decision: "accepted", Reason: "OK", Quota: quotaSnap})
	return Response{Body: body}, nil
}

// callHandler 调用下游处理器，panic 归一化为内部错误，不外泄。
func callHandler(ctx context.Context, h Handler, ac auth.Context, req Request) (body string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = gwerrors.New(gwerrors.CodeInternal, "internal error", true,
				fmt.Errorf("handler panic: %v", r))
		}
	}()
	return h(ctx, ac, req)
}

func (g *Gateway) handlerOrDefault() Handler {
	if g.handler != nil {
		return g.handler
	}
	return func(_ context.Context, ac auth.Context, req Request) (string, error) {
		return fmt.Sprintf("ok: tenant=%s sub=%s payload=%s", ac.TenantID, ac.Subject, req.Payload), nil
	}
}
