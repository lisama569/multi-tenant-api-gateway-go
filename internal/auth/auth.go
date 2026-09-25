// Package auth 负责基于令牌的租户鉴权与隔离校验。
package auth

import (
	"errors"

	"github.com/lisama569/multi-tenant-api-gateway-go/internal/gwerrors"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/token"
)

// Context 是鉴权通过后注入请求的租户上下文。
type Context struct {
	TenantID string
	Subject  string
}

// Authenticator 校验令牌并强制租户隔离。
type Authenticator struct {
	signer *token.Signer
}

// NewAuthenticator 创建鉴权器。
func NewAuthenticator(signer *token.Signer) *Authenticator {
	return &Authenticator{signer: signer}
}

// Authenticate 校验令牌，返回租户上下文；失败返回 *gwerrors.Error，
// 错误码可区分：缺失 / 格式非法 / 签名非法 / 已过期 / 缺少必需声明。
func (a *Authenticator) Authenticate(tok string) (Context, error) {
	if tok == "" {
		return Context{}, gwerrors.New(gwerrors.CodeTokenMissing, "token is required", false, nil)
	}
	claims, err := a.signer.Parse(tok)
	if err != nil {
		switch {
		case errors.Is(err, token.ErrMalformed):
			return Context{}, gwerrors.New(gwerrors.CodeTokenMalformed, "token is malformed", false, err)
		case errors.Is(err, token.ErrBadSignature):
			return Context{}, gwerrors.New(gwerrors.CodeTokenBadSig, "token signature invalid", false, err)
		case errors.Is(err, token.ErrExpired):
			return Context{}, gwerrors.New(gwerrors.CodeTokenExpired, "token expired", false, err)
		case errors.Is(err, token.ErrClaimMissing):
			return Context{}, gwerrors.New(gwerrors.CodeClaimMissing, "token missing required claims", false, err)
		default:
			return Context{}, gwerrors.New(gwerrors.CodeInternal, "authentication failed", false, err)
		}
	}
	return Context{TenantID: claims.TenantID, Subject: claims.Subject}, nil
}

// AuthorizeTenant 校验已鉴权上下文是否有权访问目标租户资源（越权拒绝）。
func AuthorizeTenant(ctx Context, targetTenant string) error {
	if targetTenant == "" || ctx.TenantID != targetTenant {
		return gwerrors.New(gwerrors.CodeTenantForbidden,
			"cross-tenant access denied", false, nil)
	}
	return nil
}
