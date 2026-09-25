package gateway

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
)

// Claims 是令牌中携带的声明。
type Claims struct {
	TenantID  string    `json:"tenant_id"`
	Subject   string    `json:"sub"`
	ExpiresAt time.Time `json:"exp"`
	IssuedAt  time.Time `json:"iat"`
}

type claimsJSON struct {
	TenantID  string `json:"tenant_id"`
	Subject   string `json:"sub"`
	ExpiresAt int64  `json:"exp"`
	IssuedAt  int64  `json:"iat"`
}

// TokenValidator 校验令牌签名、格式与声明。now 可注入以保证判定可复现。
type TokenValidator struct {
	secret []byte
	now    func() time.Time
}

func NewTokenValidator(secret []byte, now func() time.Time) *TokenValidator {
	if now == nil {
		now = time.Now
	}
	return &TokenValidator{secret: secret, now: now}
}

func (v *TokenValidator) sign(payload []byte) []byte {
	mac := hmac.New(sha256.New, v.secret)
	mac.Write(payload)
	return mac.Sum(nil)
}

// Mint 生成一个合法令牌（供测试与本地演示使用）。
func (v *TokenValidator) Mint(c Claims) string {
	payload, _ := json.Marshal(claimsJSON{
		TenantID:  c.TenantID,
		Subject:   c.Subject,
		ExpiresAt: c.ExpiresAt.Unix(),
		IssuedAt:  c.IssuedAt.Unix(),
	})
	enc := base64.RawURLEncoding
	p := enc.EncodeToString(payload)
	s := enc.EncodeToString(v.sign(payload))
	return p + "." + s
}

// Validate 解析并校验令牌，返回声明或可区分的失败原因。
func (v *TokenValidator) Validate(token string) (Claims, error) {
	if token == "" {
		return Claims{}, newError(ErrCodeTokenMissing, "token is required", nil)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return Claims{}, newError(ErrCodeTokenMalformed, "token must have payload and signature segments", nil)
	}
	enc := base64.RawURLEncoding
	payload, err := enc.DecodeString(parts[0])
	if err != nil {
		return Claims{}, newError(ErrCodeTokenMalformed, "payload is not valid base64url", err)
	}
	sig, err := enc.DecodeString(parts[1])
	if err != nil {
		return Claims{}, newError(ErrCodeTokenMalformed, "signature is not valid base64url", err)
	}
	if !hmac.Equal(sig, v.sign(payload)) {
		return Claims{}, newError(ErrCodeTokenInvalidSignature, "signature mismatch", nil)
	}
	var cj claimsJSON
	if err := json.Unmarshal(payload, &cj); err != nil {
		return Claims{}, newError(ErrCodeTokenMalformed, "payload is not valid JSON", err)
	}
	if cj.TenantID == "" || cj.Subject == "" || cj.ExpiresAt == 0 {
		return Claims{}, newError(ErrCodeTokenMissingClaim, "token requires tenant_id, sub and exp claims", nil)
	}
	claims := Claims{
		TenantID:  cj.TenantID,
		Subject:   cj.Subject,
		ExpiresAt: time.Unix(cj.ExpiresAt, 0),
		IssuedAt:  time.Unix(cj.IssuedAt, 0),
	}
	if !v.now().Before(claims.ExpiresAt) {
		return Claims{}, newError(ErrCodeTokenExpired, "token has expired", nil)
	}
	return claims, nil
}
