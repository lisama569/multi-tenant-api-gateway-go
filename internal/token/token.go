// Package token 提供基于 HMAC-SHA256 签名的令牌签发与解析。
// 令牌格式：base64url(payload-json).base64url(hmac-signature)
package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// 解析失败的分类原因，供上层映射为对外错误码。
var (
	ErrMalformed    = errors.New("token: malformed")
	ErrBadSignature = errors.New("token: bad signature")
	ErrExpired      = errors.New("token: expired")
	ErrClaimMissing = errors.New("token: required claim missing")
)

// Claims 是令牌携带的声明。
type Claims struct {
	TenantID  string    `json:"tid"`
	Subject   string    `json:"sub"`
	IssuedAt  time.Time `json:"iat"`
	ExpiresAt time.Time `json:"exp"`
}

// Signer 使用密钥签发与解析令牌。
type Signer struct {
	secret []byte
	now    func() time.Time
}

// NewSigner 创建签名器。now 可注入以便测试。
func NewSigner(secret []byte, now func() time.Time) *Signer {
	if now == nil {
		now = time.Now
	}
	return &Signer{secret: secret, now: now}
}

func (s *Signer) mac(payload []byte) []byte {
	h := hmac.New(sha256.New, s.secret)
	h.Write(payload)
	return h.Sum(nil)
}

// Sign 签发令牌。
func (s *Signer) Sign(c Claims) (string, error) {
	payload, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("token: marshal claims: %w", err)
	}
	sig := s.mac(payload)
	enc := base64.RawURLEncoding
	return enc.EncodeToString(payload) + "." + enc.EncodeToString(sig), nil
}

// Parse 解析并校验令牌：格式 → 签名 → 必需声明 → 过期。
func (s *Signer) Parse(tok string) (Claims, error) {
	enc := base64.RawURLEncoding
	parts := strings.Split(tok, ".")
	if len(parts) != 2 {
		return Claims{}, ErrMalformed
	}
	payload, err := enc.DecodeString(parts[0])
	if err != nil {
		return Claims{}, fmt.Errorf("%w: payload decode", ErrMalformed)
	}
	sig, err := enc.DecodeString(parts[1])
	if err != nil {
		return Claims{}, fmt.Errorf("%w: signature decode", ErrMalformed)
	}
	if !hmac.Equal(sig, s.mac(payload)) {
		return Claims{}, ErrBadSignature
	}
	var c Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return Claims{}, fmt.Errorf("%w: claims decode", ErrMalformed)
	}
	if c.TenantID == "" || c.Subject == "" || c.ExpiresAt.IsZero() {
		return Claims{}, ErrClaimMissing
	}
	if !s.now().Before(c.ExpiresAt) {
		return Claims{}, ErrExpired
	}
	return c, nil
}
