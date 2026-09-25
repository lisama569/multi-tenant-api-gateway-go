package gateway

import (
	"errors"
	"testing"
	"time"
)

var testNow = time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)

func newTestValidator() *TokenValidator {
	return NewTokenValidator([]byte("test-secret"), func() time.Time { return testNow })
}

func codeOf(t *testing.T, err error) ErrorCode {
	t.Helper()
	var ge *Error
	if !errors.As(err, &ge) {
		t.Fatalf("expected *Error, got %T (%v)", err, err)
	}
	t.Logf("normalized error: code=%s msg=%q", ge.Code, ge.Message)
	return ge.Code
}

func TestValidate_ValidToken(t *testing.T) {
	v := newTestValidator()
	tok := v.Mint(Claims{TenantID: "tenant-a", Subject: "u1", IssuedAt: testNow, ExpiresAt: testNow.Add(time.Minute)})
	claims, err := v.Validate(tok)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if claims.TenantID != "tenant-a" || claims.Subject != "u1" {
		t.Fatalf("unexpected claims: %+v", claims)
	}
}

func TestValidate_Missing(t *testing.T) {
	v := newTestValidator()
	if got := codeOf(t, mustErr(v.Validate(""))); got != ErrCodeTokenMissing {
		t.Fatalf("want %s, got %s", ErrCodeTokenMissing, got)
	}
}

func TestValidate_Malformed(t *testing.T) {
	v := newTestValidator()
	for _, tok := range []string{"abc", "a.b.c", "!!!.@@@", "a.b"} {
		if got := codeOf(t, mustErr(v.Validate(tok))); got != ErrCodeTokenMalformed {
			t.Fatalf("token %q: want %s, got %s", tok, ErrCodeTokenMalformed, got)
		}
	}
}

func TestValidate_InvalidSignature(t *testing.T) {
	v := newTestValidator()
	other := NewTokenValidator([]byte("wrong-secret"), nil)
	tok := other.Mint(Claims{TenantID: "tenant-a", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})
	if got := codeOf(t, mustErr(v.Validate(tok))); got != ErrCodeTokenInvalidSignature {
		t.Fatalf("want %s, got %s", ErrCodeTokenInvalidSignature, got)
	}
}

func TestValidate_Expired(t *testing.T) {
	v := newTestValidator()
	tok := v.Mint(Claims{TenantID: "tenant-a", Subject: "u1", IssuedAt: testNow.Add(-2 * time.Hour), ExpiresAt: testNow.Add(-time.Hour)})
	if got := codeOf(t, mustErr(v.Validate(tok))); got != ErrCodeTokenExpired {
		t.Fatalf("want %s, got %s", ErrCodeTokenExpired, got)
	}
}

func TestValidate_ExpiredBoundary(t *testing.T) {
	v := newTestValidator()
	// exp == now 视为过期（now 必须严格早于 exp）
	tok := v.Mint(Claims{TenantID: "tenant-a", Subject: "u1", ExpiresAt: testNow})
	if got := codeOf(t, mustErr(v.Validate(tok))); got != ErrCodeTokenExpired {
		t.Fatalf("boundary: want %s, got %s", ErrCodeTokenExpired, got)
	}
	// exp == now+1s 仍有效
	tok = v.Mint(Claims{TenantID: "tenant-a", Subject: "u1", ExpiresAt: testNow.Add(time.Second)})
	if _, err := v.Validate(tok); err != nil {
		t.Fatalf("boundary: unexpected error: %v", err)
	}
}

func TestValidate_MissingClaim(t *testing.T) {
	v := newTestValidator()
	tok := v.Mint(Claims{TenantID: "", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})
	if got := codeOf(t, mustErr(v.Validate(tok))); got != ErrCodeTokenMissingClaim {
		t.Fatalf("want %s, got %s", ErrCodeTokenMissingClaim, got)
	}
}

func mustErr(_ any, err error) error {
	if err == nil {
		panic("expected error")
	}
	return err
}
