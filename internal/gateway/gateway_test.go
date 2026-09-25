package gateway_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/multi-tenant-api-gateway-go/internal/audit"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/auth"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/gateway"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/gwerrors"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/quota"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/token"
)

var testNow = time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)

func newSigner() *token.Signer {
	return token.NewSigner([]byte("test-secret"), func() time.Time { return testNow })
}

func mustSign(t *testing.T, s *token.Signer, c token.Claims) string {
	t.Helper()
	tok, err := s.Sign(c)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return tok
}

func validClaims(tenant string) token.Claims {
	return token.Claims{TenantID: tenant, Subject: "user-" + tenant,
		IssuedAt: testNow, ExpiresAt: testNow.Add(time.Hour)}
}

func newGateway(quotas map[string]int, rec *audit.Recorder) (*gateway.Gateway, *token.Signer) {
	s := newSigner()
	return gateway.New(auth.NewAuthenticator(s), quota.NewLimiter(quotas), rec), s
}

// codeOf 断言错误为 *gwerrors.Error 并返回错误码。
func codeOf(t *testing.T, err error) gwerrors.Code {
	t.Helper()
	var ge *gwerrors.Error
	if !errors.As(err, &ge) {
		t.Fatalf("error is not normalized *gwerrors.Error: %T %v", err, err)
	}
	return ge.Code
}

// TestAuthFailureCodesDistinct 覆盖各类令牌失败，断言错误码两两可区分。
func TestAuthFailureCodesDistinct(t *testing.T) {
	s := newSigner()
	other := token.NewSigner([]byte("wrong-secret"), func() time.Time { return testNow })

	expired := mustSign(t, s, token.Claims{TenantID: "t1", Subject: "u",
		IssuedAt: testNow.Add(-2 * time.Hour), ExpiresAt: testNow.Add(-time.Hour)})
	badSig := mustSign(t, other, validClaims("t1"))
	noTenant := mustSign(t, s, token.Claims{Subject: "u",
		IssuedAt: testNow, ExpiresAt: testNow.Add(time.Hour)})

	cases := map[string]struct {
		token string
		want  gwerrors.Code
	}{
		"missing":    {"", gwerrors.CodeTokenMissing},
		"malformed":  {"not-a-token", gwerrors.CodeTokenMalformed},
		"bad-sig":    {badSig, gwerrors.CodeTokenBadSig},
		"expired":    {expired, gwerrors.CodeTokenExpired},
		"no-claim":   {noTenant, gwerrors.CodeClaimMissing},
		"valid-diff": {mustSign(t, s, validClaims("t1")), ""},
	}

	gw, _ := newGateway(map[string]int{"t1": 4}, audit.NewRecorder(audit.NewMemorySink()))
	seen := map[gwerrors.Code]bool{}
	for name, tc := range cases {
		_, err := gw.Handle(context.Background(), gateway.Request{
			Token: tc.token, TargetTenant: "t1", Payload: name})
		if tc.want == "" {
			if err != nil {
				t.Fatalf("%s: expected success, got %v", name, err)
			}
			continue
		}
		code := codeOf(t, err)
		if code != tc.want {
			t.Fatalf("%s: got code %s, want %s", name, code, tc.want)
		}
		if seen[code] {
			t.Fatalf("code %s not distinguishable (duplicated)", code)
		}
		seen[code] = true
		t.Logf("case=%-10s tenant=t1 normalized_error=%s", name, code)
	}
}

// TestCrossTenantForbidden 越权访问其它租户资源必须被拒绝。
func TestCrossTenantForbidden(t *testing.T) {
	gw, s := newGateway(map[string]int{"ta": 2, "tb": 2}, audit.NewRecorder(audit.NewMemorySink()))
	tok := mustSign(t, s, validClaims("ta"))

	_, err := gw.Handle(context.Background(), gateway.Request{
		Token: tok, TargetTenant: "tb", Payload: "steal"})
	if code := codeOf(t, err); code != gwerrors.CodeTenantForbidden {
		t.Fatalf("got %s, want TENANT_FORBIDDEN", code)
	}
	t.Logf("tenant=ta target=tb normalized_error=%s", gwerrors.CodeTenantForbidden)

	// 同租户访问应成功。
	if _, err := gw.Handle(context.Background(), gateway.Request{
		Token: tok, TargetTenant: "ta", Payload: "mine"}); err != nil {
		t.Fatalf("same-tenant access rejected: %v", err)
	}
}

// TestDownstreamErrorNormalized 下游错误与 panic 必须归一化，内部细节不外泄。
func TestDownstreamErrorNormalized(t *testing.T) {
	sink := audit.NewMemorySink()
	gw, s := newGateway(map[string]int{"t1": 4}, audit.NewRecorder(sink))
	tok := mustSign(t, s, validClaims("t1"))

	internal := errors.New("db connection refused at 10.0.0.7:5432")
	gw.WithHandler(func(context.Context, auth.Context, gateway.Request) (string, error) {
		return "", internal
	})
	_, err := gw.Handle(context.Background(), gateway.Request{Token: tok, TargetTenant: "t1"})
	ge := &gwerrors.Error{}
	errors.As(err, &ge)
	if ge.Code != gwerrors.CodeInternal || !ge.Retryable {
		t.Fatalf("got %+v, want INTERNAL retryable", ge)
	}
	if errors.Is(err, internal) || ge.Message == internal.Error() {
		t.Fatalf("internal error leaked: %v", ge.Message)
	}
	t.Logf("tenant=t1 normalized_error=%s message=%q (internal detail hidden)", ge.Code, ge.Message)

	gw.WithHandler(func(context.Context, auth.Context, gateway.Request) (string, error) {
		panic("boom")
	})
	if _, err := gw.Handle(context.Background(), gateway.Request{Token: tok, TargetTenant: "t1"}); codeOf(t, err) != gwerrors.CodeInternal {
		t.Fatalf("panic not normalized: %v", err)
	}
}

// failingSink 总是写入失败的审计目标。
type failingSink struct{}

func (failingSink) Write(audit.Record) error { return errors.New("disk full") }

// TestAuditWriteFailureDoesNotChangeOutcome 审计失败不改变业务结论，但可观测。
func TestAuditWriteFailureDoesNotChangeOutcome(t *testing.T) {
	rec := audit.NewRecorder(failingSink{})
	gw, s := newGateway(map[string]int{"t1": 2}, rec)
	tok := mustSign(t, s, validClaims("t1"))

	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := gw.Handle(context.Background(), gateway.Request{
				Token: tok, TargetTenant: "t1"}); err != nil {
				t.Errorf("business outcome changed by audit failure: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := rec.WriteFailures(); got != n {
		t.Fatalf("write failures = %d, want %d (must be observable)", got, n)
	}
	t.Logf("tenant=t1 accepted=%d audit_write_failures=%d (observable)", n, rec.WriteFailures())
}

// TestAcceptedRequestsAudited 每个被接受的请求都留有含租户与判定依据的审计记录。
func TestAcceptedRequestsAudited(t *testing.T) {
	sink := audit.NewMemorySink()
	gw, s := newGateway(map[string]int{"t1": 8}, audit.NewRecorder(sink))
	tok := mustSign(t, s, validClaims("t1"))

	const n = 5
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := gw.Handle(context.Background(), gateway.Request{
				Token: tok, TargetTenant: "t1", Payload: fmt.Sprint(i)}); err != nil {
				t.Errorf("rejected: %v", err)
			}
		}(i)
	}
	wg.Wait()

	recs := sink.Records()
	if len(recs) != n {
		t.Fatalf("audit records = %d, want %d", len(recs), n)
	}
	for _, r := range recs {
		if r.TenantID != "t1" || r.Decision != "accepted" || r.Reason != "OK" || r.Quota == "" {
			t.Fatalf("bad audit record: %+v", r)
		}
		t.Logf("audit tenant=%s decision=%s reason=%s quota_snapshot=%s",
			r.TenantID, r.Decision, r.Reason, r.Quota)
	}
}
