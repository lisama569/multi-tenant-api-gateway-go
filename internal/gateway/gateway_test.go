package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type failingSink struct{ err error }

func (f failingSink) Write(rec AuditRecord) error { return f.err }

func newTestGateway(a AuditSink, h Handler, buf io.Writer) (*Gateway, *TokenValidator) {
	v := newTestValidator()
	q := NewQuotaManager(time.Millisecond)
	q.SetLimit("tenant-a", 2)
	q.SetLimit("tenant-b", 1)
	if a == nil {
		a = NewInMemoryAuditSink()
	}
	logger := log.New(buf, "", 0)
	return NewGateway(v, q, a, h, logger), v
}

func okHandler(context.Context, Request) (string, error) { return "ok", nil }

func TestGateway_CrossTenantDenied(t *testing.T) {
	var buf strings.Builder
	gw, v := newTestGateway(nil, okHandler, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-a", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	_, err := gw.Handle(context.Background(), Request{Token: tok, TenantID: "tenant-b", Path: "/x"})
	var ge *Error
	if !errors.As(err, &ge) || ge.Code != ErrCodeCrossTenant {
		t.Fatalf("want CROSS_TENANT_ACCESS, got %v", err)
	}
	t.Logf("tenant=tenant-b normalized error: code=%s msg=%q", ge.Code, ge.Message)
}

func TestGateway_ExpiredTokenDenied(t *testing.T) {
	var buf strings.Builder
	gw, v := newTestGateway(nil, okHandler, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-a", Subject: "u1", ExpiresAt: testNow.Add(-time.Second)})

	_, err := gw.Handle(context.Background(), Request{Token: tok, TenantID: "tenant-a", Path: "/x"})
	var ge *Error
	if !errors.As(err, &ge) || ge.Code != ErrCodeTokenExpired {
		t.Fatalf("want TOKEN_EXPIRED, got %v", err)
	}
	t.Logf("tenant=tenant-a normalized error: code=%s msg=%q", ge.Code, ge.Message)
}

// 下游内部异常必须归一化为 INTERNAL，不透出内部细节。
func TestGateway_DownstreamErrorNormalized(t *testing.T) {
	var buf strings.Builder
	internal := errors.New("db connection refused at 10.0.0.7:5432")
	gw, v := newTestGateway(nil, func(context.Context, Request) (string, error) {
		return "", internal
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-a", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	_, err := gw.Handle(context.Background(), Request{Token: tok, TenantID: "tenant-a", Path: "/x"})
	var ge *Error
	if !errors.As(err, &ge) || ge.Code != ErrCodeInternal {
		t.Fatalf("want INTERNAL, got %v", err)
	}
	if strings.Contains(ge.Message, "db connection") {
		t.Fatalf("internal detail leaked into message: %q", ge.Message)
	}
	if !errors.Is(err, internal) {
		t.Fatalf("cause should be preserved via Unwrap")
	}
	t.Logf("tenant=tenant-a normalized error: code=%s msg=%q (cause preserved internally)", ge.Code, ge.Message)
}

// 审计写入失败不得改变业务结论，但必须可观测（日志）。
func TestGateway_AuditFailureDoesNotChangeOutcome(t *testing.T) {
	var buf strings.Builder
	sink := failingSink{err: errors.New("disk full")}
	gw, v := newTestGateway(sink, okHandler, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-a", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	resp, err := gw.Handle(context.Background(), Request{Token: tok, TenantID: "tenant-a", Path: "/x"})
	if err != nil {
		t.Fatalf("audit failure must not fail the request: %v", err)
	}
	if resp.Body != "ok" {
		t.Fatalf("unexpected body: %q", resp.Body)
	}
	if !strings.Contains(buf.String(), "AUDIT_WRITE_FAILED") {
		t.Fatalf("audit failure must be observable, logs: %q", buf.String())
	}
	t.Logf("tenant=tenant-a decision=ALLOW audit=failed observable via log: %q", strings.TrimSpace(buf.String()))
}

// 被接受的请求必须留下含租户与判定依据的审计记录。
func TestGateway_AcceptedRequestsAreAudited(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	gw, v := newTestGateway(sink, okHandler, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-a", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	if _, err := gw.Handle(context.Background(), Request{Token: tok, TenantID: "tenant-a", Path: "/orders"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	recs := sink.Records()
	if len(recs) != 1 {
		t.Fatalf("want 1 audit record, got %d", len(recs))
	}
	r := recs[0]
	t.Logf("audit: tenant=%s subject=%s decision=%s reason=%s quota=%+v",
		r.TenantID, r.Subject, r.Decision, r.Reason, r.Quota)
	if r.TenantID != "tenant-a" || r.Subject != "u1" || r.Decision != "ALLOW" || r.Quota.MaxConcurrent != 2 {
		t.Fatalf("bad audit record: %+v", r)
	}
}

// 网关级并发：tenant-b 上限 1，并发请求不超配，判定结果可复现。
func TestGateway_ConcurrentQuotaEnforcement(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	release := make(chan struct{})
	var started atomic.Int64
	gw, v := newTestGateway(sink, func(context.Context, Request) (string, error) {
		started.Add(1)
		<-release
		return "ok", nil
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-b", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = gw.Handle(context.Background(), Request{
				Token: tok, TenantID: "tenant-b", Path: fmt.Sprintf("/job/%d", i),
			})
		}(i)
	}
	// 等第一个请求进入下游后关闭其余
	for started.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)
	close(release)
	wg.Wait()

	var allowed, quotaDenied int
	for _, err := range errs {
		if err == nil {
			allowed++
			continue
		}
		var ge *Error
		if !errors.As(err, &ge) || ge.Code != ErrCodeQuotaExceeded {
			t.Fatalf("unexpected error: %v", err)
		}
		if ge.RetryAfter <= 0 {
			t.Fatalf("quota error must carry RetryAfter")
		}
		quotaDenied++
	}
	t.Logf("tenant=tenant-b allowed=%d quotaDenied=%d snapshot=%+v",
		allowed, quotaDenied, gw.quota.Snapshot("tenant-b"))
	if allowed != 1 || quotaDenied != n-1 {
		t.Fatalf("want 1 allowed and %d quota denied, got allowed=%d denied=%d", n-1, allowed, quotaDenied)
	}
	if got := gw.quota.Snapshot("tenant-b").InFlight; got != 0 {
		t.Fatalf("leaked slots: inFlight=%d", got)
	}
	if got := len(sink.Records()); got != n {
		t.Fatalf("every request must be audited, want %d got %d", n, got)
	}
}
