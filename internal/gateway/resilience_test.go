package gateway

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// 超时：下游处理超过 MaxDuration 时，网关返回可重试的 DEADLINE_EXCEEDED，
// 且并发额度在 Handle 返回时即已归还（不等下游 goroutine 收尾）。
func TestGateway_DeadlineExceeded(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	entered := make(chan struct{})
	unblock := make(chan struct{})
	gw, v := newTestGateway(sink, func(context.Context, Request) (string, error) {
		close(entered)
		<-unblock
		return "late", nil
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-a", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	start := time.Now()
	_, err := gw.Handle(context.Background(), Request{
		Token: tok, TenantID: "tenant-a", Path: "/slow", MaxDuration: 20 * time.Millisecond,
	})
	elapsed := time.Since(start)

	var ge *Error
	if !errors.As(err, &ge) || ge.Code != ErrCodeDeadlineExceeded {
		t.Fatalf("want DEADLINE_EXCEEDED, got %v", err)
	}
	if !ge.Retryable {
		t.Fatalf("deadline exceeded must be marked retryable")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("gateway must return promptly after deadline, took %v", elapsed)
	}
	// 下游 goroutine 仍阻塞，但额度必须已经归还
	if got := gw.quota.Snapshot("tenant-a").InFlight; got != 0 {
		t.Fatalf("slot must be released on timeout, inFlight=%d", got)
	}

	recs := sink.Records()
	if len(recs) != 1 {
		t.Fatalf("want 1 audit record, got %d", len(recs))
	}
	r := recs[0]
	t.Logf("audit: tenant=%s decision=%s reason=%s quota=%+v", r.TenantID, r.Decision, r.Reason, r.Quota)
	if r.TenantID != "tenant-a" || r.Decision != "DENY" || r.Reason != string(ErrCodeDeadlineExceeded) {
		t.Fatalf("bad audit record: %+v", r)
	}
	if !strings.Contains(buf.String(), "tenant=tenant-a") || !strings.Contains(buf.String(), "reason=DEADLINE_EXCEEDED") {
		t.Fatalf("decision log must show tenant and reason, logs: %q", buf.String())
	}
	close(unblock) // 放行下游 goroutine，避免泄漏
}

// 下游 panic：被隔离为稳定的 UPSTREAM_FAILURE，进程不挂、其它租户不受牵连，
// 额度与审计正常收尾。
func TestGateway_UpstreamPanicIsolated(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	gw, v := newTestGateway(sink, func(_ context.Context, req Request) (string, error) {
		if req.Path == "/boom" {
			panic("nil pointer in downstream handler")
		}
		return "ok", nil
	}, &buf)
	tokA := v.Mint(Claims{TenantID: "tenant-a", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})
	tokB := v.Mint(Claims{TenantID: "tenant-b", Subject: "u2", ExpiresAt: testNow.Add(time.Minute)})

	_, err := gw.Handle(context.Background(), Request{Token: tokA, TenantID: "tenant-a", Path: "/boom"})
	var ge *Error
	if !errors.As(err, &ge) || ge.Code != ErrCodeUpstreamFailure {
		t.Fatalf("want UPSTREAM_FAILURE, got %v", err)
	}
	if strings.Contains(ge.Message, "nil pointer") {
		t.Fatalf("panic detail must not leak into message: %q", ge.Message)
	}
	if got := gw.quota.Snapshot("tenant-a").InFlight; got != 0 {
		t.Fatalf("slot must be released after panic, inFlight=%d", got)
	}

	// 其它租户的正常请求不受牵连
	resp, err := gw.Handle(context.Background(), Request{Token: tokB, TenantID: "tenant-b", Path: "/ok"})
	if err != nil || resp.Body != "ok" {
		t.Fatalf("other tenant must not be affected: resp=%+v err=%v", resp, err)
	}

	recs := sink.Records()
	if len(recs) != 2 {
		t.Fatalf("both requests must be audited, got %d", len(recs))
	}
	t.Logf("audit[0]: tenant=%s decision=%s reason=%s quota=%+v",
		recs[0].TenantID, recs[0].Decision, recs[0].Reason, recs[0].Quota)
	if recs[0].TenantID != "tenant-a" || recs[0].Decision != "DENY" || recs[0].Reason != string(ErrCodeUpstreamFailure) {
		t.Fatalf("panic must be audited to the failing tenant: %+v", recs[0])
	}
	if recs[1].TenantID != "tenant-b" || recs[1].Decision != "ALLOW" {
		t.Fatalf("other tenant request must be audited as ALLOW: %+v", recs[1])
	}
	if !strings.Contains(buf.String(), "UPSTREAM_PANIC tenant=tenant-a") {
		t.Fatalf("panic must be observable with tenant, logs: %q", buf.String())
	}
	if got := gw.quota.Snapshot("tenant-b").InFlight; got != 0 {
		t.Fatalf("tenant-b slot leaked: inFlight=%d", got)
	}
}

// 并发下混合超时与 panic：额度最终回到干净状态，审计一条不少。
func TestGateway_MixedFailuresLeaveCleanState(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	gw, v := newTestGateway(sink, func(_ context.Context, req Request) (string, error) {
		switch req.Path {
		case "/panic":
			panic("boom")
		case "/slow":
			time.Sleep(200 * time.Millisecond)
			return "late", nil
		default:
			return "ok", nil
		}
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-a", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	const n = 12 // 4 ok + 4 panic + 4 timeout
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			path := "/ok"
			if i%3 == 1 {
				path = "/panic"
			} else if i%3 == 2 {
				path = "/slow"
			}
			_, errs[i] = gw.Handle(context.Background(), Request{
				Token: tok, TenantID: "tenant-a", Path: path,
				MaxDuration: 30 * time.Millisecond, MaxQueueWait: 5 * time.Second,
			})
		}(i)
	}
	wg.Wait()

	var ok, panics, timeouts int
	for _, err := range errs {
		var ge *Error
		switch {
		case err == nil:
			ok++
		case errors.As(err, &ge) && ge.Code == ErrCodeUpstreamFailure:
			panics++
		case errors.As(err, &ge) && ge.Code == ErrCodeDeadlineExceeded:
			timeouts++
		default:
			t.Fatalf("unexpected outcome: %v", err)
		}
	}
	t.Logf("tenant=tenant-a ok=%d panic=%d timeout=%d snapshot=%+v",
		ok, panics, timeouts, gw.quota.Snapshot("tenant-a"))
	if ok != 4 || panics != 4 || timeouts != 4 {
		t.Fatalf("want 4/4/4 outcomes, got ok=%d panic=%d timeout=%d", ok, panics, timeouts)
	}
	if got := gw.quota.Snapshot("tenant-a").InFlight; got != 0 {
		t.Fatalf("slots leaked after mixed failures: inFlight=%d", got)
	}
	if got := len(sink.Records()); got != n {
		t.Fatalf("every request must be audited, want %d got %d", n, got)
	}
}
