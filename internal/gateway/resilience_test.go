package gateway

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 下游超出请求级最长处理时间：返回稳定可区分、可重试的 REQUEST_TIMEOUT，
// 审计一条不少，占用的并发槽位及时归还。
func TestGateway_RequestTimeoutReleasesSlot(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	var active atomic.Int64
	releaseDownstream := make(chan struct{})
	gw, v := newTestGateway(sink, func(ctx context.Context, req Request) (string, error) {
		active.Add(1)
		defer active.Add(-1)
		select {
		case <-releaseDownstream:
			return "late", nil
		case <-ctx.Done():
			<-releaseDownstream // 模拟即便被取消也要耗一会的下游
			return "", ctx.Err()
		}
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-b", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	start := time.Now()
	_, err := gw.Handle(context.Background(), Request{
		Token: tok, TenantID: "tenant-b", Path: "/slow",
		Timeout: 30 * time.Millisecond,
	})
	elapsed := time.Since(start)

	var ge *Error
	if !errors.As(err, &ge) || ge.Code != ErrCodeRequestTimeout {
		t.Fatalf("want REQUEST_TIMEOUT, got %v", err)
	}
	if !ge.Retryable || ge.RetryAfter <= 0 {
		t.Fatalf("timeout must be retryable with RetryAfter, got retryable=%v retryAfter=%v",
			ge.Retryable, ge.RetryAfter)
	}
	if elapsed >= 200*time.Millisecond {
		t.Fatalf("gateway must return at deadline, not wait for downstream: %v", elapsed)
	}

	// 槽位在结论给出时已归还：同租户后续请求不受阻挡。
	if got := gw.quota.Snapshot("tenant-b").InFlight; got != 0 {
		t.Fatalf("slot leaked after timeout: inFlight=%d", got)
	}
	close(releaseDownstream)

	recs := sink.Records()
	if len(recs) != 1 {
		t.Fatalf("want exactly 1 audit record, got %d", len(recs))
	}
	if recs[0].Decision != "DENY" || recs[0].Reason != "REQUEST_TIMEOUT" {
		t.Fatalf("bad audit record: %+v", recs[0])
	}
	if !strings.Contains(buf.String(), "tenant=tenant-b") ||
		!strings.Contains(buf.String(), "reason=REQUEST_TIMEOUT") {
		t.Fatalf("conclusion log must name tenant and reason, logs: %q", buf.String())
	}
	t.Logf("tenant=tenant-b decision=DENY reason=REQUEST_TIMEOUT elapsed=%v snap=%+v log=%q",
		elapsed, recs[0].Quota, strings.TrimSpace(buf.String()))
}

// 超时释放的槽位必须能立刻服务同租户的后续请求。
func TestGateway_AfterTimeoutSlotIsUsableAgain(t *testing.T) {
	var buf strings.Builder
	releaseSlow := make(chan struct{})
	gw, v := newTestGateway(nil, func(ctx context.Context, req Request) (string, error) {
		if req.Path == "/slow" {
			<-releaseSlow
		}
		return "ok", nil
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-b", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	_, err := gw.Handle(context.Background(), Request{
		Token: tok, TenantID: "tenant-b", Path: "/slow", Timeout: 20 * time.Millisecond,
	})
	var ge *Error
	if !errors.As(err, &ge) || ge.Code != ErrCodeRequestTimeout {
		t.Fatalf("want REQUEST_TIMEOUT, got %v", err)
	}
	close(releaseSlow)

	// 紧跟一个正常请求，应立即拿到刚归还的槽位。
	done := make(chan error, 1)
	go func() {
		_, e := gw.Handle(context.Background(), Request{
			Token: tok, TenantID: "tenant-b", Path: "/fast",
		})
		done <- e
	}()
	select {
	case e := <-done:
		if e != nil {
			t.Fatalf("follow-up request must acquire released slot, got %v", e)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("follow-up request blocked: timeout slot was not released")
	}
	if got := gw.quota.Snapshot("tenant-b").InFlight; got != 0 {
		t.Fatalf("final inFlight must be 0, got %d", got)
	}
}

// 下游 panic 必须被隔离在单请求边界：对外是稳定的 DOWNSTREAM_UNAVAILABLE，
// 进程不崩，细节不透出，额度归还，审计正常收尾。
func TestGateway_DownstreamPanicIsIsolated(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	gw, v := newTestGateway(sink, func(context.Context, Request) (string, error) {
		panic("nil pointer dereference at orderService.go:88 secret=/etc/key")
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-a", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	_, err := gw.Handle(context.Background(), Request{Token: tok, TenantID: "tenant-a", Path: "/boom"})
	var ge *Error
	if !errors.As(err, &ge) || ge.Code != ErrCodeDownstreamUnavailable {
		t.Fatalf("want DOWNSTREAM_UNAVAILABLE, got %v", err)
	}
	if !ge.Retryable {
		t.Fatalf("crash conclusion should be retryable")
	}
	if strings.Contains(ge.Message, "secret=") || strings.Contains(ge.Message, "orderService") {
		t.Fatalf("panic detail leaked: %q", ge.Message)
	}
	if got := gw.quota.Snapshot("tenant-a").InFlight; got != 0 {
		t.Fatalf("slot leaked after panic: inFlight=%d", got)
	}
	recs := sink.Records()
	if len(recs) != 1 || recs[0].Reason != "DOWNSTREAM_UNAVAILABLE" || recs[0].Decision != "DENY" {
		t.Fatalf("bad audit after panic: %+v", recs)
	}
	logs := buf.String()
	if !strings.Contains(logs, "DOWNSTREAM_PANIC") || !strings.Contains(logs, "tenant=tenant-a") {
		t.Fatalf("panic must be observable with tenant, logs: %q", logs)
	}
	if !strings.Contains(logs, "reason=DOWNSTREAM_UNAVAILABLE") {
		t.Fatalf("conclusion log missing reason, logs: %q", logs)
	}
	t.Logf("tenant=tenant-a panic isolated, audit=%+v", recs[0])
}

// 一个租户的下游连续崩溃，不影响其它租户正常请求，也不算到别人头上。
func TestGateway_PanicInOneTenantDoesNotAffectOthers(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	gw, va := newTestGateway(sink, func(_ context.Context, req Request) (string, error) {
		if req.TenantID == "tenant-b" {
			panic("tenant-b downstream exploded")
		}
		return "ok", nil
	}, &buf)
	vb := va // 同一 validator 可签发不同租户令牌
	tokB := vb.Mint(Claims{TenantID: "tenant-b", Subject: "uB", ExpiresAt: testNow.Add(time.Minute)})
	tokA := va.Mint(Claims{TenantID: "tenant-a", Subject: "uA", ExpiresAt: testNow.Add(time.Minute)})

	var bConcluded int
	for i := 0; i < 5; i++ {
		// 每轮两个租户各一个请求并等收尾，避免同租户自身限流干扰隔离断言。
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, e := gw.Handle(context.Background(), Request{Token: tokB, TenantID: "tenant-b", Path: "/boom"})
			var ge *Error
			if !errors.As(e, &ge) || ge.Code != ErrCodeDownstreamUnavailable {
				t.Errorf("tenant-b: want DOWNSTREAM_UNAVAILABLE, got %v", e)
			}
		}()
		go func() {
			defer wg.Done()
			resp, e := gw.Handle(context.Background(), Request{Token: tokA, TenantID: "tenant-a", Path: "/ok"})
			if e != nil || resp.Body != "ok" {
				t.Errorf("tenant-a must be unaffected, resp=%q err=%v", resp.Body, e)
			}
		}()
		wg.Wait()
	}

	if got := gw.quota.Snapshot("tenant-b").InFlight; got != 0 {
		t.Fatalf("tenant-b leaked slots: %d", got)
	}
	if got := gw.quota.Snapshot("tenant-a").InFlight; got != 0 {
		t.Fatalf("tenant-a leaked slots: %d", got)
	}
	var aAllowed int
	for _, r := range sink.Records() {
		switch {
		case r.TenantID == "tenant-b" && r.Decision == "DENY":
			bConcluded++
			if r.Reason != "DOWNSTREAM_UNAVAILABLE" {
				t.Errorf("unexpected tenant-b audit reason: %s", r.Reason)
			}
		case r.TenantID == "tenant-a" && r.Decision == "ALLOW":
			aAllowed++
		default:
			t.Errorf("audit record attributed unexpectedly: tenant=%s decision=%s reason=%s",
				r.TenantID, r.Decision, r.Reason)
		}
	}
	if bConcluded != 5 || aAllowed != 5 {
		t.Fatalf("audit attribution wrong: bConcluded=%d aAllowed=%d", bConcluded, aAllowed)
	}
	t.Logf("tenant=tenant-b stableConclusions=%d tenant=tenant-a allowed=%d (isolation holds)",
		bConcluded, aAllowed)
}
