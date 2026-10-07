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

// 等待窗口内前面的请求释放槽位：排队者被放行且审计为 ALLOW。
func TestGateway_WaitAdmittedWithinWindow(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	entered := make(chan struct{}, 2)
	releaseFirst := make(chan struct{})
	gw, v := newTestGateway(sink, func(_ context.Context, req Request) (string, error) {
		entered <- struct{}{}
		if req.Path == "/first" {
			<-releaseFirst
		}
		return "ok:" + req.Path, nil
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-b", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	firstDone := make(chan struct{})
	go func() {
		_, _ = gw.Handle(context.Background(), Request{
			Token: tok, TenantID: "tenant-b", Path: "/first",
		})
		close(firstDone)
	}()
	<-entered // 第一个请求已占住唯一槽位

	// 第二个请求带等待窗口排队。
	type result struct {
		body string
		err  error
	}
	waitDone := make(chan result, 1)
	go func() {
		r, e := gw.Handle(context.Background(), Request{
			Token: tok, TenantID: "tenant-b", Path: "/second",
			WaitTimeout: 500 * time.Millisecond,
		})
		waitDone <- result{r.Body, e}
	}()

	// 等排队真正入队后，再释放第一个请求。
	deadline := time.Now().Add(200 * time.Millisecond)
	for gw.quota.Snapshot("tenant-b").Waiting != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("waiter never enqueued: %+v", gw.quota.Snapshot("tenant-b"))
		}
		time.Sleep(time.Millisecond)
	}
	if snap := gw.quota.Snapshot("tenant-b"); snap.InFlight != 1 || snap.Waiting != 1 {
		t.Fatalf("before release want inFlight=1 waiting=1, got %+v", snap)
	}
	close(releaseFirst)

	select {
	case r := <-waitDone:
		if r.err != nil {
			t.Fatalf("waiting request must be admitted, got %v", r.err)
		}
		if r.body != "ok:/second" {
			t.Fatalf("unexpected body: %q", r.body)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("waiting request not admitted after slot released")
	}
	<-firstDone

	if got := gw.quota.Snapshot("tenant-b"); got.InFlight != 0 || got.Waiting != 0 {
		t.Fatalf("quota not clean: %+v", got)
	}
	var allowed, denied int
	for _, r := range sink.Records() {
		if r.Decision == "ALLOW" {
			allowed++
		} else {
			denied++
		}
	}
	if allowed != 2 || denied != 0 {
		t.Fatalf("want 2 ALLOW 0 DENY, got allowed=%d denied=%d", allowed, denied)
	}
	logs := buf.String()
	if !strings.Contains(logs, "reason=OK") {
		t.Fatalf("admission must log ALLOW conclusion, logs: %q", logs)
	}
	t.Logf("tenant=tenant-b FIFO admission works, records=%d", len(sink.Records()))
}

// 等待窗口耗尽仍未轮到：返回 QUOTA_EXCEEDED，队列与计数保持干净。
func TestGateway_WaitTimeoutReturnsQuotaExceeded(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	releaseFirst := make(chan struct{})
	defer close(releaseFirst)
	gw, v := newTestGateway(sink, func(_ context.Context, req Request) (string, error) {
		if req.Path == "/holder" {
			<-releaseFirst
		}
		return "ok", nil
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-b", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	holderDone := make(chan struct{})
	go func() {
		_, _ = gw.Handle(context.Background(), Request{Token: tok, TenantID: "tenant-b", Path: "/holder"})
		close(holderDone)
	}()

	// 确保占槽后再排队。
	deadline := time.Now().Add(200 * time.Millisecond)
	for gw.quota.Snapshot("tenant-b").InFlight != 1 {
		if time.Now().After(deadline) {
			t.Fatal("holder never acquired slot")
		}
		time.Sleep(time.Millisecond)
	}

	start := time.Now()
	_, err := gw.Handle(context.Background(), Request{
		Token: tok, TenantID: "tenant-b", Path: "/waiter", WaitTimeout: 40 * time.Millisecond,
	})
	elapsed := time.Since(start)

	var ge *Error
	if !errors.As(err, &ge) || ge.Code != ErrCodeQuotaExceeded {
		t.Fatalf("want QUOTA_EXCEEDED after wait window, got %v", err)
	}
	if !ge.Retryable || ge.RetryAfter <= 0 {
		t.Fatalf("wait-timeout conclusion must carry retry metadata")
	}
	if elapsed < 30*time.Millisecond || elapsed > 250*time.Millisecond {
		t.Fatalf("should return right around wait window, elapsed=%v", elapsed)
	}
	if snap := gw.quota.Snapshot("tenant-b"); snap.InFlight != 1 || snap.Waiting != 0 {
		t.Fatalf("queue must be clean after wait timeout: %+v", snap)
	}
	recs := sink.Records()
	if len(recs) != 1 || recs[0].Reason != "QUOTA_EXCEEDED" || recs[0].Decision != "DENY" {
		t.Fatalf("wait-timeout must be audited once: %+v", recs)
	}
	t.Logf("tenant=tenant-b wait-timeout conclusion=%v elapsed=%v audit=%+v",
		ge.Code, elapsed, recs[0].Quota)
}

// 排队期间调用方取消：结论稳定，取消者移出队列，队尾计数不乱。
func TestGateway_WaitCanceledKeepsQueueClean(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	releaseHolder := make(chan struct{})
	defer close(releaseHolder)
	gw, v := newTestGateway(sink, func(_ context.Context, req Request) (string, error) {
		if req.Path == "/holder" {
			<-releaseHolder

		}
		return "ok", nil
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-b", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	go func() {
		_, _ = gw.Handle(context.Background(), Request{Token: tok, TenantID: "tenant-b", Path: "/holder"})
	}()
	deadline := time.Now().Add(200 * time.Millisecond)
	for gw.quota.Snapshot("tenant-b").InFlight != 1 {
		if time.Now().After(deadline) {
			t.Fatal("holder never acquired slot")
		}
		time.Sleep(time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, e := gw.Handle(ctx, Request{
			Token: tok, TenantID: "tenant-b", Path: "/canceled", WaitTimeout: time.Second,
		})
		errCh <- e
	}()
	for gw.quota.Snapshot("tenant-b").Waiting != 1 {
		time.Sleep(time.Millisecond)
	}
	cancel()

	select {
	case err := <-errCh:
		var ge *Error
		if !errors.As(err, &ge) || ge.Code != ErrCodeQuotaExceeded {
			t.Fatalf("canceled waiter must get stable QUOTA_EXCEEDED, got %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("canceled waiter did not return")
	}
	if snap := gw.quota.Snapshot("tenant-b"); snap.InFlight != 1 || snap.Waiting != 0 {
		t.Fatalf("queue corrupted after cancel: %+v", snap)
	}
	t.Logf("tenant=tenant-b canceled waiter removed cleanly: %+v", gw.quota.Snapshot("tenant-b"))
}

// FIFO 公平性：按入队顺序放行，等待者全部得到处理，超配与漏计数均不得发生。
func TestQuota_FIFOBoundedWaitingUnderStorm(t *testing.T) {
	q := NewQuotaManager(time.Millisecond)
	q.SetLimit("tenant-a", 3)

	var inFlight atomic.Int64
	var maxSeen atomic.Int64
	var admitted atomic.Int64
	var timedOut atomic.Int64
	var orderMu sync.Mutex
	var admittedOrder []int

	const n = 30
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			rel, _, err := q.AcquireWait(ctx, "tenant-a", time.Second)
			if err != nil {
				var ge *Error
				if !errors.As(err, &ge) || ge.Code != ErrCodeQuotaExceeded {
					t.Errorf("unexpected error: %v", err)
				}
				timedOut.Add(1)
				return
			}
			cur := inFlight.Add(1)
			for {
				old := maxSeen.Load()
				if cur <= old || maxSeen.CompareAndSwap(old, cur) {
					break
				}
			}
			orderMu.Lock()
			admittedOrder = append(admittedOrder, i)
			orderMu.Unlock()
			admitted.Add(1)
			time.Sleep(2 * time.Millisecond)
			inFlight.Add(-1)
			rel()
		}(i)
	}
	wg.Wait()

	if maxSeen.Load() > 3 {
		t.Fatalf("oversubscribed: maxInFlight=%d", maxSeen.Load())
	}
	if admitted.Load()+timedOut.Load() != n {
		t.Fatalf("lost count: admitted=%d timedOut=%d total=%d",
			admitted.Load(), timedOut.Load(), n)
	}
	if snap := q.Snapshot("tenant-a"); snap.InFlight != 0 || snap.Waiting != 0 {
		t.Fatalf("quota not clean after storm: %+v", snap)
	}
	t.Logf("tenant=tenant-a admitted=%d timedOut=%d maxInFlight=%d",
		admitted.Load(), timedOut.Load(), maxSeen.Load())
}

// 等待窗口不足时应保持与原限流一致的立即拒绝语义。
func TestGateway_ZeroWaitKeepsImmediateRejection(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	releaseHolder := make(chan struct{})
	defer close(releaseHolder)
	gw, v := newTestGateway(sink, func(_ context.Context, req Request) (string, error) {
		<-releaseHolder
		return "ok", nil
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-b", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	go func() {
		_, _ = gw.Handle(context.Background(), Request{Token: tok, TenantID: "tenant-b", Path: "/h"})
	}()
	for gw.quota.Snapshot("tenant-b").InFlight != 1 {
		time.Sleep(time.Millisecond)
	}

	_, err := gw.Handle(context.Background(), Request{Token: tok, TenantID: "tenant-b", Path: "/nq"})
	var ge *Error
	if !errors.As(err, &ge) || ge.Code != ErrCodeQuotaExceeded {
		t.Fatalf("want immediate QUOTA_EXCEEDED, got %v", err)
	}
	if gw.quota.Snapshot("tenant-b").Waiting != 0 {
		t.Fatalf("zero-wait request must not enqueue")
	}
}

// 严格 FIFO：等待者按入队先后（到达顺序）被放行，不允许后到者插队。
func TestQuota_StrictFIFOAdmissionOrder(t *testing.T) {
	q := NewQuotaManager(time.Millisecond)
	q.SetLimit("tenant-a", 1)

	// 占住唯一槽位。
	holderRel, _, err := q.Acquire("tenant-a")
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	const waiters = 6
	started := make(chan int, waiters)
	finished := make(chan int, waiters)
	var wg sync.WaitGroup
	enqueue := func(i int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- i
			rel, _, err := q.AcquireWait(context.Background(), "tenant-a", 2*time.Second)
			if err != nil {
				t.Errorf("waiter %d: %v", i, err)
				return
			}
			finished <- i
			rel()
		}()
	}

	for i := 0; i < waiters; i++ {
		enqueue(i)
		<-started // 确保 goroutine 已启动并即将/开始入队
		// 等待该等待者确实进入队列，再启动下一个，固定到达顺序。
		deadline := time.Now().Add(time.Second)
		for q.Snapshot("tenant-a").Waiting != i+1 {
			if time.Now().After(deadline) {
				t.Fatalf("waiter %d did not enqueue, snap=%+v", i, q.Snapshot("tenant-a"))
			}
			time.Sleep(time.Millisecond)
		}
	}

	holderRel() // 放行：之后等待者应严格按 0..N-1 完成
	var order []int
	for i := 0; i < waiters; i++ {
		select {
		case idx := <-finished:
			order = append(order, idx)
		case <-time.After(2 * time.Second):
			t.Fatalf("waiter %d never admitted, order so far %v", i, order)
		}
	}
	wg.Wait()
	for i, idx := range order {
		if idx != i {
			t.Fatalf("FIFO violated: admission order=%v, want 0..%d", order, waiters-1)
		}
	}
	if snap := q.Snapshot("tenant-a"); snap.InFlight != 0 || snap.Waiting != 0 {
		t.Fatalf("quota not clean: %+v", snap)
	}
	t.Logf("tenant=tenant-a strict FIFO order=%v", order)
}
