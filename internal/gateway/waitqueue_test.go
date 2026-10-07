package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 等待窗口内被放行：槽位释放后，排队请求按到达先后依次获得放行。
func TestGateway_WaitQueueAdmittedInFIFOOrder(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	hold := make(chan struct{})
	firstEntered := make(chan struct{})
	var mu sync.Mutex
	var admitOrder []string
	gw, v := newTestGateway(sink, func(_ context.Context, req Request) (string, error) {
		mu.Lock()
		admitOrder = append(admitOrder, req.Path)
		mu.Unlock()
		if req.Path == "/first" {
			close(firstEntered)
			<-hold
		}
		return "ok", nil
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-b", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	firstDone := make(chan error, 1)
	go func() {
		_, err := gw.Handle(context.Background(), Request{Token: tok, TenantID: "tenant-b", Path: "/first"})
		firstDone <- err
	}()
	<-firstEntered

	// 两个等待者先后入队
	waiterDone := make([]chan error, 2)
	for i, path := range []string{"/w1", "/w2"} {
		waiterDone[i] = make(chan error, 1)
		go func(path string, done chan error) {
			_, err := gw.Handle(context.Background(), Request{
				Token: tok, TenantID: "tenant-b", Path: path, MaxQueueWait: 2 * time.Second,
			})
			done <- err
		}(path, waiterDone[i])
		// 等它确实进入队列再启动下一个，保证入队次序
		for gw.quota.queueLen("tenant-b") < i+1 {
			time.Sleep(time.Millisecond)
		}
	}

	close(hold) // 释放槽位，等待者应被依次放行
	if err := <-firstDone; err != nil {
		t.Fatalf("first request: %v", err)
	}
	for i, done := range waiterDone {
		if err := <-done; err != nil {
			t.Fatalf("waiter %d must be admitted within window: %v", i, err)
		}
	}

	mu.Lock()
	t.Logf("tenant=tenant-b admitOrder=%v snapshot=%+v", admitOrder, gw.quota.Snapshot("tenant-b"))
	if len(admitOrder) != 3 || admitOrder[0] != "/first" || admitOrder[1] != "/w1" || admitOrder[2] != "/w2" {
		t.Fatalf("waiters must be admitted in arrival order, got %v", admitOrder)
	}
	mu.Unlock()
	if got := gw.quota.Snapshot("tenant-b").InFlight; got != 0 {
		t.Fatalf("slots leaked: inFlight=%d", got)
	}
	if got := len(sink.Records()); got != 3 {
		t.Fatalf("every request must be audited, want 3 got %d", got)
	}
}

// 等待窗口耗尽仍未轮到：返回带 RetryAfter 的 QUOTA_EXCEEDED，计数不受影响。
func TestGateway_WaitQueueTimeout(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	hold := make(chan struct{})
	firstEntered := make(chan struct{})
	gw, v := newTestGateway(sink, func(_ context.Context, req Request) (string, error) {
		close(firstEntered)
		<-hold
		return "ok", nil
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-b", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	firstDone := make(chan error, 1)
	go func() {
		_, err := gw.Handle(context.Background(), Request{Token: tok, TenantID: "tenant-b", Path: "/first"})
		firstDone <- err
	}()
	<-firstEntered

	start := time.Now()
	_, err := gw.Handle(context.Background(), Request{
		Token: tok, TenantID: "tenant-b", Path: "/impatient", MaxQueueWait: 30 * time.Millisecond,
	})
	elapsed := time.Since(start)

	var ge *Error
	if !errors.As(err, &ge) || ge.Code != ErrCodeQuotaExceeded {
		t.Fatalf("want QUOTA_EXCEEDED after wait window, got %v", err)
	}
	if ge.RetryAfter <= 0 || !ge.Retryable {
		t.Fatalf("quota error must carry retry metadata: %+v", ge)
	}
	if elapsed < 30*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("must wait the full window then give up, took %v", elapsed)
	}
	t.Logf("tenant=tenant-b waited=%v code=%s retryAfter=%v", elapsed, ge.Code, ge.RetryAfter)

	close(hold)
	if err := <-firstDone; err != nil {
		t.Fatalf("first request: %v", err)
	}
	if got := gw.quota.Snapshot("tenant-b").InFlight; got != 0 {
		t.Fatalf("timed-out waiter must not disturb counts: inFlight=%d", got)
	}
	if got := gw.quota.queueLen("tenant-b"); got != 0 {
		t.Fatalf("timed-out waiter must leave the queue, queueLen=%d", got)
	}
	recs := sink.Records()
	if len(recs) != 2 {
		t.Fatalf("both requests must be audited, got %d", len(recs))
	}
	var deny *AuditRecord
	for i := range recs {
		if recs[i].Decision == "DENY" {
			deny = &recs[i]
		}
	}
	if deny == nil || deny.Reason != string(ErrCodeQuotaExceeded) || deny.TenantID != "tenant-b" {
		t.Fatalf("wait timeout must be audited as QUOTA_EXCEEDED deny: %+v", recs)
	}
}

// 等待中被调用方取消：返回 REQUEST_CANCELLED，计数与队列保持干净。
func TestGateway_WaitQueueCancelled(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	hold := make(chan struct{})
	firstEntered := make(chan struct{})
	gw, v := newTestGateway(sink, func(_ context.Context, req Request) (string, error) {
		close(firstEntered)
		<-hold
		return "ok", nil
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-b", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	firstDone := make(chan error, 1)
	go func() {
		_, err := gw.Handle(context.Background(), Request{Token: tok, TenantID: "tenant-b", Path: "/first"})
		firstDone <- err
	}()
	<-firstEntered

	ctx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		_, err := gw.Handle(ctx, Request{
			Token: tok, TenantID: "tenant-b", Path: "/cancelled", MaxQueueWait: 5 * time.Second,
		})
		waiterDone <- err
	}()
	for gw.quota.queueLen("tenant-b") < 1 {
		time.Sleep(time.Millisecond)
	}
	cancel()

	var ge *Error
	if err := <-waiterDone; !errors.As(err, &ge) || ge.Code != ErrCodeRequestCancelled {
		t.Fatalf("want REQUEST_CANCELLED, got %v", err)
	}

	close(hold)
	if err := <-firstDone; err != nil {
		t.Fatalf("first request: %v", err)
	}
	if got := gw.quota.Snapshot("tenant-b").InFlight; got != 0 {
		t.Fatalf("cancelled waiter must not disturb counts: inFlight=%d", got)
	}
	if got := len(sink.Records()); got != 2 {
		t.Fatalf("every request must be audited, want 2 got %d", got)
	}
}

// 并发压力：N 个请求带等待窗口争抢有限槽位，任一时刻不超配、计数不丢，
// 结束后额度回到干净状态、审计一条不少。
func TestGateway_WaitQueueConcurrentNoOvercommit(t *testing.T) {
	var buf strings.Builder
	sink := NewInMemoryAuditSink()
	var inFlight atomic.Int64
	var maxSeen atomic.Int64
	gw, v := newTestGateway(sink, func(_ context.Context, req Request) (string, error) {
		cur := inFlight.Add(1)
		for {
			old := maxSeen.Load()
			if cur <= old || maxSeen.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		inFlight.Add(-1)
		return "ok", nil
	}, &buf)
	tok := v.Mint(Claims{TenantID: "tenant-a", Subject: "u1", ExpiresAt: testNow.Add(time.Minute)})

	const n = 40
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = gw.Handle(context.Background(), Request{
				Token: tok, TenantID: "tenant-a", Path: fmt.Sprintf("/job/%d", i),
				MaxQueueWait: 5 * time.Second,
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("request %d with generous wait window must succeed: %v", i, err)
		}
	}
	t.Logf("tenant=tenant-a admitted=%d maxInFlight=%d limit=2 snapshot=%+v",
		n, maxSeen.Load(), gw.quota.Snapshot("tenant-a"))
	if maxSeen.Load() > 2 {
		t.Fatalf("concurrency exceeded limit: max=%d", maxSeen.Load())
	}
	if got := gw.quota.Snapshot("tenant-a").InFlight; got != 0 {
		t.Fatalf("slots leaked: inFlight=%d", got)
	}
	if got := gw.quota.queueLen("tenant-a"); got != 0 {
		t.Fatalf("queue must be drained: queueLen=%d", got)
	}
	if got := len(sink.Records()); got != n {
		t.Fatalf("every request must be audited, want %d got %d", n, got)
	}
	if !strings.Contains(buf.String(), "decision=ALLOW") {
		t.Fatalf("decision logs must be observable, logs: %q", buf.String())
	}
}
