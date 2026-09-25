package gateway

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 并发临界：N 个 goroutine 争抢 limit 个槽位，任一时刻在用量不得超过上限，
// 且全部被拒绝者必须携带可重试元信息。
func TestQuota_ConcurrentNeverExceedsLimit(t *testing.T) {
	q := NewQuotaManager(time.Millisecond)
	q.SetLimit("tenant-a", 3)

	var inFlight atomic.Int64
	var maxSeen atomic.Int64
	var acquired atomic.Int64
	var rejected atomic.Int64

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, snap, err := q.Acquire("tenant-a")
			if err != nil {
				rejected.Add(1)
				var ge *Error
				if !errors.As(err, &ge) || ge.Code != ErrCodeQuotaExceeded {
					t.Errorf("unexpected error: %v", err)
				}
				if ge.RetryAfter <= 0 {
					t.Errorf("quota error must carry RetryAfter, got %v", ge.RetryAfter)
				}
				t.Logf("tenant=%s rejected snap=%+v code=%s retryAfter=%v",
					snap.TenantID, *snap, ge.Code, ge.RetryAfter)
				return
			}
			cur := inFlight.Add(1)
			for {
				old := maxSeen.Load()
				if cur <= old || maxSeen.CompareAndSwap(old, cur) {
					break
				}
			}
			acquired.Add(1)
			time.Sleep(time.Millisecond)
			inFlight.Add(-1)
			release()
		}()
	}
	wg.Wait()

	t.Logf("tenant=tenant-a acquired=%d rejected=%d maxInFlight=%d limit=3",
		acquired.Load(), rejected.Load(), maxSeen.Load())
	if maxSeen.Load() > 3 {
		t.Fatalf("concurrency exceeded limit: max=%d", maxSeen.Load())
	}
	if acquired.Load()+rejected.Load() != 100 {
		t.Fatalf("lost count: acquired=%d rejected=%d", acquired.Load(), rejected.Load())
	}
	if got := q.Snapshot("tenant-a").InFlight; got != 0 {
		t.Fatalf("leaked slots after release: inFlight=%d", got)
	}
}

// 重复释放不得重复扣减。
func TestQuota_DoubleReleaseIsSafe(t *testing.T) {
	q := NewQuotaManager(time.Millisecond)
	q.SetLimit("tenant-a", 1)
	release, _, err := q.Acquire("tenant-a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	release()
	release()
	if got := q.Snapshot("tenant-a").InFlight; got != 0 {
		t.Fatalf("double release corrupted counter: inFlight=%d", got)
	}
	// 槽位应可再次获取
	if _, _, err := q.Acquire("tenant-a"); err != nil {
		t.Fatalf("re-acquire after release: %v", err)
	}
}

// 配额规则变化后，相同输入应得到可复现的新判定。
func TestQuota_LimitChangeIsDeterministic(t *testing.T) {
	q := NewQuotaManager(time.Millisecond)
	q.SetLimit("tenant-a", 1)
	rel1, _, err := q.Acquire("tenant-a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer rel1()

	if _, _, err := q.Acquire("tenant-a"); err == nil {
		t.Fatal("expected quota exceeded with limit=1")
	}
	q.SetLimit("tenant-a", 2)
	rel2, snap, err := q.Acquire("tenant-a")
	if err != nil {
		t.Fatalf("after limit change, acquire: %v", err)
	}
	t.Logf("tenant=tenant-a after limit change snap=%+v", *snap)
	rel2()
}
