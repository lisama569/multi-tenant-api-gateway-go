package quota_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/multi-tenant-api-gateway-go/internal/gwerrors"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/quota"
)

// TestConcurrentNeverExceedsQuota 并发下实际用量不得超过配额，结束后计数归零。
func TestConcurrentNeverExceedsQuota(t *testing.T) {
	const limit = 4
	const goroutines = 200
	l := quota.NewLimiter(map[string]int{"t1": limit})

	var inFlight atomic.Int64
	var maxSeen atomic.Int64
	var acquired atomic.Int64
	var rejected atomic.Int64

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 限流可重试：被拒绝后短暂退避重试，直至拿到名额。
			var release func()
			for {
				r, err := l.Acquire("t1")
				if err == nil {
					release = r
					break
				}
				var ge *gwerrors.Error
				if !errors.As(err, &ge) || ge.Code != gwerrors.CodeQuotaExceeded || !ge.Retryable {
					t.Errorf("unexpected rejection: %v", err)
					return
				}
				rejected.Add(1)
				time.Sleep(time.Millisecond)
			}
			cur := inFlight.Add(1)
			for {
				m := maxSeen.Load()
				if cur <= m || maxSeen.CompareAndSwap(m, cur) {
					break
				}
			}
			time.Sleep(time.Millisecond) // 模拟在持有时工作
			inFlight.Add(-1)
			acquired.Add(1)
			release()
		}()
	}
	wg.Wait()

	if m := maxSeen.Load(); m > limit {
		t.Fatalf("concurrent usage %d exceeded quota %d", m, limit)
	}
	if used, lim := l.Snapshot("t1"); used != 0 || lim != limit {
		t.Fatalf("final snapshot = %d/%d, want 0/%d (count leak or double-decrement)", used, lim, limit)
	}
	if acquired.Load() != goroutines {
		t.Fatalf("acquired = %d, want %d (every goroutine must eventually acquire)",
			acquired.Load(), goroutines)
	}
	t.Logf("tenant=t1 quota_snapshot=%d/%d max_in_flight=%d acquired=%d rejected=%d",
		0, limit, maxSeen.Load(), acquired.Load(), rejected.Load())
}

// TestQuotaBoundary 配额临界：恰好 limit 次成功，第 limit+1 次被拒绝且可重试。
func TestQuotaBoundary(t *testing.T) {
	const limit = 3
	l := quota.NewLimiter(map[string]int{"t1": limit})

	releases := make([]func(), 0, limit)
	for i := 0; i < limit; i++ {
		r, err := l.Acquire("t1")
		if err != nil {
			t.Fatalf("acquire %d/%d rejected: %v", i+1, limit, err)
		}
		releases = append(releases, r)
	}
	if _, err := l.Acquire("t1"); err == nil {
		t.Fatal("over-quota acquire succeeded")
	} else {
		var ge *gwerrors.Error
		errors.As(err, &ge)
		if ge.Code != gwerrors.CodeQuotaExceeded || !ge.Retryable {
			t.Fatalf("got %+v, want retryable QUOTA_EXCEEDED", ge)
		}
		t.Logf("tenant=t1 quota_snapshot=3/3 normalized_error=%s retryable=%v", ge.Code, ge.Retryable)
	}

	// 释放一个名额后，相同输入应得到可复现的判定：再次成功。
	releases[0]()
	if _, err := l.Acquire("t1"); err != nil {
		t.Fatalf("acquire after release rejected: %v", err)
	}
	// 再次超限，判定与之前一致（可复现）。
	if _, err := l.Acquire("t1"); err == nil {
		t.Fatal("expected QUOTA_EXCEEDED again")
	}
}

// TestReleaseIdempotent 重复释放不得造成重复扣减。
func TestReleaseIdempotent(t *testing.T) {
	l := quota.NewLimiter(map[string]int{"t1": 1})
	r, err := l.Acquire("t1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	r()
	r() // 重复调用必须安全
	r()
	if used, _ := l.Snapshot("t1"); used != 0 {
		t.Fatalf("used = %d after double release, want 0", used)
	}
	// 配额应仍可正常获取。
	if _, err := l.Acquire("t1"); err != nil {
		t.Fatalf("acquire after double release: %v", err)
	}
}

// TestTenantIsolation 租户间配额互不影响。
func TestTenantIsolation(t *testing.T) {
	l := quota.NewLimiter(map[string]int{"ta": 1, "tb": 1})
	if _, err := l.Acquire("ta"); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Acquire("ta"); err == nil {
		t.Fatal("ta should be exhausted")
	}
	if _, err := l.Acquire("tb"); err != nil {
		t.Fatalf("tb affected by ta usage: %v", err)
	}
	t.Logf("tenant=ta exhausted, tenant=tb unaffected: isolation ok")
}
