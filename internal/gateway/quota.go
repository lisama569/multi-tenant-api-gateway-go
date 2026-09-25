package gateway

import (
	"sync"
	"time"
)

// QuotaSnapshot 描述某租户在判定时刻的配额状态。
type QuotaSnapshot struct {
	TenantID      string
	MaxConcurrent int
	InFlight      int
}

// QuotaManager 按租户管理并发配额。所有计数在同一互斥锁下变更，
// 保证并发下不超配、不丢计数、不重复扣减。
type QuotaManager struct {
	mu         sync.Mutex
	limits     map[string]int
	inUse      map[string]int
	retryAfter time.Duration
}

func NewQuotaManager(retryAfter time.Duration) *QuotaManager {
	if retryAfter <= 0 {
		retryAfter = 50 * time.Millisecond
	}
	return &QuotaManager{
		limits:     map[string]int{},
		inUse:      map[string]int{},
		retryAfter: retryAfter,
	}
}

// SetLimit 设置租户并发上限（<=0 表示移除限制配置）。
func (q *QuotaManager) SetLimit(tenantID string, max int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if max <= 0 {
		delete(q.limits, tenantID)
		return
	}
	q.limits[tenantID] = max
}

// Acquire 尝试占用一个并发槽位。成功时返回释放函数与占用后的快照；
// 配额已满时返回带 RetryAfter 元信息的 QUOTA_EXCEEDED 错误。
func (q *QuotaManager) Acquire(tenantID string) (func(), *QuotaSnapshot, error) {
	q.mu.Lock()
	limit, ok := q.limits[tenantID]
	if !ok {
		q.mu.Unlock()
		return nil, nil, newError(ErrCodeInternal, "no quota configured for tenant", nil)
	}
	if q.inUse[tenantID] >= limit {
		snap := QuotaSnapshot{TenantID: tenantID, MaxConcurrent: limit, InFlight: q.inUse[tenantID]}
		q.mu.Unlock()
		err := newError(ErrCodeQuotaExceeded, "tenant concurrency quota exceeded", nil)
		err.RetryAfter = q.retryAfter
		return nil, &snap, err
	}
	q.inUse[tenantID]++
	snap := QuotaSnapshot{TenantID: tenantID, MaxConcurrent: limit, InFlight: q.inUse[tenantID]}
	q.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			q.mu.Lock()
			q.inUse[tenantID]--
			q.mu.Unlock()
		})
	}
	return release, &snap, nil
}

// Snapshot 返回当前配额快照。
func (q *QuotaManager) Snapshot(tenantID string) QuotaSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	return QuotaSnapshot{
		TenantID:      tenantID,
		MaxConcurrent: q.limits[tenantID],
		InFlight:      q.inUse[tenantID],
	}
}
