package gateway

import (
	"context"
	"sync"
	"time"
)

// QuotaSnapshot 描述某租户在判定时刻的配额状态。
type QuotaSnapshot struct {
	TenantID      string
	MaxConcurrent int
	InFlight      int
}

// waiter 是排队等待配额槽位的请求。granted/cancelled 只在互斥锁下读写，
// 与超时、取消路径的交互不会竞态。
type waiter struct {
	ch        chan struct{} // 获得槽位时关闭
	granted   bool
	cancelled bool
}

// QuotaManager 按租户管理并发配额。所有计数在同一互斥锁下变更，
// 保证并发下不超配、不丢计数、不重复扣减。
type QuotaManager struct {
	mu         sync.Mutex
	limits     map[string]int
	inUse      map[string]int
	queues     map[string][]*waiter
	retryAfter time.Duration
}

func NewQuotaManager(retryAfter time.Duration) *QuotaManager {
	if retryAfter <= 0 {
		retryAfter = 50 * time.Millisecond
	}
	return &QuotaManager{
		limits:     map[string]int{},
		inUse:      map[string]int{},
		queues:     map[string][]*waiter{},
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

// quotaExceededError 构造带 RetryAfter 元信息的限流错误。
func (q *QuotaManager) quotaExceededError() *Error {
	err := newError(ErrCodeQuotaExceeded, "tenant concurrency quota exceeded", nil)
	err.RetryAfter = q.retryAfter
	err.Retryable = true
	return err
}

// dropCancelledLocked 惰性清除队首已取消的等待者。调用方须持有锁。
func (q *QuotaManager) dropCancelledLocked(tenantID string) {
	queue := q.queues[tenantID]
	for len(queue) > 0 && queue[0].cancelled {
		queue = queue[1:]
	}
	if len(queue) == 0 {
		delete(q.queues, tenantID)
	} else {
		q.queues[tenantID] = queue
	}
}

// tryGrantLocked 在队列为空且有余量时立即占用槽位。调用方须持有锁。
func (q *QuotaManager) tryGrantLocked(tenantID string, limit int) (QuotaSnapshot, bool) {
	q.dropCancelledLocked(tenantID)
	if len(q.queues[tenantID]) > 0 || q.inUse[tenantID] >= limit {
		return QuotaSnapshot{TenantID: tenantID, MaxConcurrent: limit, InFlight: q.inUse[tenantID]}, false
	}
	q.inUse[tenantID]++
	return QuotaSnapshot{TenantID: tenantID, MaxConcurrent: limit, InFlight: q.inUse[tenantID]}, true
}

// promoteLocked 把释放出的槽位按到达先后移交给队首等待者。调用方须持有锁。
func (q *QuotaManager) promoteLocked(tenantID string) {
	q.dropCancelledLocked(tenantID)
	queue := q.queues[tenantID]
	if len(queue) == 0 {
		return
	}
	w := queue[0]
	q.queues[tenantID] = queue[1:]
	if len(q.queues[tenantID]) == 0 {
		delete(q.queues, tenantID)
	}
	w.granted = true
	q.inUse[tenantID]++
	close(w.ch)
}

// newRelease 构造幂等的释放函数：归还槽位并尝试移交队首等待者。
func (q *QuotaManager) newRelease(tenantID string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			q.mu.Lock()
			q.inUse[tenantID]--
			q.promoteLocked(tenantID)
			q.mu.Unlock()
		})
	}
}

// Acquire 尝试占用一个并发槽位。成功时返回释放函数与占用后的快照；
// 配额已满时立即返回带 RetryAfter 元信息的 QUOTA_EXCEEDED 错误。
func (q *QuotaManager) Acquire(tenantID string) (func(), *QuotaSnapshot, error) {
	q.mu.Lock()
	limit, ok := q.limits[tenantID]
	if !ok {
		q.mu.Unlock()
		return nil, nil, newError(ErrCodeInternal, "no quota configured for tenant", nil)
	}
	snap, granted := q.tryGrantLocked(tenantID, limit)
	q.mu.Unlock()
	if !granted {
		return nil, &snap, q.quotaExceededError()
	}
	return q.newRelease(tenantID), &snap, nil
}

// AcquireWait 在 Acquire 之上增加有界等待：配额已满且 maxWait > 0 时按到达
// 先后排队，槽位释放时依次放行；等待窗口耗尽仍未轮到的返回 QUOTA_EXCEEDED，
// ctx 取消返回 REQUEST_CANCELLED。两种失败都不会占用或泄漏槽位计数。
func (q *QuotaManager) AcquireWait(ctx context.Context, tenantID string, maxWait time.Duration) (func(), *QuotaSnapshot, error) {
	q.mu.Lock()
	limit, ok := q.limits[tenantID]
	if !ok {
		q.mu.Unlock()
		return nil, nil, newError(ErrCodeInternal, "no quota configured for tenant", nil)
	}
	if snap, granted := q.tryGrantLocked(tenantID, limit); granted {
		q.mu.Unlock()
		return q.newRelease(tenantID), &snap, nil
	}
	if maxWait <= 0 {
		snap := QuotaSnapshot{TenantID: tenantID, MaxConcurrent: limit, InFlight: q.inUse[tenantID]}
		q.mu.Unlock()
		return nil, &snap, q.quotaExceededError()
	}
	w := &waiter{ch: make(chan struct{})}
	q.queues[tenantID] = append(q.queues[tenantID], w)
	snap := QuotaSnapshot{TenantID: tenantID, MaxConcurrent: limit, InFlight: q.inUse[tenantID]}
	q.mu.Unlock()

	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	select {
	case <-w.ch:
		grantedSnap := q.Snapshot(tenantID)
		return q.newRelease(tenantID), &grantedSnap, nil
	case <-timer.C:
		q.abandonWaiter(tenantID, w)
		return nil, &snap, q.quotaExceededError()
	case <-ctx.Done():
		q.abandonWaiter(tenantID, w)
		return nil, &snap, newError(ErrCodeRequestCancelled, "wait for quota slot cancelled", ctx.Err())
	}
}

// abandonWaiter 处理等待者超时/取消：若槽位已移交给它则代为归还并继续移交，
// 否则标记取消（惰性出队）。计数在任何交错下都保持一致。
func (q *QuotaManager) abandonWaiter(tenantID string, w *waiter) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if w.granted {
		q.inUse[tenantID]--
		q.promoteLocked(tenantID)
		return
	}
	w.cancelled = true
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

// queueLen 返回当前排队等待中的请求数（含已标记取消、待惰性清除的），
// 供测试与诊断观察排队行为。
func (q *QuotaManager) queueLen(tenantID string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queues[tenantID])
}
