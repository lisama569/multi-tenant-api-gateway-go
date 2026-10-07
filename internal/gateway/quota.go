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
	Waiting       int // 正在有界等待队列中排队的请求数
}

// waiter 是有界等待队列中的一个排队请求；槽位放行时 close(ready)。
type waiter struct {
	ready chan struct{}
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
// 上调上限时立即按 FIFO 放行正在等待的请求。
func (q *QuotaManager) SetLimit(tenantID string, max int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if max <= 0 {
		delete(q.limits, tenantID)
		return
	}
	q.limits[tenantID] = max
	q.grantLocked(tenantID)
}

// quotaExceededLocked 构造带快照与可重试元信息的限流结论。调用方持锁。
func (q *QuotaManager) quotaExceededLocked(tenantID, msg string) (*QuotaSnapshot, error) {
	snap := QuotaSnapshot{
		TenantID:      tenantID,
		MaxConcurrent: q.limits[tenantID],
		InFlight:      q.inUse[tenantID],
		Waiting:       len(q.queues[tenantID]),
	}
	err := newError(ErrCodeQuotaExceeded, msg, nil)
	err.RetryAfter = q.retryAfter
	err.Retryable = true
	return &snap, err
}

// grantLocked 在持锁状态下按 FIFO 把空闲槽位交给队首等待者。
// 计数在放行瞬间同步增加，等待者被唤醒时槽位已确定归它所有。
func (q *QuotaManager) grantLocked(tenantID string) {
	limit, ok := q.limits[tenantID]
	if !ok {
		return
	}
	queue := q.queues[tenantID]
	for len(queue) > 0 && q.inUse[tenantID] < limit {
		head := queue[0]
		queue[0] = nil
		queue = queue[1:]
		q.inUse[tenantID]++
		close(head.ready)
	}
	q.queues[tenantID] = queue
}

// newReleaseLocked 构造“释放一次槽位并把槽位移交队首”的释放函数。
func (q *QuotaManager) newReleaseLocked(tenantID string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			q.mu.Lock()
			q.inUse[tenantID]--
			q.grantLocked(tenantID)
			q.mu.Unlock()
		})
	}
}

// Acquire 尝试占用一个并发槽位。成功时返回释放函数与占用后的快照；
// 配额已满时返回带 RetryAfter 元信息的 QUOTA_EXCEEDED 错误。
func (q *QuotaManager) Acquire(tenantID string) (func(), *QuotaSnapshot, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.limits[tenantID]; !ok {
		return nil, nil, newError(ErrCodeInternal, "no quota configured for tenant", nil)
	}
	if len(q.queues[tenantID]) > 0 || q.inUse[tenantID] >= q.limits[tenantID] {
		snap, err := q.quotaExceededLocked(tenantID, "tenant concurrency quota exceeded")
		return nil, snap, err
	}
	q.inUse[tenantID]++
	snap := QuotaSnapshot{
		TenantID: tenantID, MaxConcurrent: q.limits[tenantID], InFlight: q.inUse[tenantID],
	}
	return q.newReleaseLocked(tenantID), &snap, nil
}

// AcquireWait 在配额满时进行有界等待：
//   - 有空闲槽位且队列为空时立即占用；
//   - 否则进入该租户的 FIFO 队列，在 maxWait 内等前面请求释放槽位；
//   - 等待窗口耗尽或 ctx 在排队期间被取消，返回 QUOTA_EXCEEDED 结论，
//     计数与队列保持干净（不超配、不漏计数、不留幽灵排队项）。
func (q *QuotaManager) AcquireWait(ctx context.Context, tenantID string, maxWait time.Duration) (func(), *QuotaSnapshot, error) {
	q.mu.Lock()
	limit, ok := q.limits[tenantID]
	if !ok {
		q.mu.Unlock()
		return nil, nil, newError(ErrCodeInternal, "no quota configured for tenant", nil)
	}

	// 队列空且有空闲槽位：立即放行。队列非空时即使有空位也不得插队。
	if len(q.queues[tenantID]) == 0 && q.inUse[tenantID] < limit {
		q.inUse[tenantID]++
		snap := QuotaSnapshot{TenantID: tenantID, MaxConcurrent: limit, InFlight: q.inUse[tenantID]}
		release := q.newReleaseLocked(tenantID)
		q.mu.Unlock()
		return release, &snap, nil
	}

	// 不愿等 / ctx 已取消：维持原来的立即拒绝语义。
	if maxWait <= 0 || ctx.Err() != nil {
		snap, err := q.quotaExceededLocked(tenantID, "tenant concurrency quota exceeded")
		q.mu.Unlock()
		return nil, snap, err
	}

	w := &waiter{ready: make(chan struct{})}
	q.queues[tenantID] = append(q.queues[tenantID], w)
	timer := time.NewTimer(maxWait)
	q.mu.Unlock()

	select {
	case <-w.ready:
		timer.Stop()
		q.mu.Lock()
		snap := QuotaSnapshot{
			TenantID: tenantID, MaxConcurrent: limit,
			InFlight: q.inUse[tenantID], Waiting: len(q.queues[tenantID]),
		}
		release := q.newReleaseLocked(tenantID)
		q.mu.Unlock()
		return release, &snap, nil

	case <-timer.C:
		q.mu.Lock()
		q.removeWaiterLocked(tenantID, w)
		snap, err := q.quotaExceededLocked(tenantID, "quota wait deadline elapsed")
		q.mu.Unlock()
		return nil, snap, err

	case <-ctx.Done():
		timer.Stop()
		q.mu.Lock()
		q.removeWaiterLocked(tenantID, w)
		// 若恰好在取消瞬间已被放行（持锁串行，放行必发生在移除之前），
		// 槽位与计数已归本请求：正常占住并交给调用方释放，不重复计数。
		select {
		case <-w.ready:
			snap := QuotaSnapshot{
				TenantID: tenantID, MaxConcurrent: limit, InFlight: q.inUse[tenantID],
			}
			release := q.newReleaseLocked(tenantID)
			q.mu.Unlock()
			return release, &snap, nil
		default:
		}
		snap, err := q.quotaExceededLocked(tenantID, "quota wait canceled before admission")
		q.mu.Unlock()
		return nil, snap, err
	}
}

// removeWaiterLocked 从 FIFO 队列中删除指定等待者，保持其余等待者相对顺序。
func (q *QuotaManager) removeWaiterLocked(tenantID string, w *waiter) {
	queue := q.queues[tenantID]
	out := queue[:0]
	for _, item := range queue {
		if item != w {
			out = append(out, item)
		}
	}
	q.queues[tenantID] = out
}

// Snapshot 返回当前配额快照。
func (q *QuotaManager) Snapshot(tenantID string) QuotaSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	return QuotaSnapshot{
		TenantID:      tenantID,
		MaxConcurrent: q.limits[tenantID],
		InFlight:      q.inUse[tenantID],
		Waiting:       len(q.queues[tenantID]),
	}
}
