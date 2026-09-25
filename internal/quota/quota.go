// Package quota 实现按租户的并发配额与限流。
// 计数由互斥锁保护，Acquire/release 严格配对，杜绝丢失或重复扣减。
package quota

import (
	"fmt"
	"sync"

	"github.com/lisama569/multi-tenant-api-gateway-go/internal/gwerrors"
)

// Limiter 按租户限制并发用量。
type Limiter struct {
	mu     sync.Mutex
	quotas map[string]int
	used   map[string]int
}

// NewLimiter 以租户配额表创建限流器（配额表拷贝保存，外部修改不影响判定）。
func NewLimiter(quotas map[string]int) *Limiter {
	q := make(map[string]int, len(quotas))
	for k, v := range quotas {
		q[k] = v
	}
	return &Limiter{quotas: q, used: map[string]int{}}
}

// Acquire 尝试为租户占用一个并发名额。
// 成功返回 release 函数（必须且只能调用一次）；超限返回 *gwerrors.Error（可重试）。
func (l *Limiter) Acquire(tenant string) (release func(), err error) {
	l.mu.Lock()
	limit, ok := l.quotas[tenant]
	if !ok {
		limit = 0
	}
	if l.used[tenant] >= limit {
		used := l.used[tenant]
		l.mu.Unlock()
		return nil, gwerrors.New(gwerrors.CodeQuotaExceeded,
			fmt.Sprintf("tenant %q concurrency quota exceeded (%d/%d)", tenant, used, limit),
			true, nil)
	}
	l.used[tenant]++
	l.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			l.used[tenant]--
			l.mu.Unlock()
		})
	}, nil
}

// Snapshot 返回租户当前用量快照（用于审计与测试日志）。
func (l *Limiter) Snapshot(tenant string) (used, limit int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.used[tenant], l.quotas[tenant]
}
