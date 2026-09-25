package gateway

import (
	"sync"
	"time"
)

// AuditRecord 记录一次请求的判定依据。
type AuditRecord struct {
	TenantID  string
	Subject   string
	Path      string
	Decision  string // "ALLOW" / "DENY"
	Reason    string
	Quota     QuotaSnapshot
	Timestamp time.Time
}

// AuditSink 是审计写入接口，可由内存或失败注入实现。
type AuditSink interface {
	Write(rec AuditRecord) error
}

// InMemoryAuditSink 是线程安全的内存实现。
type InMemoryAuditSink struct {
	mu      sync.Mutex
	records []AuditRecord
}

func NewInMemoryAuditSink() *InMemoryAuditSink { return &InMemoryAuditSink{} }

func (s *InMemoryAuditSink) Write(rec AuditRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, rec)
	return nil
}

func (s *InMemoryAuditSink) Records() []AuditRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]AuditRecord, len(s.records))
	copy(out, s.records)
	return out
}
