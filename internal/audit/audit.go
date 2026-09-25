// Package audit 提供审计记录写入与内存存储。
// 审计写入失败不得改变主流程业务结论，但必须可观测（失败计数）。
package audit

import (
	"sync"
	"time"
)

// Record 是一条审计记录。
type Record struct {
	TenantID string
	Decision string // "accepted" / "rejected"
	Reason   string // 判定依据：错误码或 "OK"
	Quota    string // 配额快照，如 "1/3"
	At       time.Time
}

// Sink 是审计写入目标。
type Sink interface {
	Write(Record) error
}

// MemorySink 是并发安全的内存审计存储。
type MemorySink struct {
	mu      sync.Mutex
	records []Record
}

// NewMemorySink 创建内存存储。
func NewMemorySink() *MemorySink { return &MemorySink{} }

// Write 追加一条记录。
func (m *MemorySink) Write(r Record) error {
	m.mu.Lock()
	m.records = append(m.records, r)
	m.mu.Unlock()
	return nil
}

// Records 返回全部记录副本。
func (m *MemorySink) Records() []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Record, len(m.records))
	copy(out, m.records)
	return out
}

// Recorder 包装 Sink：写入失败不改变主流程，但累计失败次数以便观测。
type Recorder struct {
	sink Sink

	mu         sync.Mutex
	writeFails int
}

// NewRecorder 创建审计记录器。
func NewRecorder(sink Sink) *Recorder { return &Recorder{sink: sink} }

// Log 写入审计；失败仅计数，不影响主流程结论。
func (r *Recorder) Log(rec Record) {
	if rec.At.IsZero() {
		rec.At = time.Now()
	}
	if err := r.sink.Write(rec); err != nil {
		r.mu.Lock()
		r.writeFails++
		r.mu.Unlock()
	}
}

// WriteFailures 返回累计写入失败次数（可观测性）。
func (r *Recorder) WriteFailures() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.writeFails
}
