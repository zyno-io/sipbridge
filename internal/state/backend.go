package state

import (
	"context"
	"errors"
)

const (
	MemoryBackend = "memory"
	RedisBackend  = "redis"
)

// Backend is the complete service state boundary. Implementations atomically
// commit protocol values, receipts, scheduling and owner-lease guards together.
// Returned snapshots belong to the caller. No method dispatches external effects.
// Historical claimed execution is deliberately excluded.
type Backend interface {
	Authority() Authority
	Namespace() string
	Versions() Versions
	PartitionCount() uint16
	Limits() Limits
	GroupForCallID(string) (Ref, error)
	Available() bool
	Ready(context.Context) error
	Close() error
	Load(context.Context, Ref) (Record, error)
	LoadAt(context.Context, Ref) (Record, int64, error)
	ReadUpdate(context.Context, Ref, Input) (UpdateResult, error)
	CompareAndSwap(context.Context, Update, Payload, []byte) (Receipt, error)
	InspectOperation(context.Context, Ref, string, string) (Inspection, error)
	AcquireApplicationLease(context.Context, string, string) (ApplicationLease, error)
	RenewApplicationLease(context.Context, string, string) (ApplicationLease, error)
	ResumeApplicationLease(context.Context, string, string) (ApplicationLease, error)
	ReadApplicationLease(context.Context, string) (ApplicationLease, error)
	AcquireRecoverableApplicationLease(context.Context, string, string, ApplicationLeaseOwner) (ApplicationLease, error)
	RenewApplicationAttachment(context.Context, ApplicationLeaseRef, ApplicationLeaseHolder) (ApplicationLease, error)
	CompleteApplicationAttachment(context.Context, ApplicationLeaseRef, ApplicationLeaseHolder) (ApplicationLease, error)
	AttachApplicationLease(context.Context, string, string, uint64, ApplicationLeaseAttach) (ApplicationLease, error)
	ListApplicationContexts(context.Context, ApplicationLeaseRef, string, int) (ApplicationContextPage, error)
	ScheduleHints(context.Context, uint16, int, int) ([]HintBatch, error)
	RepairSchedule(context.Context, uint64, int) (uint64, int, error)
	Retire(context.Context, Ref) error
}

// OpenBackend selects a backend without waiting for external services. Redis
// Open/OpenDeferred remain explicit constructors for integration fixtures.
func OpenBackend(cfg Config) (Backend, error) {
	switch cfg.Backend {
	case "", MemoryBackend:
		return NewMemory(cfg)
	case RedisBackend:
		return OpenDeferred(cfg)
	default:
		return nil, errors.New("unknown SIP state backend")
	}
}

var _ Backend = (*Store)(nil)
var _ Backend = (*MemoryStore)(nil)
