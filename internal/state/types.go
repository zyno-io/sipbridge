// Package state implements bounded authoritative SIP execution records.
// It performs no SIP reduction or external effects.
package state

import (
	"crypto/tls"
	"errors"
	"time"
)

var (
	ErrAuthority = errors.New("SIP state authority changed or unavailable")
	ErrClock     = errors.New("SIP state clock skew; retry after clocks recover")
	ErrNotFound  = errors.New("SIP state group not found")
	ErrBusy      = errors.New("SIP state computation already claimed")
	ErrIdle      = errors.New("SIP state group has no pending input")
	ErrStale     = errors.New("SIP state claim superseded")
	ErrConflict  = errors.New("SIP state operation identity conflict")
	ErrExpired   = errors.New("SIP state operation admission expired")
	ErrCapacity  = errors.New("SIP state capacity exceeded")
	ErrVersion   = errors.New("SIP state version incompatible")
	ErrCorrupt   = errors.New("SIP state record malformed")
	ErrNotReady  = errors.New("SIP state group cannot retire")
	ErrUnknown   = errors.New("SIP state mutation outcome unknown; inspect before replay")
)

// Authority is a configuration compatibility identifier, not state-survival evidence.
// RunID is stable across Redis primary changes, but fresh for every memory store.
type Authority struct {
	Generation string `json:"generation"`
	RunID      string `json:"runId"`
}

func (a Authority) identity() string { return a.Generation + "/" + a.RunID }

type Versions struct {
	Schema  string `json:"schema"`
	Reducer string `json:"reducer"`
	Timers  string `json:"timers"`
}

type MemoryOptions struct {
	MaxGroups int
	MaxBytes  int64
}

type RedisOptions struct {
	Address           string
	SentinelAddresses []string
	SentinelMaster    string
	Username          string
	Password          string
	SentinelUsername  string
	SentinelPassword  string
	TLS               *tls.Config
	DialTimeout       time.Duration
	IOTimeout         time.Duration
	PoolSize          int
}

type Limits struct {
	GroupBytes   int
	InputBytes   int
	OutcomeBytes int
	Effects      int
	Timers       int
	Receipts     int
	Transactions int
}

type Config struct {
	// Backend is selected only by OpenBackend. Open/OpenDeferred are Redis constructors.
	Backend string
	Memory  MemoryOptions
	Redis   RedisOptions
	// Prefix must be the exclusively granted SIP namespace, never a shared firewall prefix.
	Prefix       string
	Namespace    string
	NamespaceKey []byte
	Partitions   uint16
	Generation   string
	Versions     Versions
	ClaimTTL     time.Duration
	Limits       Limits
}

// Ref may omit CallID when returned by a scheduling hint. Incarnation is required
// when acting on an already allocated logical context or retiring its group.
type Ref struct {
	ID          string `json:"id"`
	Partition   uint16 `json:"partition"`
	CallID      string `json:"callId,omitempty"`
	Incarnation string `json:"incarnation,omitempty"`
}

// Input is immutable after admission. Deadline bounds fresh admission; reclaiming
// an admitted input retains it even after that deadline so native late-message
// and completion obligations are not silently dropped. No raw credentials belong here.
type Input struct {
	// Proof uses reserved cleanup capacity instead of ordinary replay capacity.
	// Legacy internal inputs replace one marker; CAS retains it only for external cleanup commands.
	Proof               bool     `json:"proof,omitempty"`
	ID                  string   `json:"id"`
	Kind                string   `json:"kind"`
	Fingerprint         string   `json:"fingerprint"`
	Payload             []byte   `json:"payload,omitempty"`
	DeadlineMS          int64    `json:"deadlineMs"`
	ReceiptUntilMS      int64    `json:"receiptUntilMs"`
	Create              bool     `json:"create,omitempty"`
	ApplicationLeaseIDs []string `json:"applicationLeaseIds,omitempty"`
}

type EffectStatus string

const (
	EffectPending     EffectStatus = "pending"
	EffectAuthorized  EffectStatus = "authorized"
	EffectDispatching EffectStatus = "dispatching"
	EffectWritten     EffectStatus = "written"
	EffectNotWritten  EffectStatus = "not-written"
	EffectUnknown     EffectStatus = "unknown"
	EffectCancelled   EffectStatus = "cancelled"
)

// Effect metadata is bounded and authoritative. Payload is the reducer-owned
// typed encoding; this package never dispatches it or edits its status separately.
type Effect struct {
	ID               string               `json:"id"`
	Kind             string               `json:"kind"`
	Status           EffectStatus         `json:"status"`
	DeadlineMS       int64                `json:"deadlineMs"`
	AttemptID        string               `json:"attemptId,omitempty"`
	Payload          []byte               `json:"payload,omitempty"`
	ApplicationLease *ApplicationLeaseRef `json:"applicationLease,omitempty"`
}

type Timer struct {
	ID         string `json:"id"`
	Generation string `json:"generation"`
	DueMS      int64  `json:"dueMs"`
	DeadlineMS int64  `json:"deadlineMs"`
}

type Retention struct {
	Terminal bool  `json:"terminal,omitempty"`
	UntilMS  int64 `json:"untilMs,omitempty"`
}

type Payload struct {
	ProtocolState      []byte    `json:"protocolState,omitempty"`
	ActiveTransactions int       `json:"activeTransactions"`
	Effects            []Effect  `json:"effects,omitempty"`
	Timers             []Timer   `json:"timers,omitempty"`
	Retention          Retention `json:"retention"`
	// Reducer-owned reservation for already-admitted completion obligations.
	ReservedCompletionBytes int                       `json:"reservedCompletionBytes"`
	ApplicationContexts     []ApplicationContextIndex `json:"applicationContexts,omitempty"`
	ApplicationLeaseIDs     []string                  `json:"applicationLeaseIds,omitempty"`
	CommitLeaseGuards       []ApplicationLeaseRef     `json:"-"`
	CommitLeaseFences       []ApplicationLeaseRef     `json:"-"`
}

type Receipt struct {
	OperationID string `json:"operationId"`
	Fingerprint string `json:"fingerprint"`
	InputDigest string `json:"inputDigest"`
	Revision    string `json:"revision"`
	ExpiresMS   int64  `json:"expiresMs,string"`
	Outcome     []byte `json:"outcome,omitempty"`
}

type Record struct {
	Ref               Ref                         `json:"ref"`
	Authority         Authority                   `json:"authority"`
	Versions          Versions                    `json:"versions"`
	Revision          string                      `json:"revision"`
	Payload           Payload                     `json:"payload"`
	Receipts          []Receipt                   `json:"receipts,omitempty"`
	Proof             *Receipt                    `json:"proof,omitempty"`
	Pending           *Input                      `json:"pending,omitempty"`
	ApplicationLeases map[string]ApplicationLease `json:"-"`
}

type Claim struct {
	transport          *primaryObservation
	applicationEffects map[string]applicationLeaseEffect
	ID                 string
	Token              string
	Worker             string
	BaseRevision       string
	UntilMS            int64
	InputDigest        string
	Ref                Ref
	Authority          Authority
}

type Step struct {
	Record    Record
	Input     Input
	Claim     Claim
	NowMS     int64
	Recovered bool
}

type BeginResult struct {
	Step    *Step
	Receipt *Receipt
}

type Hint struct {
	Ref   Ref
	DueMS int64
}

type HintBatch struct {
	Ready      []Hint
	Due        []Hint
	ReadyError error
	DueError   error
}

type Inspection struct {
	Receipt  *Receipt
	Pending  bool
	Revision string
	NowMS    int64
}
