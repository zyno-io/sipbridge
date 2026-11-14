package engine

import (
	"context"
	"sync"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

// ResidentMemoryOptions limits the actual resident records and completion
// obligations. Authority names this process-lifetime memory authority.
type ResidentMemoryOptions struct {
	Authority   *pb.StateAuthority
	MaxContexts int
	MaxBytes    int64
}

// ResidentMemory owns the process-local SIP authority. mu protects indexes,
// accounting, and the deadline heap; protocol values have per-context locks.
type ResidentMemory struct {
	cfg       Config
	control   *control.Server
	authority *pb.StateAuthority
	ctx       context.Context
	cancel    context.CancelFunc

	mu              sync.Mutex
	closed          bool
	contexts        map[string]*residentContext
	transactions    map[string]*residentContext
	dialogs         map[string]*residentContext
	admissions      map[string]residentAdmission
	admissionByWire map[[32]byte]string
	admissionCounts map[string]int
	admissionSerial uint64
	deadlines       residentDeadlines
	deadlineWake    chan struct{}
	reserved        int64
	bytes           int64
	maxBytes        int64
	maxContexts     int
	owners          map[string]map[*residentContext]struct{}
	leases          map[string]*residentLease
	work            chan struct{}
	ingressWork     chan struct{}
	eventWork       chan struct{}
	callbackMetrics callbackStatistics
	wg              sync.WaitGroup
}

// residentContext owns only the records for one application context, rather
// than an aggregate of every context sharing a Call-ID.
type residentContext struct {
	receiptPins          int
	reservedOutputs      int
	reservedEvents       int
	ownerKey             string
	directUnbound        bool
	ownerLost            bool
	ownerCleanupAt       time.Time
	borrowed             int64
	budget               *residentBudget
	subscriptionBudget   *residentBudget
	requiredNotifyBudget *residentBudget
	subscriptionBorrowed int64
	retireReady          bool
	retireGeneration     uint64
	bytes                int64
	reserved             int64
	mu                   sync.Mutex
	snapshot             *pb.ContextSnapshot
	flow                 *pb.FlowRef
	revision             uint64
	transactions         map[string]*residentTransaction
	requests             map[string]string
	dialogs              map[string]*residentDialog
	outputs              map[string]*residentOutput
	events               map[string]*residentEvent
	subscription         *stateSubscription
	selectedDialog       string
	cancelled            bool
	command              *residentCommand
	commandOperation     string
	commandFingerprint   [32]byte
}

type residentAdmission struct {
	callID   string
	wireHash [32]byte
	expires  time.Time
}

type residentTransaction struct {
	direct                  bool
	decisionPending         bool
	decisionProposal        *pb.FlowAdoptionRequest
	requestDialog           string
	emissions               uint16
	provisionalEmissions    uint16
	policy                  *pb.CommandAdmission
	budget                  *residentBudget
	borrowed                int64
	retireAt                time.Time
	selectors               []string
	charged                 int64
	retired                 bool
	reliableRegisterWritten bool
	wireHash                [32]byte
	command                 *residentCommand
	cleanup                 bool
	credential              *pb.CredentialRef
	digestAttempts          uint32
	digestHandled           bool
	deadline                time.Time
	native                  portable.Transaction
	flow                    *pb.FlowRef
	requestID               string
	application             *pb.ProcessRef
	presentDeadline         time.Time
}

type residentDialog struct {
	completionFlow     *pb.FlowRef
	completionBorrowed int64
	borrowed           int64
	budget             *residentBudget
	ackWritten         bool
	transaction        string
	localTransaction   string
	native             portable.Dialog
}
