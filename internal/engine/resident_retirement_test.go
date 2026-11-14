package engine

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestResidentTerminalRegisterRetainsCompactDisposition(t *testing.T) {
	e, c, tx := residentSubscriptionFixture(t)
	request := stateTestRequest("REGISTER", 1, "", "register")
	native, err := portable.NewTransaction(portable.NonInviteServer, portable.RestoreMessage(request.Wire, "tls", "", ""), portable.DefaultTimers(), "local")
	if err != nil {
		t.Fatal(err)
	}
	native.Phase = portable.Terminated
	tx.native = native
	tx.selectors = []string{"selector"}
	tx.charged = 2048
	tx.policy = &pb.CommandAdmission{DirectoryEpoch: "dead-policy"}
	tx.credential = &pb.CredentialRef{Id: "dead-reference", Version: "one"}
	tx.presentDeadline = time.Now().Add(time.Second)
	flow, requestID, presentDeadline := tx.flow, tx.requestID, tx.presentDeadline
	c.snapshot.Kind = pb.ContextSnapshot_TRANSACTION
	c.transactions = map[string]*residentTransaction{native.Key: tx}
	c.requests = map[string]string{"request": native.Key}
	c.dialogs = make(map[string]*residentDialog)
	c.outputs = map[string]*residentOutput{"proof": {}}
	c.bytes = 4096
	c.reserved = 512
	e.contexts = map[string]*residentContext{c.snapshot.Context.Id: c}
	e.transactions = map[string]*residentContext{"selector": c}
	e.bytes = c.bytes
	e.reserved = c.reserved
	e.retireResidentTransactionLocked(c, tx)
	if !tx.retired || len(tx.native.Origin.WireBytes()) != 0 || tx.wireHash != sha256.Sum256(request.Wire) || e.transactions["selector"] != c {
		t.Fatal("terminal request did not retain compact exact disposition")
	}
	if tx.policy != nil || tx.application != nil || tx.credential != nil || tx.flow != flow || tx.requestID != requestID || tx.presentDeadline != presentDeadline {
		t.Fatal("retirement retained dead references or cleared admitted-notification/replay fields")
	}
	if e.contexts[c.snapshot.Context.Id] != c {
		t.Fatal("retired context while physical proof exists")
	}
	delete(c.outputs, "proof")
	tx.retireAt = time.Now().Add(-time.Second)
	e.afterResidentOutputReleaseLocked(c)
	if e.contexts[c.snapshot.Context.Id] != nil || e.transactions["selector"] != nil || e.bytes != 0 || e.reserved != 0 {
		t.Fatal("settled context or accounting retained")
	}
}

func TestResidentSubscriptionTransactionRetiresWithoutRetiringSubscription(t *testing.T) {
	e, c, tx := residentSubscriptionFixture(t)
	native := tx.native
	native.Phase = portable.Terminated
	tx.native = native
	tx.selectors = []string{"old-transaction"}
	tx.charged = 2048
	c.subscription = &stateSubscription{Event: "message-summary"}
	c.transactions = map[string]*residentTransaction{native.Key: tx}
	c.requests = map[string]string{"request": native.Key}
	c.dialogs = make(map[string]*residentDialog)
	c.outputs = make(map[string]*residentOutput)
	c.bytes = 4096
	e.bytes = c.bytes
	e.contexts = map[string]*residentContext{c.snapshot.Context.Id: c}
	e.transactions = map[string]*residentContext{"old-transaction": c}
	e.retireResidentTransactionLocked(c, tx)
	if !tx.retired || len(tx.native.Origin.WireBytes()) != 0 {
		t.Fatal("terminal subscription transaction retained its wire")
	}
	tx.retireAt = time.Now().Add(-time.Second)
	handled := e.fireResidentTransactionRetirement(residentDeadline{key: residentDeadlineKey{kind: residentTransactionRetirementDeadline, contextID: c.snapshot.Context.Id, record: native.Key}, generation: uint64(tx.retireAt.UnixMilli())})
	if !handled || len(c.transactions) != 0 || e.transactions["old-transaction"] != nil || e.contexts[c.snapshot.Context.Id] != c || c.subscription == nil {
		t.Fatal("transaction cutoff disturbed live subscription or retained history")
	}
}

func TestResidentTransactionCompactionReleasesReceiptReference(t *testing.T) {
	e, c, tx := residentSubscriptionFixture(t)
	e.maxBytes = 64 << 20
	e.deadlineWake = make(chan struct{}, 1)
	request := stateTestRequest("REGISTER", 1, "", "receipt-retirement")
	native, err := portable.NewTransaction(portable.NonInviteServer, portable.RestoreMessage(request.Wire, "tls", "", ""), portable.DefaultTimers(), "local")
	if err != nil {
		t.Fatal(err)
	}
	native.Phase = portable.Terminated
	tx.native = native
	c.transactions = map[string]*residentTransaction{native.Key: tx}
	c.dialogs = make(map[string]*residentDialog)
	c.outputs = make(map[string]*residentOutput)
	e.contexts = map[string]*residentContext{c.snapshot.Context.Id: c}
	tx.budget, err = e.openResidentBudgetLocked(c, residentSmallBudget)
	if err != nil {
		t.Fatal(err)
	}
	receipt := newResidentCommand("retiring", 1)
	receipt.engine = e
	receipt.budget = tx.budget
	if err = e.prepareResidentCommandLocked(c, receipt, tx.budget); err != nil {
		t.Fatal(err)
	}
	receipt.registered = true
	c.command = receipt
	c.receiptPins = 1
	tx.command = receipt
	retainResidentCommandLocked(receipt)
	finishResidentCommandLocked(c, receipt, &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED})
	c.command = nil
	releaseResidentCommandLocked(c, receipt)
	e.retireResidentTransactionLocked(c, tx)
	if !tx.retired || tx.command != nil || receipt.references != 1 {
		t.Fatal("transaction compaction did not release its receipt reference")
	}
	if _, err = awaitResidentCommand(context.Background(), c, receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.references != 0 || receipt.baseBytes != 0 || c.receiptPins != 0 {
		t.Fatal("final waiter did not reclaim receipt marker")
	}
}
