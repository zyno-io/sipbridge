package engine

import (
	"bytes"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func ingestNativeCancelMessage(t *testing.T, r *stateReduction, c *stateContext, message portable.Message) {
	t.Helper()
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	r.in = stateEngineInput{Kind: "ingest", Context: c.Snapshot.Context, Application: c.Snapshot.Application,
		Ingest: &pb.OwnerIngest{Context: c.Snapshot.Context, Application: c.Snapshot.Application, Flow: c.Flow, Wire: message.Wire,
			Admission: pb.OwnerIngest_AUTHENTICATED_BINDING, PrincipalId: c.Snapshot.PrincipalId, BindingId: c.Snapshot.BindingId}}
	if err := r.ingest(); err != nil {
		t.Fatal(err)
	}
}

func TestStateNativeCancelFinalOwnsItsDeadlineProofAndEmissionQuota(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			r, c, invite := advertiseFixture(t, transport, sip.INVITE, "")
			r.deadline = r.now + 2000
			r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
			advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: invite.RequestID, Status: 180}}})
			completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
			provisionalDeadline, completedProvisional := invite.RespondDeadlineMS, proto.Clone(c.Result)
			if c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.Error != "" {
				t.Fatal("short-budget180 did not positively complete")
			}
			r.now = provisionalDeadline + 500
			cancel, err := portable.BuildCancel(invite.Native.Origin)
			if err != nil {
				t.Fatal(err)
			}
			ingestNativeCancelMessage(t, r, c, cancel)
			parsed, err := cancel.Parse()
			if err != nil {
				t.Fatal(err)
			}
			cancelKey, err := sip.ServerTxKeyMake(parsed)
			if err != nil {
				t.Fatal(err)
			}
			cancelTX := c.Transactions[cancelKey]
			if cancelTX == nil || cancelTX == invite || cancelTX.Native.Family != portable.NonInviteServer {
				t.Fatal("paired native CANCEL transaction missing")
			}
			cancelIndex, _, cancelResponse := responseEffectForStatus(t, r, 200)
			finalIndex, original, final := responseEffectForStatus(t, r, 487)
			h := invite.Native.Timers[portable.TimerH]
			if cancelResponse.Transaction != cancelKey || final.Transaction != invite.Native.Key || cancelIndex >= finalIndex ||
				cancelTX.Emissions != 1 || invite.Emissions != 2 || invite.ProvisionalEmissions != 1 ||
				cancelResponse.Command != "" || final.Command != "" {
				t.Fatal("CANCEL200 and INVITE487 lost transaction ownership, native ordering or independent quota")
			}
			if !h.Active || invite.Native.Timers[portable.TimerG].Active || invite.FinalResponse.UntilMS != h.At ||
				invite.FinalResponse.DeadlineMS != 0 || invite.RespondDeadlineMS != provisionalDeadline || h.At <= r.now {
				t.Fatal("commandless487 inherited the completed180 deadline instead of its original native TimerH")
			}
			completeResponseEffectAt(t, r, cancelIndex, pb.DeliveryResult_WRITTEN)
			if invite.FinalResponse.Written || invite.FinalResponse.AnyWritten || invite.FinalResponse.FinalWritten ||
				!cancelTX.FinalResponse.Written || !proto.Equal(c.Result, completedProvisional) {
				t.Fatal("writtenCANCEL200 polluted INVITE487 proof or the earlier180 result")
			}
			completeResponseEffectAt(t, r, finalIndex, pb.DeliveryResult_NOT_WRITTEN)
			if invite.FinalResponse.RetryAtMS <= r.now || invite.FinalResponse.RetryAtMS >= h.At ||
				!proto.Equal(c.Result, completedProvisional) {
				t.Fatal("positive no-start487 lost its native retry after the unrelated180 deadline")
			}
			r.now = invite.FinalResponse.RetryAtMS
			if err := r.recoverServerResponses(); err != nil {
				t.Fatal(err)
			}
			_, retry, retried := responseEffectForStatus(t, r, 487)
			if retry.ID == original.ID || !bytes.Equal(final.Wire, retried.Wire) || !proto.Equal(final.Flow, retried.Flow) ||
				retried.Transaction != invite.Native.Key || retried.Command != "" || invite.Native.Timers[portable.TimerH] != h ||
				invite.FinalResponse.UntilMS != h.At || invite.RespondDeadlineMS != provisionalDeadline ||
				invite.Emissions != 3 || cancelTX.Emissions != 1 {
				t.Fatal("native487 recovery changed bytes/flow, transaction quotas or original deadlines")
			}
			completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
			if !invite.FinalResponse.Written || !proto.Equal(c.Result, completedProvisional) {
				t.Fatal("actual487 write lost proof or acknowledged the earlier180 again")
			}

			// A real retransmitted CANCEL uses its own retained transaction even
			// though its branch also matches the related INVITE transaction.
			inviteEmissions, cancelRetire := invite.Emissions, cancelTX.RetireMS
			ingestNativeCancelMessage(t, r, c, cancel)
			_, _, replay := responseEffectForStatus(t, r, 200)
			if c.Transactions[cancelKey] != cancelTX || cancelTX.Emissions != 2 || cancelTX.RetireMS != cancelRetire ||
				replay.Transaction != cancelKey || invite.Emissions != inviteEmissions || invite.FinalResponse.Status != 487 ||
				!invite.FinalResponse.Written || !proto.Equal(c.Result, completedProvisional) {
				t.Fatal("CANCEL replay reset its proof/quota or displaced the retained INVITE final")
			}
			completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
			for cancelTX.Emissions < stateProfileTransmissions {
				ingestNativeCancelMessage(t, r, c, cancel)
				completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
			}
			count := len(r.payload.Effects)
			ingestNativeCancelMessage(t, r, c, cancel)
			if cancelTX.Emissions != stateProfileTransmissions || len(r.payload.Effects) != count ||
				invite.Emissions != inviteEmissions || cancelTX.RetireMS != cancelRetire {
				t.Fatal("repeated CANCEL escaped its retained final quota or charged/restarted the INVITE")
			}
			ack, err := portable.BuildNon2xxACK(invite.Native.Origin, *invite.Native.LastResponse)
			if err != nil {
				t.Fatal(err)
			}
			ingestNativeCancelMessage(t, r, c, ack)
			if invite.Native.Phase != portable.Terminated || invite.Native.Timers[portable.TimerH].Active {
				t.Fatal("native ACK failed to finish the reliable INVITE487 transaction")
			}
			r.now += invite.Native.Profile.T1
			if err := r.recoverServerResponses(); err != nil || invite.Emissions != inviteEmissions ||
				invite.FinalResponse.RetryAtMS != 0 || !proto.Equal(c.Result, completedProvisional) {
				t.Fatalf("ACK/termination restarted native487 recovery or rewrote an old command result: %v", err)
			}
		})
	}
}

func TestStateResponseUntilKeepsExplicitRespondAndAutomaticTryingBounds(t *testing.T) {
	r, _, tx := provisionalResponseFixture(t)
	tx.RespondDeadlineMS = r.now + 2000
	tx.RetireMS = r.now + 120000
	if got := stateResponseUntil(tx, 180, "explicit180"); got != tx.RespondDeadlineMS {
		t.Fatalf("explicit provisional Respond deadline changed: %d", got)
	}
	if got := stateResponseUntil(tx, 100, ""); got != 0 {
		t.Fatalf("automatic100 acquired an application/helper budget: %d", got)
	}
	tx.Native.Timers[portable.TimerH] = portable.Deadline{Active: true, At: r.now + 32000}
	if got := stateResponseUntil(tx, 487, "explicit487"); got != tx.RespondDeadlineMS {
		t.Fatalf("explicit final Respond deadline changed: %d", got)
	}
	if got := stateResponseUntil(tx, 487, ""); got != tx.Native.Timers[portable.TimerH].At {
		t.Fatalf("commandless native final inherited an unrelated Respond deadline: %d", got)
	}
}
