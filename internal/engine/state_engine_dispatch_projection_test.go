package engine

import (
	"bytes"
	"encoding/json"
	"testing"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestNativeDispatchProjectionExcludesColdHistoryAndUnstartableNetworkWork(t *testing.T) {
	e := &StateEngine{cfg: Config{StateIdentityKey: bytes.Repeat([]byte{17}, 32)}}
	process := &pb.ProcessRef{InstanceId: "physical-ingress", Epoch: "epoch"}
	native := stateEngineEffect{Native: true, Permit: &pb.SendPermit{Flow: &pb.FlowRef{Ingress: process}}}
	active, err := json.Marshal(native)
	if err != nil {
		t.Fatal(err)
	}
	native.Settled = true
	settled, err := json.Marshal(native)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, kind string
		status     state.EffectStatus
		body       []byte
		worker     string
	}{
		{"pending-control", "event", state.EffectPending, nil, state.AnyDispatchWorker},
		{"completed-control", "event", state.EffectWritten, nil, ""},
		{"cancelled-credential", "credential", state.EffectCancelled, nil, ""},
		{"pending-network", "open", state.EffectPending, nil, state.AnyDispatchWorker},
		{"started-network-cannot-retry", "open", state.EffectDispatching, nil, ""},
		{"native-ready", "deliver", state.EffectAuthorized, active, e.dispatchWorker(process)},
		{"native-known-unsettled", "deliver", state.EffectWritten, active, e.dispatchWorker(process)},
		{"native-settled", "deliver", state.EffectWritten, settled, ""},
		{"native-cancelled", "deliver", state.EffectCancelled, active, ""},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			r := &stateReduction{engine: e, now: 1000, payload: state.Payload{Effects: []state.Effect{{ID: "effect", Kind: test.kind, Status: test.status, DeadlineMS: 2000, Payload: test.body, Worker: "obsolete-target"}}}}
			if err := r.projectDispatchWorkers(); err != nil {
				t.Fatal(err)
			}
			if worker := r.payload.Effects[0].Worker; worker != test.worker {
				t.Fatal("cold history or wrong executor entered the dispatch index", worker, test.worker)
			}
		})
	}
}
