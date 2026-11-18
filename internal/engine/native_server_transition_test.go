package engine

import (
	"bytes"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestNativeObservedServerResponseSelectsExactlySentWire(t *testing.T) {
	for _, method := range []string{"REGISTER", "SUBSCRIBE"} {
		for _, transport := range []string{"udp", "tcp", "tls"} {
			t.Run(method+"/"+transport, func(t *testing.T) {
				wire := strings.ReplaceAll(string(stateTestRequest(method, 1, "", "observed").Wire), "SIP/2.0/TLS", "SIP/2.0/"+strings.ToUpper(transport))
				wire = strings.ReplaceAll(wire, ";rport", "")
				flow := &pb.FlowRef{Ingress: &pb.ProcessRef{InstanceId: "worker", Epoch: "epoch"}, ConnectionId: "phone", Transport: transport, SourceIp: "2001:db8::1", SourcePort: 5091, Listener: "sip.example.test:5061"}
				source := net.JoinHostPort(flow.SourceIp, strconv.Itoa(int(flow.SourcePort)))
				message, err := portable.ParseMessage([]byte(wire), transport, source, "")
				if err != nil {
					t.Fatal(err)
				}
				transaction, _, err := nativeServerStart(message, "local-tag", 1000)
				if err != nil {
					t.Fatal(err)
				}
				logical, err := nativeServerBuildResponse(transaction, 200, "OK", nil, "local-tag", "sip:edge@example.test", nil, 1001)
				if err != nil {
					t.Fatal(err)
				}
				physical, err := logical.Message.WithObservedResponseFlow(flow.SourceIp, int(flow.SourcePort))
				if err != nil {
					t.Fatal(err)
				}
				selected, err := nativeServerBuildObservedResponse(transaction, 200, "OK", nil, "local-tag", "sip:edge@example.test", nil, flow, 1001)
				if err != nil {
					t.Fatal(err)
				}
				if !selected.Message.EqualWire(physical) || selected.Transaction.LastResponse == nil || !selected.Transaction.LastResponse.EqualWire(physical) || !portable.MatchResponse(message, selected.Message) || transaction.LastResponse != nil || logical.Transaction.Key != selected.Transaction.Key || logical.Transaction.Phase != selected.Transaction.Phase || !reflect.DeepEqual(logical.Transaction.Timers, selected.Transaction.Timers) {
					t.Fatal("observed selection changed emitted wire, identity or native lifetime")
				}
				again, err := selected.Message.WithObservedResponseFlow(flow.SourceIp, int(flow.SourcePort))
				if err != nil || !again.EqualWire(selected.Message) {
					t.Fatal("physical dispatch must reuse selected wire", err)
				}
				if transport == "udp" {
					replayed, effects, err := portable.StepTransaction(selected.Transaction, portable.Input{Kind: portable.ReceiveRequest, Now: 1002, Message: message})
					if err != nil || !reflect.DeepEqual(replayed.Timers, selected.Transaction.Timers) || len(effects) != 1 || !effects[0].Replay || effects[0].Message == nil || !effects[0].Message.EqualWire(selected.Message) {
						t.Fatal("UDP duplicate changed selected response or Timer J", err)
					}
				}
				wrong := proto.Clone(flow).(*pb.FlowRef)
				wrong.SourcePort++
				if _, err = nativeServerBuildObservedResponse(transaction, 200, "OK", nil, "local-tag", "sip:edge@example.test", nil, wrong, 1001); err == nil {
					t.Fatal("mismatched observed source accepted")
				}
			})
		}
	}
}

func TestNativeObservedServerResponseCapsNormalizedPacket(t *testing.T) {
	flow := &pb.FlowRef{Ingress: &pb.ProcessRef{InstanceId: "worker", Epoch: "epoch"}, ConnectionId: "phone", Transport: "tls", SourceIp: "203.0.113.99", SourcePort: 5091, Listener: "sip.example.test:5061"}
	wire := strings.ReplaceAll(string(stateTestRequest("REGISTER", 1, "", "packet-bound").Wire), ";rport", "")
	message, err := portable.ParseMessage([]byte(wire), "tls", "203.0.113.99:5091", "")
	if err != nil {
		t.Fatal(err)
	}
	transaction, _, err := nativeServerStart(message, "local-tag", 1000)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := nativeServerBuildResponse(transaction, 200, "OK", []byte{'x'}, "local-tag", "sip:edge@example.test", nil, 1001)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte{'x'}, stateWireLimit-empty.Message.WireLen()-4)
	logical, err := nativeServerBuildResponse(transaction, 200, "OK", body, "local-tag", "sip:edge@example.test", nil, 1001)
	if err != nil || logical.Message.WireLen() > stateWireLimit {
		t.Fatal("fixture must fit before observed Via expansion", err)
	}
	if _, err = nativeServerBuildObservedResponse(transaction, 200, "OK", body, "local-tag", "sip:edge@example.test", nil, flow, 1001); err == nil {
		t.Fatal("normalized oversized packet accepted")
	}
}

func TestNativeServerRegisterCandidatesDoNotPublish(t *testing.T) {
	request := stateTestRequest("REGISTER", 1, "", "native-shared-register")
	message, err := portable.ParseMessage(request.Wire, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	started, _, err := nativeServerStart(message, "local-tag", 1_000)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := nativeServerBuildResponse(started, 200, "OK", nil, "local-tag", "sip:edge@example.test;transport=tls", nil, 1_001)
	if err != nil {
		t.Fatal(err)
	}
	if started.LastResponse != nil {
		t.Fatal("selected response modified the original transaction")
	}
	if candidate.Transaction.LastResponse == nil || candidate.Transaction.Key != started.Key {
		t.Fatal("candidate lost selected response or transaction identity")
	}
	facts, err := candidate.Message.Facts()
	if err != nil || facts.IsRequest || facts.Status != 200 || string(facts.CSeqMethod) != "REGISTER" {
		t.Fatal("selected response changed SIP identity", facts, err)
	}
	found := false
	for _, effect := range candidate.Effects {
		if (effect.Kind == portable.Send || effect.Kind == portable.Respond) && effect.Message != nil && effect.Message.WireHash() == candidate.Message.WireHash() {
			found = true
		}
	}
	if !found {
		t.Fatal("native response transition did not stage the selected wire")
	}
}

func TestNativeReliableRegisterRetirementRequiresExactCompletion(t *testing.T) {
	base := nativeReliableRegisterFinal{Method: "REGISTER", Status: 200, Transport: "tls", Written: true, Settled: true, DispatcherAcknowledged: true}
	if !nativeReliableRegisterMayRetire(base) {
		t.Fatal("completed TLS REGISTER should permit narrow retirement")
	}
	for _, scenario := range []struct {
		name string
		edit func(*nativeReliableRegisterFinal)
	}{
		{"unwritten", func(f *nativeReliableRegisterFinal) { f.Written = false }},
		{"unsettled", func(f *nativeReliableRegisterFinal) { f.Settled = false }},
		{"unacknowledged", func(f *nativeReliableRegisterFinal) { f.DispatcherAcknowledged = false }},
		{"provisional", func(f *nativeReliableRegisterFinal) { f.Status = 180 }},
		{"udp", func(f *nativeReliableRegisterFinal) { f.Transport = "udp" }},
		{"invite", func(f *nativeReliableRegisterFinal) { f.Method = "INVITE" }},
		{"subscription", func(f *nativeReliableRegisterFinal) { f.HasSubscription = true }},
		{"dialog", func(f *nativeReliableRegisterFinal) { f.HasDialog = true }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			candidate := base
			scenario.edit(&candidate)
			if nativeReliableRegisterMayRetire(candidate) {
				t.Fatal("retirement permitted without all required evidence")
			}
		})
	}
	base.Transport = "tcp"
	if !nativeReliableRegisterMayRetire(base) {
		t.Fatal("completed TCP REGISTER should permit narrow retirement")
	}
}
