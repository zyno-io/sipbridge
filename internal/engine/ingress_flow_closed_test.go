package engine

import (
	"net"
	"testing"
	"time"

	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

func TestIngressFlowClosedCallbackObservesRemovedFlowOutsideLocks(t *testing.T) {
	process := &pb.ProcessRef{InstanceId: "sipbridge", Epoch: "one"}
	var ingress *Ingress
	var physical *physicalFlow
	closed := make(chan *pb.FlowRef, 1)
	ingress = NewIngress(process, new(control.Server), "sip", 4, IngressOptions{OnFlowClosed: func(flow *pb.FlowRef) {
		if ingress.LiveFlow(flow) {
			t.Error("native close callback observed an active physical flow")
		}
		if !physical.write.TryLock() {
			t.Error("native close callback ran under the physical write lock")
		} else {
			physical.write.Unlock()
		}
		closed <- flow
	}})
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	var err error
	physical, err = ingress.addStream(local, "192.0.2.1", 12345, "tls")
	if err != nil {
		t.Fatal(err)
	}
	if !ingress.LiveFlow(physical.ref) {
		t.Fatal("initial phone flow unavailable")
	}
	go ingress.remove(physical)
	select {
	case flow := <-closed:
		if flow != physical.ref {
			t.Fatal("native close callback received another flow")
		}
	case <-time.After(time.Second):
		t.Fatal("native close callback missing or retained ingress locks")
	}
}
