package control

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestConfiguredLocalAuthenticationRequiresAwareControllerAndPublishesMethods(t *testing.T) {
	for _, aware := range []bool{false, true} {
		name := "old-controller"
		if aware {
			name = "aware-controller"
		}
		t.Run(name, func(t *testing.T) {
			pki := newPKI(t)
			policy := &pb.InboundAuthPolicy{RequireRegister: true, RequireInvite: true}
			_, address := testServer(t, pki, 8, func(cfg *Config) {
				cfg.Capabilities = []string{"sip.v1", "sip.local-challenge.v1"}
				cfg.InboundAuthPolicy = policy
				cfg.HeartbeatInterval, cfg.HeartbeatTimeout = time.Second, 6*time.Second
			})
			connection, err := pki.dial(address)
			if err != nil {
				t.Fatal(err)
			}
			defer connection.CloseNow()
			capabilities := []string{"sip.v1"}
			if aware {
				capabilities = append(capabilities, "sip.local-challenge.v1")
			}
			send(t, connection, &pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{
				Controller: &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}, ConnectionAttempt: 1, Major: 2,
				Capabilities: capabilities, Bounds: &pb.Bounds{MaxEnvelopeBytes: 4096},
			}}})
			reply, err := receive(connection)
			if !aware {
				if err == nil {
					t.Fatal("old controller negotiated required authentication", reply)
				}
				return
			}
			if err != nil || reply.GetHelloAck() == nil || !proto.Equal(reply.GetHelloAck().InboundAuthPolicy, policy) {
				t.Fatal("required methods were not authenticated in HelloAck", err)
			}
		})
	}
}
