package engine

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"sipbridge/internal/capture"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

type observedCapture struct {
	Direction    string `json:"direction"`
	Transport    string `json:"transport"`
	Outcome      string `json:"outcome"`
	BytesWritten int    `json:"bytesWritten"`
	Wire         string `json:"wire"`
}

func captureFixture(t *testing.T) (*capture.Exporter, <-chan observedCapture) {
	t.Helper()
	observed := make(chan observedCapture, 20)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Streams []struct {
				Values [][]json.RawMessage `json:"values"`
			} `json:"streams"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("capture request: %v", err)
			w.WriteHeader(400)
			return
		}
		for _, stream := range payload.Streams {
			for _, value := range stream.Values {
				var encoded string
				var record observedCapture
				if len(value) < 2 || json.Unmarshal(value[1], &encoded) != nil || json.Unmarshal([]byte(encoded), &record) != nil {
					t.Error("invalid capture record")
					continue
				}
				observed <- record
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	exporter, err := capture.New(server.URL+"/loki/api/v1/push", "fixture", "epoch")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); exporter.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done; server.Close() })
	return exporter, observed
}

func nextCapture(t *testing.T, observed <-chan observedCapture) observedCapture {
	t.Helper()
	select {
	case record := <-observed:
		return record
	case <-time.After(3 * time.Second):
		t.Fatal("capture was not exported")
		return observedCapture{}
	}
}

func TestPhysicalStreamCaptureUsesFramedPlaintextForTCPAndTLS(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			exporter, observed := captureFixture(t)
			client, server := outboundStreamPair(t, transport)
			ingress := NewIngress(&pb.ProcessRef{InstanceId: "fixture", Epoch: "epoch"}, new(control.Server), "127.0.0.1:5060", 8, IngressOptions{Capture: exporter})
			flow, err := ingress.addStream(client, "127.0.0.1", 5060, transport)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() { defer close(done); ingress.readStream(ctx, flow) }()
			// Incomplete mandatory headers make Observe reject after the framing hook.
			wire := []byte("OPTIONS sip:x SIP/2.0\r\nCall-ID: captured\r\nContent-Length: 0\r\n\r\n")
			if _, err := server.Write(wire[:10]); err != nil {
				t.Fatal(err)
			}
			if _, err := server.Write(wire[10:]); err != nil {
				t.Fatal(err)
			}
			record := nextCapture(t, observed)
			if record.Direction != "in" || record.Transport != transport || string(record.Wire) != string(wire) {
				t.Fatalf("unexpected capture: %+v", record)
			}
			client.Close()
			<-done
		})
	}
}

func TestPhysicalUDPCapturePrecedesSIPParsing(t *testing.T) {
	exporter, observed := captureFixture(t)
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := net.Dial("udp", listener.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	ingress := NewIngress(&pb.ProcessRef{InstanceId: "fixture", Epoch: "epoch"}, new(control.Server), listener.LocalAddr().String(), 8, IngressOptions{Capture: exporter})
	done := make(chan struct{})
	go func() { defer close(done); _ = ingress.ServeUDP(ctx, listener) }()
	wire := []byte("not a valid SIP message")
	if _, err := client.Write(wire); err != nil {
		t.Fatal(err)
	}
	record := nextCapture(t, observed)
	if record.Direction != "in" || record.Transport != "udp" || string(record.Wire) != string(wire) {
		t.Fatalf("unexpected UDP capture: %+v", record)
	}
	waitOutboundStream(t, func() bool { return ingress.Statistics().InvalidMessages == 1 })
	cancel()
	<-done
}

func TestPhysicalPartialWriteIsNotACompleteCapture(t *testing.T) {
	exporter, observed := captureFixture(t)
	conn := &framingConn{writes: []partialWrite{{n: 3, err: io.ErrClosedPipe}}}
	flow := &physicalFlow{conn: conn, ref: &pb.FlowRef{Transport: "tcp"}, capture: exporter}
	if _, err := flow.writeWire([]byte("INVITE sip:x SIP/2.0\r\n\r\n")); err == nil {
		t.Fatal("expected partial failure")
	}
	record := nextCapture(t, observed)
	if record.Outcome != "partial" || record.BytesWritten != 3 {
		t.Fatalf("unexpected partial capture: %+v", record)
	}
}
