package engine

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"testing"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestFramedObservationPreservesPublicProjectionAndValidation(t *testing.T) {
	for _, sample := range selectorWireCases() {
		if sample.outbound {
			continue
		}
		t.Run(sample.name, func(t *testing.T) {
			parser := sip.NewParser().NewSIPStream()
			defer parser.Close()
			if _, err := parser.Write(sample.wire); err != nil {
				t.Fatal(err)
			}
			parsed, size, err := parser.ParseNext()
			if err != nil || size != len(sample.wire) {
				t.Fatal("fixture framing failed", err)
			}
			want, wantErr := Observe(sample.wire, false)
			got, gotErr := observeFramedMessage(sample.wire, parsed)
			if wantErr != nil {
				if gotErr == nil || gotErr.Error() != wantErr.Error() {
					t.Fatal("stream ingress bypassed SIP identity validation", gotErr)
				}
				return
			}
			if gotErr != nil || !proto.Equal(got, want) {
				t.Fatal("parsed reuse changed public message", gotErr)
			}
		})
	}
}

func TestFramedObservationOwnsBodyAcrossSubsequentStreamMessages(t *testing.T) {
	small := selectorWireCases()[0].wire
	body := bytes.Repeat([]byte("large-session\r\n"), 1500)
	large := bytes.Replace(small, []byte("Content-Length: 4"), []byte("Content-Length: "+strconv.Itoa(len(body))), 1)
	large = append(large[:len(large)-4], body...)
	stream := sip.NewParser().NewSIPStream()
	defer stream.Close()
	var messages []*pb.SipMessage
	for _, wire := range [][]byte{large, small, small} {
		for start := 0; start < len(wire); start += 333 {
			if _, err := stream.Write(wire[start:min(start+333, len(wire))]); err != nil {
				t.Fatal(err)
			}
			parsed, _, err := stream.ParseNext()
			if errors.Is(err, io.ErrUnexpectedEOF) {
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			message, err := observeFramedMessage(wire, parsed)
			if err != nil {
				t.Fatal(err)
			}
			messages = append(messages, message)
		}
	}
	if len(messages) != 3 || !bytes.Equal(messages[0].Body, body) || !bytes.Equal(messages[1].Body, []byte("body")) || !bytes.Equal(messages[2].Body, []byte("body")) {
		t.Fatal("subsequent stream use changed a previously observed body")
	}
}
