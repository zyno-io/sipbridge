package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
)

type partialWrite struct {
	n   int
	err error
}
type framingConn struct {
	writes    []partialWrite
	sent      bytes.Buffer
	closed    bool
	deadlines int
}

func (c *framingConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *framingConn) Write(b []byte) (int, error) {
	if c.closed {
		return 0, net.ErrClosed
	}
	if len(c.writes) == 0 {
		return c.sent.Write(b)
	}
	r := c.writes[0]
	c.writes = c.writes[1:]
	if r.n > 0 && r.n <= len(b) {
		c.sent.Write(b[:r.n])
	}
	return r.n, r.err
}
func (c *framingConn) Close() error                     { c.closed = true; return nil }
func (c *framingConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *framingConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *framingConn) SetDeadline(time.Time) error      { return nil }
func (c *framingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *framingConn) SetWriteDeadline(time.Time) error { c.deadlines++; return nil }

func TestStreamWriteCompletesOneFrameUnderOriginalDeadline(t *testing.T) {
	writer := &evidenceWriter{reserved: &CommittedWrite{Wire: []byte("first-frame")}}
	ingress, command, conn := stateWriterFixture(t, writer)
	conn.writes = []partialWrite{{n: 2}, {n: 1}}
	result := ingress.WriteCommitted(context.Background(), command)
	if result.Outcome != pb.DeliveryResult_WRITTEN || conn.sent.String() != "first-frame" || conn.closed || conn.deadlines != 1 {
		t.Fatalf("partial success: result=%v sent=%q closed=%v deadlines=%d", result, conn.sent.String(), conn.closed, conn.deadlines)
	}
	command.Effect.Id = "second-effect"
	writer.reserved = &CommittedWrite{Wire: []byte("second-frame")}
	result = ingress.WriteCommitted(context.Background(), command)
	if result.Outcome != pb.DeliveryResult_WRITTEN || conn.sent.String() != "first-framesecond-frame" || writer.reserves != 2 {
		t.Fatalf("next committed frame: %v %q", result, conn.sent.String())
	}
}

func TestPartialStreamFailureCannotAppendAnotherDialogsFrame(t *testing.T) {
	writer := &evidenceWriter{reserved: &CommittedWrite{Wire: []byte("first-frame")}}
	ingress, command, conn := stateWriterFixture(t, writer)
	conn.writes = []partialWrite{{n: 3, err: errors.New("injected write failure")}}
	result := ingress.WriteCommitted(context.Background(), command)
	if result.Outcome != pb.DeliveryResult_UNKNOWN || !conn.closed || conn.sent.String() != "fir" {
		t.Fatalf("write failure: result=%v sent=%q closed=%v", result, conn.sent.String(), conn.closed)
	}
	command.Effect.Id = "another-dialog-effect"
	writer.reserved = &CommittedWrite{Wire: []byte("other-dialog-frame")}
	result = ingress.WriteCommitted(context.Background(), command)
	if result.Outcome != pb.DeliveryResult_NOT_WRITTEN || conn.sent.String() != "fir" {
		t.Fatalf("poisoned stream reused: %v sent=%q", result, conn.sent.String())
	}
}

func TestNoProgressStreamWriteClosesInsteadOfSpinning(t *testing.T) {
	writer := &evidenceWriter{reserved: &CommittedWrite{Wire: []byte("frame")}}
	ingress, command, conn := stateWriterFixture(t, writer)
	conn.writes = []partialWrite{{n: 0}}
	result := ingress.WriteCommitted(context.Background(), command)
	if result.Outcome != pb.DeliveryResult_UNKNOWN || !conn.closed || conn.sent.Len() != 0 || writer.reserves != 1 {
		t.Fatalf("no progress: result=%v closed=%v", result, conn.closed)
	}
}
