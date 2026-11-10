package engine

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
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
	conn := &framingConn{writes: []partialWrite{{n: 2}, {n: 1}}}
	flow := &physicalFlow{conn: conn}
	message := []byte("first-frame")
	n, err := flow.writeWire(message)
	if err != nil || n != len(message) || conn.sent.String() != string(message) || conn.closed || conn.deadlines != 1 {
		t.Fatalf("partial success: n=%d err=%v sent=%q closed=%v deadlines=%d", n, err, conn.sent.String(), conn.closed, conn.deadlines)
	}
	if _, err = flow.writeWire([]byte("second-frame")); err != nil || conn.sent.String() != "first-framesecond-frame" {
		t.Fatalf("next frame: %v %q", err, conn.sent.String())
	}
}

func TestPartialStreamFailureCannotAppendAnotherDialogsFrame(t *testing.T) {
	failure := errors.New("injected write failure")
	conn := &framingConn{writes: []partialWrite{{n: 3, err: failure}}}
	flow := &physicalFlow{conn: conn}
	n, err := flow.writeWire([]byte("first-frame"))
	if n != 3 || !errors.Is(err, failure) || !conn.closed {
		t.Fatalf("write failure: n=%d err=%v closed=%v", n, err, conn.closed)
	}
	n, err = flow.writeWire([]byte("other-dialog-frame"))
	if n != 0 || !errors.Is(err, net.ErrClosed) || conn.sent.String() != "fir" {
		t.Fatalf("poisoned stream reused: n=%d err=%v sent=%q", n, err, conn.sent.String())
	}
}

func TestNoProgressStreamWriteClosesInsteadOfSpinning(t *testing.T) {
	conn := &framingConn{writes: []partialWrite{{n: 0}}}
	flow := &physicalFlow{conn: conn}
	if _, err := flow.writeWire([]byte("frame")); !errors.Is(err, io.ErrNoProgress) || !conn.closed {
		t.Fatalf("no progress: %v closed=%v", err, conn.closed)
	}
}
