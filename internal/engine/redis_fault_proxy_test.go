package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"sipbridge/internal/state"
)

const redisFaultFrameLimit = 16 << 20

// Keep the actual bytes for transparent forwarding, including the RESP3 HELLO
// response. Never print a frame: bulk values may contain SIP or credentials.
type redisFaultFrame struct {
	kind     byte
	scalar   []byte
	children []redisFaultFrame
	raw      []byte
}

func readRedisFaultFrame(reader *bufio.Reader, depth int, remaining *int) (redisFaultFrame, error) {
	var frame redisFaultFrame
	if depth > 32 || *remaining <= 0 {
		return frame, errors.New("Redis fault proxy frame bound exceeded")
	}
	header, err := reader.ReadSlice('\n')
	if err != nil {
		return frame, err
	}
	if len(header) < 3 || header[len(header)-2] != '\r' || len(header) > *remaining {
		return frame, errors.New("invalid Redis fault proxy header")
	}
	*remaining -= len(header)
	frame.kind = header[0]
	frame.raw = append(frame.raw, header...)
	line := string(header[1 : len(header)-2])
	switch frame.kind {
	case '+', '-', ':', ',', '(', '#', '_':
		frame.scalar = []byte(line)
		return frame, nil
	case '$', '!', '=':
		length, err := strconv.Atoi(line)
		if err != nil || length < -1 || length > *remaining-2 {
			return frame, errors.New("invalid Redis fault proxy bulk bound")
		}
		if length == -1 {
			return frame, nil
		}
		payload := make([]byte, length+2)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return frame, err
		}
		if !bytes.Equal(payload[length:], []byte("\r\n")) {
			return frame, errors.New("invalid Redis fault proxy bulk terminator")
		}
		*remaining -= len(payload)
		frame.raw = append(frame.raw, payload...)
		frame.scalar = payload[:length]
		return frame, nil
	case '*', '%', '~', '>', '|':
		length, err := strconv.Atoi(line)
		if err != nil || length < -1 || length > 65536 {
			return frame, errors.New("invalid Redis fault proxy aggregate bound")
		}
		if length == -1 {
			return frame, nil
		}
		if frame.kind == '%' || frame.kind == '|' {
			length *= 2
		}
		for n := 0; n < length; n++ {
			child, err := readRedisFaultFrame(reader, depth+1, remaining)
			if err != nil {
				return frame, err
			}
			frame.raw = append(frame.raw, child.raw...)
			frame.children = append(frame.children, child)
		}
		return frame, nil
	default:
		return frame, errors.New("unsupported Redis fault proxy frame kind")
	}
}

func writeRedisFaultFrame(conn net.Conn, raw []byte) error {
	for len(raw) > 0 {
		n, err := conn.Write(raw)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(raw) {
			return io.ErrShortWrite
		}
		raw = raw[n:]
	}
	return nil
}

type redisReservationFaultTarget struct {
	groupKey, operation, effect, attempt string
	partition                            bool
}

type redisReservationCommitHit struct {
	receipt state.Receipt
}

type redisReservationFaultProxy struct {
	address  string
	upstream string
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	target   *redisReservationFaultTarget
	fired    bool
	closed   bool
	isolated bool
	success  int
	err      error
	hit      chan redisReservationCommitHit
	release  chan struct{}
	once     sync.Once
	wg       sync.WaitGroup
}

func newRedisReservationFaultProxy(t *testing.T, upstream string) *redisReservationFaultProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("create Redis fault proxy listener")
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := &redisReservationFaultProxy{address: listener.Addr().String(), upstream: upstream, listener: listener, ctx: ctx, cancel: cancel,
		conns: make(map[net.Conn]struct{}), hit: make(chan redisReservationCommitHit, 1), release: make(chan struct{})}
	proxy.wg.Add(1)
	go func() {
		defer proxy.wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			if !proxy.track(client) {
				continue
			}
			proxy.wg.Add(1)
			go func() { defer proxy.wg.Done(); proxy.forward(client) }()
		}
	}()
	t.Cleanup(func() {
		cancel()
		proxy.mu.Lock()
		proxy.closed = true
		proxy.mu.Unlock()
		listener.Close()
		proxy.disconnect()
		done := make(chan struct{})
		go func() { proxy.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Redis fault proxy did not stop")
		}
	})
	return proxy
}

func (p *redisReservationFaultProxy) track(conn net.Conn) bool {
	p.mu.Lock()
	allowed := !p.closed && !p.isolated
	if allowed {
		p.conns[conn] = struct{}{}
	}
	p.mu.Unlock()
	if !allowed {
		conn.Close()
	}
	return allowed
}

func (p *redisReservationFaultProxy) untrack(conn net.Conn) {
	conn.Close()
	p.mu.Lock()
	delete(p.conns, conn)
	p.mu.Unlock()
}

func (p *redisReservationFaultProxy) disconnect() {
	p.mu.Lock()
	conns := make([]net.Conn, 0, len(p.conns))
	for conn := range p.conns {
		conns = append(conns, conn)
	}
	p.mu.Unlock()
	for _, conn := range conns {
		conn.Close()
	}
}

func (p *redisReservationFaultProxy) heal() {
	p.mu.Lock()
	p.isolated = false
	p.mu.Unlock()
}

func (p *redisReservationFaultProxy) isolateWriter(target net.Conn) {
	p.mu.Lock()
	p.isolated = true
	others := make([]net.Conn, 0, len(p.conns))
	for conn := range p.conns {
		if conn != target {
			others = append(others, conn)
		}
	}
	p.mu.Unlock()
	for _, conn := range others {
		conn.Close()
	}
	// Release the failed Commit waiter only after every other pooled socket
	// is closed and new connections are refused. Its receipt inspection must
	// not race a still-healthy pre-existing connection through this proxy.
	target.Close()
}

func (p *redisReservationFaultProxy) releaseReplyLoss() { p.once.Do(func() { close(p.release) }) }

func (p *redisReservationFaultProxy) arm(target redisReservationFaultTarget) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = &target
}

func (p *redisReservationFaultProxy) status() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.success, p.err
}

func (p *redisReservationFaultProxy) fail() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed && !p.isolated && p.err == nil {
		p.err = errors.New("Redis fault proxy framing or forwarding failed")
	}
}

func (p *redisReservationFaultProxy) forward(client net.Conn) {
	defer p.untrack(client)
	dial, cancel := context.WithTimeout(p.ctx, 2*time.Second)
	upstream, err := new(net.Dialer).DialContext(dial, "tcp", p.upstream)
	cancel()
	if err != nil {
		p.fail()
		return
	}
	if !p.track(upstream) {
		return
	}
	defer p.untrack(upstream)
	commands, replies := bufio.NewReader(client), bufio.NewReader(upstream)
	for {
		bound := redisFaultFrameLimit
		command, err := readRedisFaultFrame(commands, 0, &bound)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				p.fail()
			}
			return
		}
		if err := writeRedisFaultFrame(upstream, command.raw); err != nil {
			p.fail()
			return
		}
		for {
			bound = redisFaultFrameLimit
			reply, err := readRedisFaultFrame(replies, 0, &bound)
			if err != nil {
				p.fail()
				return
			}
			// Attribute and push frames do not consume the command's reply.
			if reply.kind == '|' || reply.kind == '>' {
				if err := writeRedisFaultFrame(client, reply.raw); err != nil {
					p.fail()
					return
				}
				continue
			}
			target, receipt, matched := p.matchCommit(command, reply)
			if matched {
				// The real successful response has been fully consumed. The
				// test now verifies the server mutation through a direct client.
				p.hit <- redisReservationCommitHit{receipt: receipt}
				select {
				case <-p.release:
				case <-p.ctx.Done():
					return
				}
				if target.partition {
					p.isolateWriter(client)
				}
				// Close without forwarding any part of this successful reply.
				return
			}
			if err := writeRedisFaultFrame(client, reply.raw); err != nil {
				p.fail()
				return
			}
			break
		}
	}
}

func (p *redisReservationFaultProxy) matchCommit(command, reply redisFaultFrame) (redisReservationFaultTarget, state.Receipt, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var empty redisReservationFaultTarget
	var receipt state.Receipt
	if p.target == nil || command.kind != '*' || len(command.children) < 3 {
		return empty, receipt, false
	}
	args := command.children
	name := strings.ToUpper(string(args[0].scalar))
	if name != "EVAL" && name != "EVALSHA" {
		return empty, receipt, false
	}
	keyCount, err := strconv.Atoi(string(args[2].scalar))
	if err != nil || keyCount != 3 || len(args) < 3+keyCount+14 || string(args[3].scalar) != p.target.groupKey {
		return empty, receipt, false
	}
	argv := args[3+keyCount:]
	if json.Unmarshal(argv[13].scalar, &receipt) != nil || receipt.OperationID != p.target.operation {
		return empty, state.Receipt{}, false
	}
	var payload state.Payload
	if json.Unmarshal(argv[11].scalar, &payload) != nil {
		return empty, state.Receipt{}, false
	}
	selected := false
	for _, effect := range payload.Effects {
		if effect.ID != p.target.effect || effect.Kind != "deliver" || effect.AttemptID != p.target.attempt || effect.Status != state.EffectDispatching {
			continue
		}
		var body stateEngineEffect
		if json.Unmarshal(effect.Payload, &body) == nil && len(body.Wire) == 0 {
			selected = true
		}
	}
	// NOSCRIPT is passed through, allowing go-redis to send the normal EVAL.
	if !selected || reply.kind != '*' || len(reply.children) != 4 || string(reply.children[0].scalar) != "SIPSTATE-CLOCK-V1" {
		return empty, state.Receipt{}, false
	}
	result := reply.children[3]
	if result.kind != '*' || len(result.children) != 2 || string(result.children[0].scalar) != "RECEIPT" || !bytes.Equal(result.children[1].scalar, argv[13].scalar) || receipt.Revision != string(argv[7].scalar) {
		return empty, state.Receipt{}, false
	}
	p.success++
	if p.fired {
		return empty, receipt, false
	}
	p.fired = true
	return *p.target, receipt, true
}
