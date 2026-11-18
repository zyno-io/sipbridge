// Package capture exports bounded, best-effort physical SIP observations.
package capture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const maxWire = 65536
const queueSize = 256
const batchSize = 32

// Observation contains one physical SIP message or attempted socket write.
// Wire is copied by Submit before the caller is allowed to reuse it.
type Observation struct {
	Timestamp    time.Time
	Direction    string
	Transport    string
	Source       string
	Destination  string
	Outcome      string
	BytesWritten int
	Wire         []byte
}

type queued struct {
	Observation
	Sequence uint64
}

type exporter struct {
	url      string
	instance string
	epoch    string
	client   *http.Client
	queue    chan queued
	slots    chan struct{}
	mu       sync.RWMutex
	closed   bool
	sequence atomic.Uint64
	dropped  atomic.Uint64
	failures atomic.Uint64
}

// Exporter owns the nonblocking capture queue and worker.
type Exporter struct{ *exporter }

// New creates an exporter. The destination must be an explicitly configured
// local collector URL, not an arbitrary URL containing credentials.
func New(destination, instance, epoch string) (*Exporter, error) {
	u, err := url.Parse(destination)
	if err != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/loki/api/v1/push" || instance == "" || epoch == "" {
		return nil, fmt.Errorf("invalid capture collector configuration")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	e := &exporter{url: destination, instance: instance, epoch: epoch, client: &http.Client{Timeout: 3 * time.Second, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, queue: make(chan queued, queueSize), slots: make(chan struct{}, queueSize)}
	return &Exporter{e}, nil
}

// Submit never waits for the collector. Queue overflow discards evidence, not
// signaling; counters let operators detect missing observations.
func (e *Exporter) Submit(o Observation) {
	if e == nil || e.exporter == nil {
		return
	}
	if len(o.Wire) == 0 || len(o.Wire) > maxWire {
		e.dropped.Add(1)
		return
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		e.dropped.Add(1)
		return
	}
	select {
	case e.slots <- struct{}{}:
	default:
		e.dropped.Add(1)
		return
	}
	o.Wire = bytes.Clone(o.Wire)
	item := queued{Observation: o, Sequence: e.sequence.Add(1)}
	e.queue <- item // A reserved slot guarantees space, including in-flight records.
}

// Stats returns cumulative queue-drop and failed-batch counts.
func (e *Exporter) Stats() (uint64, uint64) { return e.dropped.Load(), e.failures.Load() }

type push struct {
	Streams []stream `json:"streams"`
}
type stream struct {
	Stream map[string]string `json:"stream"`
	Values [][]interface{}   `json:"values"`
}
type line struct {
	Version      int    `json:"version"`
	Instance     string `json:"instance"`
	Epoch        string `json:"epoch"`
	Sequence     string `json:"sequence"`
	Direction    string `json:"direction"`
	Transport    string `json:"transport"`
	Source       string `json:"source"`
	Destination  string `json:"destination"`
	Outcome      string `json:"outcome"`
	BytesWritten int    `json:"bytesWritten"`
	Wire         string `json:"wire"`
	WireBytes    int    `json:"wireBytes"`
}

// displayWire keeps text readable and replaces a non-UTF-8 body as a whole.
// Content-Length and write counts still describe the original physical message.
func displayWire(wire []byte) string {
	if end := bytes.Index(wire, []byte("\r\n\r\n")); end >= 0 {
		bodyStart := end + 4
		if !utf8.Valid(wire[bodyStart:]) {
			return string(wire[:bodyStart]) + fmt.Sprintf("[%d bytes]", len(wire)-bodyStart)
		}
	}
	return string(wire)
}

// identity extracts only bounded lookup hints; API authorization and exact SIP
// parsing do not trust these values. Duplicate/invalid fields use no ID.
func identity(wire []byte) (string, string, string) {
	end := bytes.Index(wire, []byte("\r\n\r\n"))
	if end < 0 {
		return "", "", ""
	}
	head := strings.Split(string(wire[:end]), "\r\n")
	unfolded := make([]string, 0, len(head))
	for _, header := range head[1:] {
		if len(header) > 0 && (header[0] == ' ' || header[0] == '\t') && len(unfolded) > 0 {
			unfolded[len(unfolded)-1] += " " + strings.TrimSpace(header)
		} else {
			unfolded = append(unfolded, header)
		}
	}
	var id, delivery, cseq string
	var idCount, deliveryCount, cseqCount int
	for _, h := range unfolded {
		name, value, ok := strings.Cut(h, ":")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "call-id", "i":
			idCount++
			id = value
		case "x-zyno-event-id":
			deliveryCount++
			delivery = value
		case "cseq":
			cseqCount++
			cseq = value
		}
	}
	if idCount != 1 || len(id) > 512 || !utf8.ValidString(id) || strings.ContainsAny(id, "\r\n\x00") {
		id = ""
	}
	if deliveryCount != 1 || len(delivery) > 256 || !utf8.ValidString(delivery) || strings.ContainsAny(delivery, "\r\n\x00") {
		delivery = ""
	}
	fields := strings.Fields(head[0])
	method := ""
	if len(fields) >= 2 && fields[0] == "SIP/2.0" {
		if len(fields[1]) == 3 && fields[1][0] >= '1' && fields[1][0] <= '6' && fields[1][1] >= '0' && fields[1][1] <= '9' && fields[1][2] >= '0' && fields[1][2] <= '9' && cseqCount == 1 {
			parts := strings.Fields(cseq)
			if len(parts) == 2 {
				if validSequence(parts[0]) && validMethod(parts[1]) {
					method = parts[1]
				}
			}
		}
	} else if len(fields) == 3 && fields[2] == "SIP/2.0" && validMethod(fields[0]) {
		method = fields[0]
	}
	return id, delivery, method
}

func validSequence(value string) bool {
	if len(value) == 0 || len(value) > 10 {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	_, err := strconv.ParseUint(value, 10, 31)
	return err == nil
}

// Method names are case-sensitive RFC token values. Keep their length bounded
// even though unknown names never become indexed labels.
func validMethod(method string) bool {
	if len(method) == 0 || len(method) > 64 {
		return false
	}
	for _, char := range method {
		if (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || strings.ContainsRune("-.!%*_+`'~", char) {
			continue
		}
		return false
	}
	return true
}

func indexedMethod(method string) string {
	switch method {
	case "INVITE", "ACK", "BYE", "CANCEL", "OPTIONS", "REGISTER", "PRACK", "SUBSCRIBE", "NOTIFY", "PUBLISH", "INFO", "REFER", "MESSAGE", "UPDATE":
		return method
	default:
		return "OTHER"
	}
}

func encode(items []queued, instance, epoch string) ([]byte, error) {
	groups := make(map[string]*stream)
	for _, item := range items {
		id, delivery, method := identity(item.Wire)
		methodLabel := indexedMethod(method)
		bucket := "missing"
		if id != "" {
			sum := sha256.Sum256([]byte(id))
			bucket = fmt.Sprintf("%02d", sum[0]&63)
		}
		key := bucket + "/" + methodLabel
		group := groups[key]
		if group == nil {
			group = &stream{Stream: map[string]string{"job": "sipbridge", "type": "sip", "sip_bucket": bucket, "sip_method": methodLabel}}
			groups[key] = group
		}
		body, err := json.Marshal(line{Version: 1, Instance: instance, Epoch: epoch, Sequence: strconv.FormatUint(item.Sequence, 10), Direction: item.Direction, Transport: item.Transport, Source: item.Source, Destination: item.Destination, Outcome: item.Outcome, BytesWritten: item.BytesWritten, Wire: displayWire(item.Wire), WireBytes: len(item.Wire)})
		if err != nil {
			return nil, err
		}
		srcIP, srcPort, _ := net.SplitHostPort(item.Source)
		dstIP, dstPort, _ := net.SplitHostPort(item.Destination)
		metadata := map[string]string{"call_id": id, "delivery_id": delivery, "method": method, "src_ip": srcIP, "src_port": srcPort, "dst_ip": dstIP, "dst_port": dstPort, "direction": item.Direction, "transport": item.Transport, "write_outcome": item.Outcome, "capture_id": epoch + ":" + strconv.FormatUint(item.Sequence, 10)}
		group.Values = append(group.Values, []interface{}{strconv.FormatInt(item.Timestamp.UnixNano(), 10), string(body), metadata})
	}
	p := push{}
	for _, group := range groups {
		p.Streams = append(p.Streams, *group)
	}
	return json.Marshal(p)
}

func (e *exporter) send(ctx context.Context, items []queued) error {
	body, err := encode(items, e.instance, e.epoch)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := e.client.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
	_ = res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("collector status %d", res.StatusCode)
	}
	return nil
}

// Run drains records until canceled. A single failed batch is dropped so a
// failing collector cannot accumulate unbounded memory or delay shutdown.
func (e *Exporter) Run(ctx context.Context) {
	if e == nil || e.exporter == nil {
		return
	}
	defer func() {
		dropped, failures := e.Stats()
		if dropped > 0 || failures > 0 {
			slog.Warn("SIP capture stopped with losses", "dropped", dropped, "failed_batches", failures)
		}
	}()
	failed := false
	for {
		var first queued
		select {
		case <-ctx.Done():
			e.mu.Lock()
			e.closed = true
			e.mu.Unlock()
			drainCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			for len(e.queue) > 0 {
				items := make([]queued, 0, batchSize)
				for len(items) < batchSize && len(e.queue) > 0 {
					items = append(items, <-e.queue)
				}
				if err := e.send(drainCtx, items); err != nil {
					e.failures.Add(1)
					e.dropped.Add(uint64(len(items)))
				}
				for range items {
					<-e.slots
				}
				if drainCtx.Err() != nil {
					e.dropped.Add(uint64(len(e.queue)))
					return
				}
			}
			return
		case first = <-e.queue:
		}
		items := []queued{first}
		timer := time.NewTimer(10 * time.Millisecond)
	collect:
		for len(items) < batchSize {
			select {
			case item := <-e.queue:
				items = append(items, item)
			case <-timer.C:
				break collect
			case <-ctx.Done():
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if err := e.send(ctx, items); err != nil {
			e.failures.Add(1)
			e.dropped.Add(uint64(len(items)))
			if !failed {
				slog.Warn("SIP capture export unavailable", "error", err)
				failed = true
			}
		} else if failed {
			slog.Info("SIP capture export recovered")
			failed = false
		}
		for range items {
			<-e.slots
		}
	}
}
