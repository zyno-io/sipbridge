package engine

import (
	"bufio"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var preflightRedisCounters sync.Map

// Count only this fixture's connections, without observing another package's
// Redis traffic or retaining/logging any command arguments. Redis replies pass
// through unchanged; the proxy has no protocol state or retry behavior.
func preflightRedisProxy(t *testing.T) string {
	t.Helper()
	upstream := os.Getenv("SIPBRIDGE_STATE_TEST_REDIS")
	if upstream == "" {
		t.Skip("set SIPBRIDGE_STATE_TEST_REDIS for Redis-native preflight qualification")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	address := listener.Addr().String()
	preflightRedisCounters.Store(address, &calls)
	var workers sync.WaitGroup
	var connectionMu sync.Mutex
	connections := make(map[net.Conn]struct{})
	closing := false
	track := func(connection net.Conn) bool {
		connectionMu.Lock()
		defer connectionMu.Unlock()
		if closing {
			connection.Close()
			return false
		}
		connections[connection] = struct{}{}
		return true
	}
	closeConnection := func(connection net.Conn) {
		connection.Close()
		connectionMu.Lock()
		delete(connections, connection)
		connectionMu.Unlock()
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			if !track(client) {
				return
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer closeConnection(client)
				server, err := net.DialTimeout("tcp", upstream, time.Second)
				if err != nil {
					return
				}
				if !track(server) {
					return
				}
				defer closeConnection(server)
				repliesDone := make(chan struct{})
				go func() {
					defer close(repliesDone)
					_, _ = io.Copy(client, server)
				}()
				defer func() {
					client.Close()
					server.Close()
					<-repliesDone
				}()
				reader := bufio.NewReader(client)
				for {
					line, err := reader.ReadString('\n')
					if err != nil || len(line) < 3 || line[0] != '*' {
						return
					}
					arguments, err := strconv.Atoi(strings.TrimSpace(line[1:]))
					if err != nil || arguments < 1 {
						return
					}
					if _, err = io.WriteString(server, line); err != nil {
						return
					}
					for index := 0; index < arguments; index++ {
						line, err = reader.ReadString('\n')
						if err != nil || len(line) < 3 || line[0] != '$' {
							return
						}
						size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
						if err != nil || size < 0 || size > 1<<20 {
							return
						}
						value := make([]byte, size+2)
						if _, err = io.ReadFull(reader, value); err != nil {
							return
						}
						if index == 0 && strings.EqualFold(string(value[:size]), "evalsha") {
							calls.Add(1)
						}
						if _, err = io.WriteString(server, line); err != nil {
							return
						}
						if _, err = server.Write(value); err != nil {
							return
						}
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		<-stopped
		connectionMu.Lock()
		closing = true
		for connection := range connections {
			connection.Close()
		}
		connectionMu.Unlock()
		workers.Wait()
		preflightRedisCounters.Delete(address)
	})
	return address
}

func TestStateControlPreflightProxyClosesUnownedConnections(t *testing.T) {
	connected := make(chan net.Conn, 1)
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		t.Run("abandoned-client", func(t *testing.T) {
			address := preflightRedisProxy(t)
			connection, err := net.DialTimeout("tcp", address, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			// Deliberately omit owner cleanup, as when bootstrap fails before
			// Store.Close has been registered. The parent closes after testing.
			connected <- connection
			connection.SetDeadline(time.Now().Add(time.Second))
			if _, err = io.WriteString(connection, "*1\r\n$4\r\nPING\r\n"); err != nil {
				t.Fatal(err)
			}
			reply, err := bufio.NewReader(connection).ReadString('\n')
			if err != nil || reply != "+PONG\r\n" {
				t.Fatal("fixture connection never reached Redis")
			}
		})
	}()
	var connection net.Conn
	select {
	case connection = <-connected:
		defer connection.Close()
	case <-completed:
		return // The required Redis environment was absent or setup failed.
	case <-time.After(3 * time.Second):
		t.Fatal("proxy setup did not finish")
	}
	select {
	case <-completed:
	case <-time.After(3 * time.Second):
		connection.Close() // Release an old broken cleanup before reporting failure.
		<-completed
		t.Fatal("proxy cleanup waited for an abandoned client's owner")
	}
	connection.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := connection.Read(make([]byte, 1)); err != io.EOF {
		t.Fatal("proxy cleanup did not close the accepted client connection", err)
	}
}
