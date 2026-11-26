package rediscore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func prepareResume(t *testing.T, s *Store, client *redis.Client, id Identity, old OutputRef) OutputIntent {
	t.Helper()
	ctx := context.Background()
	proof, err := s.InspectOutput(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	new := OutputIntent{
		ID: "replacement", Attempt: "replacement-attempt", Worker: "worker-b", Epoch: "epoch-b", Connection: "socket-b",
		MessageField: proof.MessageField, MetadataField: proof.MetadataField, Dependencies: []string{"r:procedure"},
		WireHash: proof.WireHash, ProofHash: sha256.Sum256([]byte("replacement proof")),
		Application: &ApplicationLeaseRef{Application: id.Owner, Term: id.Term}, DeliveryTag: sha256.Sum256([]byte("selected route")),
		Deadline: time.UnixMilli(proof.DeadlineMS),
	}
	if err = client.HSet(ctx, s.workerKey(new.Worker), "epoch", new.Epoch, "expires_ms", new.Deadline.UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err = client.Del(ctx, s.workerKey(new.Worker), s.workerReadyKey(new.Worker)).Err(); err != nil {
			t.Error(err)
		}
	})
	return new
}

func TestResumeParkedApplicationOutput(t *testing.T) {
	s, client, id, old := pendingApplicationOutputFixture(t, time.Minute)
	ctx := context.Background()
	new := prepareResume(t, s, client, id, old)
	procedure := Change{Field: "r:procedure", Value: []byte("updated physical and response flow")}
	metadata := []byte("replacement write permit")
	if _, err := s.ResumeParkedApplicationOutput(ctx, s.primary, id, 1, old, new, procedure, metadata); !errors.Is(err, ErrConflict) {
		t.Fatalf("unparked output could be replaced: %v", err)
	}
	if err := s.ParkPendingApplicationOutput(ctx, id, 1, old); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResumeParkedApplicationOutput(ctx, "promoted-primary", id, 1, old, new, procedure, metadata); !errors.Is(err, ErrAuthority) {
		t.Fatalf("history proof used across primary changes: %v", err)
	}
	altered := new
	altered.WireHash = sha256.Sum256([]byte("different wire"))
	if _, err := s.ResumeParkedApplicationOutput(ctx, s.primary, id, 1, old, altered, procedure, metadata); !errors.Is(err, ErrConflict) {
		t.Fatalf("resume could change the original request: %v", err)
	}
	altered = new
	altered.Deadline = altered.Deadline.Add(time.Second)
	if _, err := s.ResumeParkedApplicationOutput(ctx, s.primary, id, 1, old, altered, procedure, metadata); !errors.Is(err, ErrConflict) {
		t.Fatalf("resume could restart the original deadline: %v", err)
	}
	begin := make(chan struct{})
	var wg sync.WaitGroup
	var revisions [2]uint64
	var failures [2]error
	for i := range revisions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-begin
			revisions[i], failures[i] = s.ResumeParkedApplicationOutput(ctx, s.primary, id, 1, old, new, procedure, metadata)
		}()
	}
	close(begin)
	wg.Wait()
	committed := 0
	for i, err := range failures {
		if err == nil && revisions[i] == 2 {
			committed++
		} else if !errors.Is(err, ErrAbsent) && !errors.Is(err, ErrConflict) {
			t.Fatalf("unexpected resume result: %d %v", revisions[i], err)
		}
	}
	if committed != 1 {
		t.Fatalf("expected one exact reassignment, got %d: %v", committed, failures)
	}
	if _, err := s.ReserveOutput(ctx, old); !errors.Is(err, ErrAbsent) {
		t.Fatalf("old socket retained write permission: %v", err)
	}
	if err := s.CompleteOutput(ctx, old, "WRITTEN"); !errors.Is(err, ErrAbsent) {
		t.Fatalf("late completion from A affected replacement: %v", err)
	}
	view, err := s.Read(ctx, id, procedure.Field, new.MessageField, new.MetadataField)
	if err != nil || view.Revision != 2 || !bytes.Equal(view.Records[procedure.Field], procedure.Value) || !bytes.Equal(view.Records[new.MetadataField], metadata) {
		t.Fatalf("resume did not commit procedure and output together: %+v %v", view, err)
	}
	if deadline, err := client.ZScore(ctx, s.contextDeadlineKey(id.Context), "out-"+new.MetadataField[2:]).Result(); err != nil || int64(deadline) != new.Deadline.UnixMilli() {
		t.Fatalf("lost exact output cutoff: %v %v", deadline, err)
	}
	replacement := outputRef(id, new)
	if write, err := s.ReserveOutput(ctx, replacement); err != nil || !bytes.Equal(write.Wire, mustOutputWire(t, view.Records[new.MessageField])) || !bytes.Equal(write.Metadata, metadata) || write.Connection != new.Connection {
		t.Fatalf("replacement did not receive original wire: %+v %v", write, err)
	}
	if write, err := s.ReserveOutput(ctx, replacement); !errors.Is(err, ErrUnknown) || len(write.Wire) != 0 {
		t.Fatalf("replacement was granted twice: %+v %v", write, err)
	}
	checkNamespace(t, s, client, id)
}
