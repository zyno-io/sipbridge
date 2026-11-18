package rediscore

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestObserveContextReturnsCurrentOwnerAndSelectedFields(t *testing.T) {
	s, c, id, out, data := outputFixture(t)
	ctx := context.Background()
	first, err := s.AcquireApplication(ctx, "application1", "term1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.AcquireApplication(ctx, "application2", "term2")
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{s.applicationKey("application1"), s.applicationKey("application2"), s.applicationOwnerKey("term1"), s.applicationOwnerKey("term2"), s.applicationOwnedKey("term1"), s.applicationOwnedKey("term2"), s.applicationAdmissionKey(), s.ownerExpiryKey()}
	t.Cleanup(func() {
		if e := c.Del(ctx, keys...).Err(); e != nil {
			t.Error(e)
		}
	})
	id.Owner, id.Term = first.Application, first.Term
	if _, err = s.Commit(ctx, Publication{Identity: id, Create: true, Application: &first.ApplicationLeaseRef, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: out.MessageField, Value: data}, {Field: out.MetadataField, Value: []byte("metadata")}}, NewOutputs: []OutputIntent{out}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Application: &second.ApplicationLeaseRef, BindApplication: true, Deadline: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	observed, err := s.ObserveContext(ctx, id.Context, id.Incarnation, "r:metadata", "r:absent")
	if err != nil {
		t.Fatal(err)
	}
	if observed.Identity.Owner != second.Application || observed.Identity.Term != second.Term || observed.View.Revision != 2 || !bytes.Equal(observed.View.Records["r:metadata"], []byte("metadata")) || observed.View.Records["r:absent"] != nil || len(observed.View.Records) != 2 {
		t.Fatal("wrong current selected context", observed)
	}
	if _, err = s.Read(ctx, id, "r:metadata"); !errors.Is(err, ErrAuthority) {
		t.Fatal("old owner read accepted", err)
	}
	if _, err = s.ObserveContext(ctx, id.Context, "other-incarnation", "r:metadata"); !errors.Is(err, ErrAuthority) {
		t.Fatal("reused context accepted", err)
	}
	if _, err = s.ObserveContext(ctx, id.Context, id.Incarnation, "r:metadata", "r:metadata"); !errors.Is(err, ErrConflict) {
		t.Fatal("duplicate field accepted", err)
	}
}
func TestInspectOutputExactProofWithoutReservation(t *testing.T) {
	s, _, id, out, data := outputFixture(t)
	ctx := context.Background()
	publishOutput(t, s, id, out, data)
	ref := outputRef(id, out)
	got, err := s.InspectOutput(ctx, ref)
	if err != nil || got.State != OutputPending || got.MetadataField != out.MetadataField || got.MessageField != out.MessageField || got.WireHash != out.WireHash || !bytes.Equal(got.Metadata, []byte("metadata")) || got.Acknowledged || got.Settled {
		t.Fatal("pending inspection", got, err)
	}
	wrong := ref
	wrong.ProofHash[0] ^= 1
	if _, err = s.InspectOutput(ctx, wrong); !errors.Is(err, ErrConflict) {
		t.Fatal("wrong proof inspected", err)
	}
	if _, err = s.ReserveOutput(ctx, ref); err != nil {
		t.Fatal(err)
	}
	got, err = s.InspectOutput(ctx, ref)
	if err != nil || got.State != OutputStarted {
		t.Fatal("started inspection", got, err)
	}
	if err = s.CompleteOutput(ctx, ref, "WRITTEN"); err != nil {
		t.Fatal(err)
	}
	if err = s.SettleOutput(ctx, ref, true, false); err != nil {
		t.Fatal(err)
	}
	got, err = s.InspectOutput(ctx, ref)
	if err != nil || got.State != OutputWritten || got.MetadataField != out.MetadataField || !got.Acknowledged || got.Settled {
		t.Fatal("proof inspection", got, err)
	}
	if err = s.SettleOutput(ctx, ref, false, true); err != nil {
		t.Fatal(err)
	}
	if _, err = s.InspectOutput(ctx, ref); !errors.Is(err, ErrAbsent) {
		t.Fatal("released proof still present", err)
	}
}
