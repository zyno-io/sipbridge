package rediscore

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
)

func TestApplicationAttachmentCloseFencesOnlyItsOwnMutationsAndOutput(t *testing.T) {
	s, client, id, out, message := outputFixture(t)
	ctx := context.Background()
	app := "application1"
	a, err := s.AcquireAttachedApplication(ctx, app, "term1", "sessionA")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.AcquireAttachedApplication(ctx, app, "term2", "sessionB")
	if err != nil || a.Term != b.Term {
		t.Fatal("sessions failed to share the live application term", b, err)
	}
	t.Cleanup(func() {
		if err := client.Del(ctx, s.applicationKey(app), s.applicationOwnerKey(a.Term), s.applicationOwnedKey(a.Term),
			s.applicationAttachmentKey(a.Attachment), s.applicationAttachmentKey(b.Attachment), s.ownerExpiryKey()).Err(); err != nil {
			t.Error(err)
		}
	})
	id.Owner, id.Term = app, a.Term
	out.Application = &a.ApplicationLeaseRef
	out.DeliveryTag = sha256.Sum256([]byte("accepted-flow"))
	_, err = s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: out.Deadline, Application: out.Application,
		Changes: []Change{
			{Field: out.MessageField, Value: message},
			{Field: out.MetadataField, Value: []byte("metadata")},
			{Field: "r:delivery-tag", Value: out.DeliveryTag[:]},
		}, NewOutputs: []OutputIntent{out}})
	if err != nil {
		t.Fatal("publish A output", err)
	}
	if err = s.CloseApplicationAttachment(ctx, a.ApplicationLeaseRef); err != nil {
		t.Fatal("close A", err)
	}
	if _, err = s.RenewApplication(ctx, a.ApplicationLeaseRef); !errors.Is(err, ErrAuthority) {
		t.Fatal("closed A renewed its attachment", err)
	}
	if _, err = s.RenewApplication(ctx, b.ApplicationLeaseRef); err != nil {
		t.Fatal("closing A revoked B", err)
	}
	if _, err = s.ReadApplication(ctx, id, a.ApplicationLeaseRef, out.MessageField); !errors.Is(err, ErrAuthority) {
		t.Fatal("closed A retained read authority", err)
	}
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: out.Deadline, Application: out.Application,
		Changes: []Change{{Field: "r:stale-change", Value: []byte("wrong")}}}); !errors.Is(err, ErrAuthority) {
		t.Fatal("closed A retained publication authority", err)
	}
	ref := outputRef(id, out)
	if got, err := s.ReserveOutput(ctx, ref); !errors.Is(err, ErrAuthority) || len(got.Wire) != 0 {
		t.Fatal("closed A's queued output was eligible", err)
	}
	if err = s.CompleteOutput(ctx, ref, "NOT_WRITTEN"); err != nil {
		t.Fatal("native cleanup could not settle the old output", err)
	}
	if err = s.SettleOutput(ctx, ref, true, true); err != nil {
		t.Fatal("native cleanup could not release the old output", err)
	}
	out.Application = &b.ApplicationLeaseRef
	out.ID, out.Attempt = "output2", "attempt2"
	if _, err = s.Commit(ctx, Publication{Identity: id, Revision: 1, Deadline: out.Deadline, Application: out.Application,
		NewOutputs: []OutputIntent{out}}); err != nil {
		t.Fatal("active B could not use the shared context", err)
	}
	bRef := outputRef(id, out)
	got, err := s.ReserveOutput(ctx, bRef)
	if err != nil || got.Attachment != b.Attachment || len(got.Wire) == 0 {
		t.Fatal("active B did not receive its output", err)
	}
	if err = s.CloseApplicationAttachment(ctx, a.ApplicationLeaseRef); err != nil {
		t.Fatal("duplicate stale close", err)
	}
	if err = s.CloseApplicationAttachment(ctx, b.ApplicationLeaseRef); err != nil {
		t.Fatal("close B", err)
	}
	if err = s.CompleteOutput(ctx, bRef, "WRITTEN"); err != nil {
		t.Fatal("exact late physical proof failed after close", err)
	}
	if _, err = s.ObserveApplication(ctx, app); err != nil {
		t.Fatal("attachment close revoked the application term instead of letting it expire", err)
	}
}
