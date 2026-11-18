package rediscore

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"
	"time"
)

func TestCommitAuthorityDiagnosticsRefuseWithoutMutation(t *testing.T) {
	for _, reason := range []string{"primary", "profile", "identity", "application-term", "application-owner"} {
		t.Run(reason, func(t *testing.T) {
			s, client, id := fixture(t)
			ctx := context.Background()
			publish(t, s, id, Change{Field: "r:tx", Value: []byte("original")})
			publication := Publication{Identity: id, Revision: 1, Deadline: time.Now().Add(time.Minute), Changes: []Change{{Field: "r:tx", Value: []byte("must-not-publish")}}}
			switch reason {
			case "primary":
				s.primary = strings.Repeat("0", 40)
			case "profile":
				s.profile = []byte("different-profile")
			case "identity":
				publication.Identity.Incarnation = "different-incarnation"
			default:
				lease, err := s.AcquireApplication(ctx, "diagnostic-app", "diagnostic-term")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					_ = client.Del(context.Background(), s.applicationKey("diagnostic-app"), s.applicationOwnerKey("diagnostic-term")).Err()
				})
				publication.Application = &lease.ApplicationLeaseRef
				if reason == "application-term" {
					publication.Application.Term = "different-term"
				} else if err = client.HSet(ctx, s.applicationOwnerKey(lease.Term), "application", "different-application").Err(); err != nil {
					t.Fatal(err)
				}
			}
			before, err := client.HGetAll(ctx, s.contextKey(id.Context)).Result()
			if err != nil {
				t.Fatal(err)
			}
			capacity, err := client.HGetAll(ctx, s.capacityKey()).Result()
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.Commit(ctx, publication)
			if !errors.Is(err, ErrAuthority) || errors.Is(err, ErrUnknown) || !strings.HasSuffix(err.Error(), "commit "+reason) {
				t.Fatal("authority identity/diagnostic changed", err)
			}
			after, readErr := client.HGetAll(ctx, s.contextKey(id.Context)).Result()
			if readErr != nil || !maps.Equal(before, after) {
				t.Fatal("authority refusal mutated context", readErr)
			}
			afterCapacity, readErr := client.HGetAll(ctx, s.capacityKey()).Result()
			if readErr != nil || !maps.Equal(capacity, afterCapacity) {
				t.Fatal("authority refusal mutated capacity", readErr)
			}
		})
	}
}

func TestCommitDiagnosticRejectsUnrecognizedData(t *testing.T) {
	for _, value := range []interface{}{"private-context-or-secret", strings.Repeat("x", 4096), []byte("profile"), nil} {
		if err := commitStatus([]interface{}{"AUTHORITY", value}); err != ErrAuthority {
			t.Fatal("unrecognized diagnostic was propagated", err)
		}
	}
	if err := status([]interface{}{"AUTHORITY", "profile"}); err != ErrAuthority {
		t.Fatal("another operation consumed Commit diagnostic", err)
	}
}
