package engine

import (
	"context"
	"errors"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// Application-owner cleanup is exceptional work, not periodic work per dialog.
// A failed page is retried from its saved cursor; only exact authoritative
// absence or a completed/no-op transition permits progress.
func (e *StateEngine) processApplicationExpiry(ctx context.Context) error {
	for n := 0; n < 8; n++ {
		// Acknowledge each independently completed context so a slow tail can
		// never force a successful prefix to repeat indefinitely.
		page, err := e.store.ApplicationExpiryPage(ctx, 1)
		if err != nil || page.OwnerID == "" {
			return err
		}
		for _, entry := range page.Entries {
			result, committed, err := e.applyCommitted(ctx, entry.Ref,
				e.id("owner-expiry", page.OwnerID, page.SweepToken, entry.Ref.ID, entry.Ref.Incarnation),
				stateEngineInput{Kind: "owner-check"}, time.Now().Add(2*time.Second), false)
			if errors.Is(err, state.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if result == nil || result.Certainty != pb.OperationResult_COMPLETED || result.Error != "" {
				return errors.New("application owner sweep did not complete")
			}
			if committed != nil {
				e.dispatchCommitted(entry.Ref, committed, "", "")
			}
		}
		if err := e.store.AckApplicationExpiryPage(ctx, page); err != nil {
			return err
		}
	}
	return nil
}
