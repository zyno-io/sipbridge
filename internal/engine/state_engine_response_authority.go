package engine

import (
	"encoding/hex"
	"errors"

	"github.com/emiago/sipgo/sip"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

var errStateBusinessAuthorityChanged = errors.New("response directory epoch changed")

// This is the live controller's already-admitted trusted-directory receipt.
// Native protocol completion uses its separate context-derived capability.
func (r *stateReduction) retainResponseAuthority(c *stateContext, tx *stateTransaction, response *pb.Respond) error {
	if !r.engine.autonomousCompletion() {
		return nil
	}
	if tx.Policy != nil {
		return nil // The same authenticated command already supplied its policy decision.
	}
	authority := response.Authority
	if authority == nil || !state.ValidReference(authority.DirectoryEpoch) {
		return errors.New("response directory authority required")
	}
	if c.OwnerDirectoryEpoch != "" && c.OwnerDirectoryEpoch != authority.DirectoryEpoch {
		return errStateBusinessAuthorityChanged
	}
	scope := authority.CompletionScope
	if scope != "" {
		decoded, err := hex.DecodeString(scope)
		if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != scope {
			return errors.New("invalid response completion scope")
		}
	}
	facts, err := tx.Native.Origin.Facts()
	if err != nil {
		return err
	}
	if !facts.IsRequest {
		return errors.New("response request origin required")
	}
	initial := facts.Method == sip.INVITE && facts.ToTag == ""
	if initial && response.Status >= 200 && response.Status < 300 && scope == "" {
		return errors.New("successful initial INVITE response requires its admitted completion scope")
	}
	if scope != "" {
		if tx.CompletionScope != "" && tx.CompletionScope != scope {
			return errors.New("response transaction completion scope changed")
		}
		if initial && (c.CompletionScope != "" && c.CompletionScope != scope || c.ScopeOrigin != nil &&
			(c.ScopeOrigin.Transport() != tx.Native.Origin.Transport() || !c.ScopeOrigin.EqualWire(tx.Native.Origin))) {
			return errors.New("initial response completion anchor changed")
		}
	}
	c.OwnerDirectoryEpoch = authority.DirectoryEpoch
	if scope != "" {
		tx.CompletionScope = scope
		if initial {
			c.CompletionScope = scope
			origin := tx.Native.Origin.Clone()
			c.ScopeOrigin = &origin
		}
	}
	return nil
}
