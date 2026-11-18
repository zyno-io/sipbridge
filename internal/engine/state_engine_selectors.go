package engine

import (
	"github.com/emiago/sipgo/sip/portable"
	"sort"

	pb "sipbridge/internal/controlpb"
)

const (
	stateSelectorMemoEntries = 256
	stateSelectorMemoBytes   = 1 << 20
)

// The exact immutable key owns its bytes. Values are read-only derived hints,
// never parsed message objects or permission to perform an external operation.
func (r *stateReduction) projectionForWire(wire []byte) *wireProjection {
	if projection, found := r.selectorMemo[string(wire)]; found {
		return projection
	}
	projection := projectWire(wire)
	if len(r.selectorMemo) < stateSelectorMemoEntries && len(wire) <= stateSelectorMemoBytes-r.selectorMemoBytes {
		if r.selectorMemo == nil {
			r.selectorMemo = make(map[string]*wireProjection)
		}
		r.selectorMemo[string(wire)] = projection
		r.selectorMemoBytes += len(wire)
	}
	return projection
}

func (r *stateReduction) selectorsForWire(wire []byte, outbound bool) ([]string, error) {
	return r.projectionForWire(wire).directionSelectors(outbound)
}

func stateSelectorsForMessage(message portable.Message, outbound bool) ([]string, error) {
	facts, err := message.Facts()
	if err != nil {
		return nil, err
	}
	client, clientErr := message.ClientTxKey()
	if facts.IsRequest && clientErr != nil {
		return nil, clientErr
	}
	key, prefix := client, "ctx:"
	if facts.IsRequest != outbound {
		key, _ = message.CancelServerTxKey()
		prefix = "stx:"
	}
	var selectors []string
	if key != "" {
		selectors = append(selectors, opaque(prefix, key))
	}
	if facts.FromTag != "" && facts.ToTag != "" {
		tags := []string{facts.FromTag, facts.ToTag}
		sort.Strings(tags)
		selectors = append(selectors, opaque("dlg:", facts.CallID, tags[0], tags[1]))
	}
	return selectors, nil
}

func (r *stateReduction) portableIdentity(message portable.Message) (wireIdentity, error) {
	facts, err := message.Facts()
	if err != nil {
		return wireIdentity{}, err
	}
	return wireIdentity{Method: facts.CSeqMethod, Sequence: facts.CSeq, ToTag: facts.ToTag, ViaBranch: facts.ViaBranch, SentBy: facts.ViaSentBy}, nil
}

func (r *stateReduction) clearSelectorMemo() {
	r.selectorMemo, r.selectorMemoBytes = nil, 0
}

func (r *stateReduction) pruneTransactions() {
	for id, c := range r.group.Contexts {
		for key, tx := range c.Transactions {
			if r.transactionRetirable(id, key, tx) {
				delete(c.Transactions, key)
				delete(c.Requests, tx.RequestID)
			}
		}
	}
}

// Selector indexes are derived hints, never an accumulated transaction history.
func (r *stateReduction) refreshSelectors() error {
	r.group.Selectors = make(map[string]string)
	for _, id := range stateOwnerContextIDs(r.group) {
		c := r.group.Contexts[id]
		c.Snapshot.Selectors = nil
		if c.Rejection {
			continue
		}
		keys := make([]string, 0, len(c.Transactions))
		for key := range c.Transactions {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			tx := c.Transactions[key]
			client := tx.Native.Family == portable.InviteClient || tx.Native.Family == portable.NonInviteClient
			r.index(c, tx.Native.Origin, client)
			if tx.Native.LastResponse != nil {
				r.index(c, *tx.Native.LastResponse, !client)
			}
		}
		dialogs := make([]string, 0, len(c.Dialogs))
		for key := range c.Dialogs {
			dialogs = append(dialogs, key)
		}
		sort.Strings(dialogs)
		addDialog := func(call, local, remote string) {
			if local == "" || remote == "" {
				return
			}
			tags := []string{local, remote}
			sort.Strings(tags)
			selector := opaque("dlg:", call, tags[0], tags[1])
			r.group.Selectors[selector] = id
			if !contains(c.Snapshot.Selectors, selector) {
				c.Snapshot.Selectors = append(c.Snapshot.Selectors, selector)
			}
		}
		for _, key := range dialogs {
			d := c.Dialogs[key]
			addDialog(d.CallID, d.LocalTag, d.RemoteTag)
		}
		if c.Subscription != nil {
			addDialog(c.Snapshot.CallId, c.Snapshot.LocalTag, c.Snapshot.RemoteTag)
		}
		sort.Strings(c.Snapshot.Selectors)
	}
	return nil
}

func stateRefreshResultSnapshot(group stateGroup, result *pb.OperationResult, executor *pb.ProcessRef) {
	if result == nil || result.Snapshot.GetContext() == nil {
		return
	}
	if c := group.Contexts[result.Snapshot.Context.Id]; c != nil {
		result.Snapshot = stateSnapshot(c, executor)
	}
}
