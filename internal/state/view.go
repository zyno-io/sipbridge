package state

import "context"

// StateView is a non-authorizing, independently owned protocol projection.
// It deliberately cannot carry replay receipts, input bodies or observed leases.
type StateView struct {
	Ref        Ref
	Authority  Authority
	Versions   Versions
	Revision   string
	Payload    Payload
	HasPending bool
}

func protocolView(record Record, pending bool) StateView {
	return StateView{Ref: record.Ref, Authority: record.Authority, Versions: record.Versions,
		Revision: record.Revision, Payload: record.Payload, HasPending: pending}
}

var readStateScript = stateScript(`
if not primary() then return fail('AUTHORITY') end
local err=checkgroup(); if err then return fail(err) end
local now=nowms()
return {'STATE',integer(now),redis.call('HGET',KEYS[1],'incarnation'),redis.call('HGET',KEYS[1],'revision'),
 redis.call('HGET',KEYS[1],'data'),redis.call('HGET',KEYS[1],'callId'),
 redis.call('HSTRLEN',KEYS[1],'pending')>0 and '1' or '0'}
`)

// ReadState does not fetch replay history or observe application leases. Every
// authorizing operation still requires its own fresh store admission/permit.
func (s *Store) ReadState(ctx context.Context, ref Ref) (StateView, int64, error) {
	if err := s.validateRef(ref); err != nil {
		return StateView{}, 0, err
	}
	values, err := s.run(ctx, readStateScript, ref, nil, false)
	if err != nil {
		return StateView{}, 0, err
	}
	if len(values) != 7 || values[0] != "STATE" {
		return StateView{}, 0, ErrCorrupt
	}
	now, err := millisAt(values, 1)
	if err != nil {
		return StateView{}, 0, err
	}
	fields := make([]string, 5)
	for n := range fields {
		fields[n], err = textAt(values, n+2)
		if err != nil {
			return StateView{}, 0, err
		}
	}
	if fields[4] != "0" && fields[4] != "1" {
		return StateView{}, 0, ErrCorrupt
	}
	record, err := s.decodeProtocolRecord(ref, fields[0], fields[1], []byte(fields[2]), fields[3])
	if err != nil {
		return StateView{}, 0, err
	}
	return protocolView(record, fields[4] == "1"), now, nil
}

func (m *MemoryStore) ReadState(ctx context.Context, ref Ref) (StateView, int64, error) {
	now, err := m.lock(ctx)
	if err != nil {
		return StateView{}, 0, err
	}
	defer m.unlock()
	group, err := m.group(ref)
	if err != nil {
		return StateView{}, 0, err
	}
	record, err := m.protocolRecord(group)
	if err != nil {
		return StateView{}, 0, err
	}
	return protocolView(record, false), now, nil
}
