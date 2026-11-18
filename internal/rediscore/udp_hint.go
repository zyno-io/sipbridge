package rediscore

// UDPHint is an authority-free, bounded wake for one local derivative retry.
type UDPHint struct {
	Worker, Context, Incarnation, Procedure string
	Stop                                    bool
}

func encodeUDPHint(h UDPHint) ([]byte, error) {
	if !reference(h.Worker) || len(h.Worker) > maxWorkerReferenceBytes || !reference(h.Context) || !reference(h.Incarnation) || !recordField(h.Procedure) {
		return nil, ErrConflict
	}
	kind := byte(1)
	if h.Stop {
		kind = 2
	}
	b := []byte{2, kind, byte(len(h.Context))}
	b = append(b, h.Context...)
	b = append(b, byte(len(h.Incarnation)))
	b = append(b, h.Incarnation...)
	b = append(b, byte(len(h.Procedure)))
	b = append(b, h.Procedure...)
	return b, nil
}

func decodeUDPHint(worker string, b []byte) (UDPHint, error) {
	var h UDPHint
	if len(b) < 6 || b[0] != 2 || (b[1] != 1 && b[1] != 2) {
		return h, ErrCorrupt
	}
	h.Worker = worker
	h.Stop = b[1] == 2
	n := int(b[2])
	p := 3
	if n == 0 || len(b) < p+n+1 {
		return UDPHint{}, ErrCorrupt
	}
	h.Context = string(b[p : p+n])
	p += n
	n = int(b[p])
	p++
	if n == 0 || len(b) < p+n+1 {
		return UDPHint{}, ErrCorrupt
	}
	h.Incarnation = string(b[p : p+n])
	p += n
	n = int(b[p])
	p++
	if n == 0 || len(b) != p+n {
		return UDPHint{}, ErrCorrupt
	}
	h.Procedure = string(b[p:])
	if !reference(h.Worker) || len(h.Worker) > maxWorkerReferenceBytes || !reference(h.Context) || !reference(h.Incarnation) || !recordField(h.Procedure) {
		return UDPHint{}, ErrCorrupt
	}
	return h, nil
}
