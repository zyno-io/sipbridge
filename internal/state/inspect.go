package state

// DecodeInspectionPayload validates a bounded offline record using the same
// structural and capacity rules as a default native store. It creates no backend
// or authority and supplies no permission to execute the returned value.
func DecodeInspectionPayload(data []byte) (Payload, error) {
	cfg := Config{Namespace: "inspection", NamespaceKey: make([]byte, 32), Versions: Versions{Schema: "1", Reducer: "1", Timers: "1"}}
	if err := commonDefaults(&cfg); err != nil {
		return Payload{}, err
	}
	var value Payload
	if err := value.UnmarshalBinary(data); err != nil {
		return Payload{}, err
	}
	validation := metadata{cfg: cfg}
	if err := validation.validatePayloadStructure(value); err != nil {
		return Payload{}, err
	}
	if err := validation.validatePayloadSize(value, len(data)); err != nil {
		return Payload{}, err
	}
	return value, nil
}
