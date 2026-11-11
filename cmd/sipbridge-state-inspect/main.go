// sipbridge-state-inspect decodes a bounded raw retained data value offline.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"sipbridge/internal/buildinfo"
	"sipbridge/internal/engine"
	"sipbridge/internal/state"
	"sipbridge/internal/storagecodec"
)

type effectSummary struct {
	Kind       string
	Status     state.EffectStatus
	DeadlineMS int64
	Bytes      int
}
type timerSummary struct{ DueMS, DeadlineMS int64 }
type inspection struct {
	Encoding, BuildRevision                                                  string
	EncodedBytes, ProtocolBytes, ActiveTransactions, ReservedCompletionBytes int
	Protocol                                                                 engine.RetainedInspection
	Effects                                                                  []effectSummary
	Timers                                                                   []timerSummary
	Retention                                                                state.Retention
	ApplicationContexts, ApplicationLeases                                   int
}

func readPayload(reader io.Reader) (state.Payload, int, error) {
	data, err := io.ReadAll(io.LimitReader(reader, storagecodec.MaxBytes+1))
	if err != nil {
		return state.Payload{}, 0, err
	}
	if len(data) > storagecodec.MaxBytes {
		return state.Payload{}, 0, errors.New("record exceeds 64 MiB")
	}
	payload, err := state.DecodeInspectionPayload(data)
	if err != nil {
		return state.Payload{}, 0, errors.New("record failed binary state validation")
	}
	return payload, len(data), nil
}

func inspect(reader io.Reader, writer io.Writer) error {
	payload, encodedBytes, err := readPayload(reader)
	if err != nil {
		return err
	}
	protocol, err := engine.InspectRetainedProtocol(payload.ProtocolState)
	if err != nil {
		return errors.New("record failed native protocol validation")
	}
	out := inspection{Encoding: storagecodec.Encoding, BuildRevision: buildinfo.Revision, EncodedBytes: encodedBytes, ProtocolBytes: len(payload.ProtocolState),
		ActiveTransactions: payload.ActiveTransactions, ReservedCompletionBytes: payload.ReservedCompletionBytes, Protocol: protocol, Retention: payload.Retention,
		ApplicationContexts: len(payload.ApplicationContexts), ApplicationLeases: len(payload.ApplicationLeaseIDs)}
	for _, effect := range payload.Effects {
		kind := effect.Kind
		switch kind {
		case "present", "event", "adopt", "open", "credential", "owner", "wire", "deliver":
		default:
			kind = "unsupported"
		}
		out.Effects = append(out.Effects, effectSummary{Kind: kind, Status: effect.Status, DeadlineMS: effect.DeadlineMS, Bytes: len(effect.Payload)})
	}
	for _, timer := range payload.Timers {
		out.Timers = append(out.Timers, timerSummary{DueMS: timer.DueMS, DeadlineMS: timer.DeadlineMS})
	}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(out)
}
func inspectFixtureCompletion(reader io.Reader, writer io.Writer) error {
	payload, _, err := readPayload(reader)
	if err != nil {
		return err
	}
	projection, err := engine.InspectFixtureCompletion(payload)
	if err != nil {
		return errors.New("record failed fixture completion validation")
	}
	out := struct {
		Format         string                             `json:"format"`
		SchemaIdentity string                             `json:"schemaIdentity"`
		BuildRevision  string                             `json:"buildRevision"`
		Projection     engine.FixtureCompletionProjection `json:"projection"`
	}{"sipbridge.fixture.completion.v1", storagecodec.SchemaIdentity(), buildinfo.Revision, projection}
	return json.NewEncoder(writer).Encode(out)
}

func run() error {
	input := flag.String("input", "-", "raw binary data file, or - for stdin")
	fixture := flag.Bool("fixture-completion-projection", false, "identifying offline projection for disposable synthetic qualification only")
	flag.Parse()
	var reader io.Reader = os.Stdin
	if *input != "-" {
		file, err := os.Open(*input)
		if err != nil {
			return errors.New("cannot open input file")
		}
		defer file.Close()
		reader = file
	}
	if *fixture {
		return inspectFixtureCompletion(reader, os.Stdout)
	}
	return inspect(reader, os.Stdout)
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
