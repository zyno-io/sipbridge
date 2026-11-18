// sipbridge-state-inspect provides redacted native records and explicit fixture inspection.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"sipbridge/internal/buildinfo"
	pb "sipbridge/internal/controlpb"
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

func inspectHistorical(reader io.Reader, writer io.Writer) error {
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
func inspect(reader io.Reader, writer io.Writer) error {
	data, err := io.ReadAll(io.LimitReader(reader, storagecodec.MaxNativeRecordBytes+1))
	if err != nil {
		return err
	}
	result, err := engine.InspectNativeRecord(data)
	if err != nil {
		return errors.New("record failed selected native validation")
	}
	return json.NewEncoder(writer).Encode(result)
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
	fixture := flag.Bool("fixture-completion-projection", false, "historical checkpoint completion projection only")
	historical := flag.Bool("historical-checkpoint", false, "explicit historical checkpoint fixture reader")
	nativeRedis := flag.Bool("fixture-native-redis", false, "read-only binary inspection of disposable synthetic Redis")
	query := engine.NativeFixtureQuery{}
	flag.StringVar(&query.ContextID, "context-id", "", "exact native context identity")
	flag.StringVar(&query.Incarnation, "incarnation", "", "exact native context incarnation")
	flag.StringVar(&query.CallID, "call-id", "", "fixture-only bounded Call-ID lookup")
	flag.StringVar(&query.LocalTag, "local-tag", "", "exact native local dialog tag")
	flag.StringVar(&query.RemoteTag, "remote-tag", "", "exact native remote dialog tag")
	applicationInstance := flag.String("application-instance", "", "exact application process instance")
	applicationEpoch := flag.String("application-epoch", "", "exact application process epoch")
	flag.Parse()
	if *nativeRedis {
		if *historical || *fixture || *input != "-" {
			return errors.New("native Redis mode cannot read offline or historical inputs")
		}
		if (*applicationInstance == "") != (*applicationEpoch == "") {
			return errors.New("application instance and epoch required together")
		}
		if *applicationInstance != "" {
			query.Application = &pb.ProcessRef{InstanceId: *applicationInstance, Epoch: *applicationEpoch}
		}
		return inspectNativeRedis(query, os.Getenv, os.Stdout)
	}
	if query.ContextID != "" || query.Incarnation != "" || query.CallID != "" || query.LocalTag != "" || query.RemoteTag != "" || *applicationInstance != "" || *applicationEpoch != "" {
		return errors.New("Redis query requires explicit fixture-native-redis")
	}
	if *fixture && !*historical {
		return errors.New("completion projection requires explicit historical-checkpoint")
	}
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
	if *historical {
		return inspectHistorical(reader, os.Stdout)
	}
	return inspect(reader, os.Stdout)
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
