package rediscore

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/storagecodec"
	"sipbridge/internal/storagepb"
)

// ProfileConfig is immutable namespace compatibility, not connection metadata.
// Primary promotions and process epochs must not change logical SIP authority.
type ProfileConfig struct {
	Root, Service, Generation string
	IdentityKey               []byte
	RecordBudget              int64
	NamespaceMaxContexts      int64
	NamespaceMaxBytes         int64
	InstanceAdmission         bool
}

type ProfileBinding struct {
	Value     []byte
	Authority *pb.StateAuthority
}

func profileReference(value string) bool {
	if len(value) == 0 || len(value) > 96 {
		return false
	}
	for _, c := range value {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("_.:@+-", c) {
			continue
		}
		return false
	}
	return true
}

// BindProfile stores a compact raw MAC of a deterministic binary tuple. It
// binds the actual schema graph and frame/index layout, not version 1 alone.
// Image/source receipts remain separately pinned; source-only edits do not
// invalidate compatible retained state. No JSON, textual payload wrapping or
// state/credential lookup participates in this calculation.
func BindProfile(cfg ProfileConfig) (ProfileBinding, error) {
	if cfg.NamespaceMaxContexts == 0 {
		cfg.NamespaceMaxContexts = DefaultNamespaceMaxContexts
	}
	if cfg.NamespaceMaxBytes == 0 {
		cfg.NamespaceMaxBytes = DefaultNamespaceMaxBytes
	}
	if len(cfg.Root) == 0 || len(cfg.Root) > 512 || !strings.HasSuffix(cfg.Root, ":") || strings.ContainsAny(cfg.Root, "\x00\r\n*?[]") ||
		!profileReference(cfg.Service) || !profileReference(cfg.Generation) || len(cfg.IdentityKey) < 32 || len(cfg.IdentityKey) > 1024 ||
		cfg.RecordBudget <= 0 || cfg.RecordBudget > maxExactInteger || cfg.NamespaceMaxContexts <= 0 || cfg.NamespaceMaxContexts > maxExactInteger ||
		cfg.NamespaceMaxBytes <= 0 || cfg.NamespaceMaxBytes > maxExactInteger {
		return ProfileBinding{}, ErrAuthority
	}
	tuple := make([]byte, 0, 768)
	text := func(value string) {
		tuple = binary.AppendUvarint(tuple, uint64(len(value)))
		tuple = append(tuple, value...)
	}
	text("sipbridge-native-redis-profile-v1")
	text(cfg.Root)
	text(cfg.Service)
	text(cfg.Generation)
	for _, version := range []uint64{1, 1, 1} {
		tuple = binary.AppendUvarint(tuple, version)
	}
	text(storagecodec.Encoding)
	tuple = append(tuple, selectedSchemaDigest[:]...)
	// Change this layout identifier when native keys, projections or their
	// accounting/authority interpretation change incompatibly.
	text("context-hash-record-deadlines-worker-heads-ready-cutoff-owner-terms-application-admission-raw-selectors-worker-reacquire-output-proof-dependencies-delivery-register-obsolete-pooled-credit-application-witness-finished-notify-obsolete-superseded-provisional-original-result-pinned-sub-dialog-invite-disconnect-peer-reinvite-ack-v1")
	if cfg.InstanceAdmission {
		text("instance-admission-v2")
	}
	text("command-refusal-status-v1")
	for _, kind := range []byte{storagecodec.SelectedMessage, storagecodec.NativeContext, storagecodec.NativeProcedure, storagecodec.NativeDialog,
		storagecodec.NativeSubscription, storagecodec.NativeResult, storagecodec.NativeLifecycle, storagecodec.NativeOutput} {
		tuple = append(tuple, kind)
	}
	for _, bound := range []int64{cfg.RecordBudget, cfg.NamespaceMaxContexts, cfg.NamespaceMaxBytes, maxFields, maxOutputs, maxRecordBytes, maxOperationSize,
		storagecodec.MaxBytes, storagecodec.MaxDepth, storagecodec.MaxCollection, storagecodec.MaxFields, storagecodec.MaxNativeRecordBytes,
		storagecodec.MaxSelectedMessageFieldBytes, storagecodec.MaxSelectedMessageBytes, ApplicationMaxGroups, MaxExpiredApplicationBatch,
		storagecodec.MaxNativeOutputBytes, storagecodec.MaxNativePublicSelectors, storagecodec.MaxNativePublicSelectorBytes,
		ApplicationLeaseLifetime.Milliseconds(), ApplicationIdleLifetime.Milliseconds(), WorkerLeaseLifetime.Milliseconds(),
		WorkerIdleLifetime.Milliseconds(), maxWorkerReferenceBytes, MaxWorkerTransferBatch, MaxReadyPeek, OutputObsoleteGrace.Milliseconds(),
		workerRedisTimeout.Milliseconds(), workerClockTolerance.Milliseconds(), WorkerRenewInterval.Milliseconds(), maxOutputDependencies} {
		tuple = binary.AppendUvarint(tuple, uint64(bound))
	}
	mac := hmac.New(sha256.New, cfg.IdentityKey)
	_, _ = mac.Write(tuple)
	value := mac.Sum(nil)
	identity := hmac.New(sha256.New, cfg.IdentityKey)
	_, _ = identity.Write([]byte("sipbridge-native-logical-authority-v1\x00"))
	_, _ = identity.Write(value)
	// This is a public opaque reference identifier, not an encoded record blob.
	authority := &pb.StateAuthority{Service: cfg.Service, Generation: cfg.Generation,
		PrimaryId: hex.EncodeToString(identity.Sum(nil))[:40], SchemaVersion: 1}
	return ProfileBinding{Value: value, Authority: authority}, nil
}

var selectedSchemaDigest = func() [32]byte {
	value, err := schemaGraphDigest([]protoreflect.MessageDescriptor{
		(&storagepb.PortableMessage{}).ProtoReflect().Descriptor(), (&nativepb.Context{}).ProtoReflect().Descriptor(),
		(&nativepb.Procedure{}).ProtoReflect().Descriptor(), (&nativepb.Dialog{}).ProtoReflect().Descriptor(),
		(&nativepb.Subscription{}).ProtoReflect().Descriptor(), (&nativepb.Result{}).ProtoReflect().Descriptor(),
		(&nativepb.Lifecycle{}).ProtoReflect().Descriptor(), (&nativepb.Output{}).ProtoReflect().Descriptor(),
	})
	if err != nil {
		panic("invalid selected native schema")
	}
	return value
}()

// Walk only the transitive selected-record descriptor graph. Runtime messages
// and source paths are never reflected or hashed per operation.
func schemaGraphDigest(roots []protoreflect.MessageDescriptor) ([32]byte, error) {
	entries := make(map[string]proto.Message)
	var visit func(protoreflect.MessageDescriptor)
	visit = func(message protoreflect.MessageDescriptor) {
		name := "message:" + string(message.FullName())
		if _, ok := entries[name]; ok {
			return
		}
		entries[name] = protodesc.ToDescriptorProto(message)
		fields := message.Fields()
		for i := 0; i < fields.Len(); i++ {
			field := fields.Get(i)
			if child := field.Message(); child != nil {
				visit(child)
			}
			if enum := field.Enum(); enum != nil {
				entries["enum:"+string(enum.FullName())] = protodesc.ToEnumDescriptorProto(enum)
			}
		}
	}
	for _, root := range roots {
		visit(root)
	}
	keys := make([]string, 0, len(entries))
	for name := range entries {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	hash := sha256.New()
	_, _ = hash.Write([]byte("sipbridge-selected-schema-graph-v1\x00"))
	for _, name := range keys {
		data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(entries[name])
		if err != nil {
			return [32]byte{}, err
		}
		frame := binary.AppendUvarint(nil, uint64(len(name)))
		frame = append(frame, name...)
		frame = binary.AppendUvarint(frame, uint64(len(data)))
		_, _ = hash.Write(frame)
		_, _ = hash.Write(data)
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result, nil
}
