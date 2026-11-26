package control

import (
	"crypto/hmac"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

const DialogIDOnlyCapability = "sip.dialog-id.v2"
const dialogIDOnlyVersion = "d2"
const maxDialogIDOnlyLength = 2131

func dialogIDOnlyFor(authority *pb.StateAuthority, key []byte, ref *pb.ContextRef) (string, error) {
	state := ref.GetState()
	owner := ref.GetOwner()
	if authority == nil || state == nil || !proto.Equal(state.Authority, authority) ||
		!reference.MatchString(ref.Id) || !reference.MatchString(state.GroupId) || !reference.MatchString(state.Incarnation) ||
		state.Partition > 4095 || state.CallId == "" || len(state.CallId) > 1024 || !utf8.ValidString(state.CallId) ||
		strings.ContainsAny(state.CallId, "\r\n\x00") || (owner != nil && !validProcess(owner)) {
		return "", errors.New("invalid native context for ID-only projection")
	}
	payload := strings.Join([]string{ref.Id, state.GroupId, state.Incarnation, strconv.FormatUint(uint64(state.Partition), 10),
		state.CallId, owner.GetInstanceId(), owner.GetEpoch()}, "\x00")
	digest := dialogDirectoryDigest(ref)
	return encodeDialogID(DialogIDOnlyCapability, dialogIDOnlyVersion, authority, key, digest, payload), nil
}

func decodeDialogIDOnly(authority *pb.StateAuthority, key []byte, id string) (*pb.ContextRef, error) {
	if len(id) > maxDialogIDOnlyLength {
		return nil, errors.New("ID-only dialog reference exceeds its wire bound")
	}
	parts := strings.SplitN(id, ".", 5)
	if authority == nil || len(parts) != 4 || parts[0] != dialogIDOnlyVersion || len(parts[1]) != 64 || len(parts[3]) != 43 {
		return nil, errors.New("invalid ID-only dialog reference")
	}
	locator, err := base64.RawURLEncoding.Strict().DecodeString(parts[2])
	if err != nil {
		return nil, errors.New("invalid ID-only dialog locator")
	}
	fields := strings.SplitN(string(locator), "\x00", 8)
	if len(fields) != 7 {
		return nil, errors.New("invalid ID-only dialog locator")
	}
	partition, err := strconv.ParseUint(fields[3], 10, 12)
	if err != nil {
		return nil, errors.New("invalid ID-only dialog partition")
	}
	ref := &pb.ContextRef{Id: fields[0], State: &pb.StateRef{
		Authority: proto.Clone(authority).(*pb.StateAuthority), GroupId: fields[1], Incarnation: fields[2],
		Partition: uint32(partition), CallId: fields[4],
	}}
	if fields[5] != "" || fields[6] != "" {
		ref.Owner = &pb.ProcessRef{InstanceId: fields[5], Epoch: fields[6]}
	}
	expected, err := dialogIDOnlyFor(authority, key, ref)
	if err != nil || !hmac.Equal([]byte(expected), []byte(id)) {
		return nil, errors.New("ID-only dialog reference authentication failed")
	}
	return ref, nil
}
