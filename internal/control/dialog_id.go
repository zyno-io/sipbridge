package control

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

const DialogRetirementCapability = "sip.dialog-retirement.v1"

func encodeDialogID(capability, version string, authority *pb.StateAuthority, key []byte, digest, payload string) string {
	mac := hmac.New(sha256.New, key)
	for _, field := range []string{capability, version, authority.Service, authority.Generation,
		authority.PrimaryId, strconv.FormatUint(uint64(authority.SchemaVersion), 10), digest, payload} {
		_, _ = fmt.Fprintf(mac, "%d:%s", len(field), field)
	}
	return version + "." + digest + "." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// Use the same byte representation as the existing Nexus directory key.
func dialogDirectoryDigest(ref *pb.ContextRef) string {
	state := ref.State
	authority := state.Authority
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	// An array of strings written into a buffer cannot fail to encode.
	_ = encoder.Encode([]string{authority.Service, authority.Generation, authority.PrimaryId, state.GroupId,
		state.Incarnation, state.CallId, strconv.FormatUint(uint64(state.Partition), 10), ref.Id})
	raw := bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'})
	// Go escapes these separators in JSON. ECMAScript writes them literally.
	// An escaped backslash is consumed as a pair so a literal "\\u2028" stays literal.
	normalized := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		if raw[i] == '\\' {
			if i+6 <= len(raw) && bytes.Equal(raw[i:i+5], []byte(`\u202`)) && (raw[i+5] == '8' || raw[i+5] == '9') {
				normalized = append(normalized, 0xe2, 0x80, 0xa8+raw[i+5]-'8')
				i += 5
				continue
			}
			normalized = append(normalized, raw[i], raw[i+1])
			i++
			continue
		}
		normalized = append(normalized, raw[i])
	}
	digest := sha256.Sum256(normalized)
	return hex.EncodeToString(digest[:])
}

// Only these control fields carry ContextRef. Inspect both references where a
// result or effect can carry more than one, even if they point to the same call.
func envelopeDialogReferences(e *pb.Envelope) []*pb.ContextRef {
	switch payload := e.GetPayload().(type) {
	case *pb.Envelope_Ingress:
		return []*pb.ContextRef{payload.Ingress.GetContext()}
	case *pb.Envelope_Ingest:
		return []*pb.ContextRef{payload.Ingest.GetContext()}
	case *pb.Envelope_Request:
		return []*pb.ContextRef{payload.Request.GetSnapshot().GetContext(), payload.Request.GetFlowAdoption().GetContext()}
	case *pb.Envelope_Command:
		return []*pb.ContextRef{payload.Command.GetContext()}
	case *pb.Envelope_RequestDecision:
		return []*pb.ContextRef{payload.RequestDecision.GetContext()}
	case *pb.Envelope_Result:
		return []*pb.ContextRef{payload.Result.GetSnapshot().GetContext(), payload.Result.GetRetirementProof().GetContext()}
	case *pb.Envelope_Egress:
		return []*pb.ContextRef{payload.Egress.GetSnapshot().GetContext(), payload.Egress.GetAuthorization().GetEffect().GetContext()}
	case *pb.Envelope_Write:
		return flowWriteDialogReferences(payload.Write)
	case *pb.Envelope_DeliverEffect:
		return flowWriteDialogReferences(payload.DeliverEffect)
	case *pb.Envelope_InspectEffect:
		return flowWriteDialogReferences(payload.InspectEffect)
	case *pb.Envelope_Delivery:
		return []*pb.ContextRef{payload.Delivery.GetEffect().GetContext()}
	case *pb.Envelope_SendPermit:
		return []*pb.ContextRef{payload.SendPermit.GetAuthorization().GetEffect().GetContext()}
	case *pb.Envelope_DialogEvent:
		return []*pb.ContextRef{payload.DialogEvent.GetSnapshot().GetContext()}
	case *pb.Envelope_Inspect:
		return []*pb.ContextRef{payload.Inspect.GetContext()}
	case *pb.Envelope_FlowAdoption:
		return []*pb.ContextRef{payload.FlowAdoption.GetContext()}
	case *pb.Envelope_CredentialLookup:
		return []*pb.ContextRef{payload.CredentialLookup.GetContext()}
	case *pb.Envelope_ProbeApplication:
		return []*pb.ContextRef{payload.ProbeApplication.GetContext()}
	case *pb.Envelope_ApplicationStatus:
		return []*pb.ContextRef{payload.ApplicationStatus.GetContext()}
	case *pb.Envelope_ControllerContextsResult:
		return payload.ControllerContextsResult.GetContexts()
	default:
		return nil
	}
}

func flowWriteDialogReferences(write *pb.FlowWrite) []*pb.ContextRef {
	return []*pb.ContextRef{write.GetEffect().GetContext(), write.GetPermit().GetAuthorization().GetEffect().GetContext()}
}

func projectDialogIDs(e *pb.Envelope, authority *pb.StateAuthority, key []byte) (*pb.Envelope, error) {
	if proof := e.GetResult().GetRetirementProof(); proof != nil &&
		(proof.GetContext().GetState() == nil || proof.GetAuthority() != nil) {
		return nil, errors.New("invalid scoped dialog retirement proof")
	}
	for _, ref := range envelopeDialogReferences(e) {
		if ref.GetState() == nil {
			continue
		}
		wire := proto.Clone(e).(*pb.Envelope)
		for _, scoped := range envelopeDialogReferences(wire) {
			if scoped.GetState() == nil {
				continue
			}
			id, err := dialogIDOnlyFor(authority, key, scoped)
			if err != nil {
				return nil, err
			}
			scoped.Owner, scoped.Id, scoped.State, scoped.OpaqueDialogId = nil, "", nil, id
		}
		return wire, nil
	}
	return e, nil
}

func normalizeDialogIDs(e *pb.Envelope, authority *pb.StateAuthority, key []byte, enabled bool) error {
	for _, ref := range envelopeDialogReferences(e) {
		if ref.GetOpaqueDialogId() == "" {
			continue
		}
		if !enabled {
			return errors.New("dialog IDs were not negotiated")
		}
		if ref.Id != "" || ref.Owner != nil || ref.State != nil {
			return errors.New("ID-only dialog reference carries companion fields")
		}
		normalized, err := decodeDialogIDOnly(authority, key, ref.OpaqueDialogId)
		if err != nil {
			return err
		}
		ref.Owner, ref.Id, ref.State, ref.OpaqueDialogId = normalized.Owner, normalized.Id, normalized.State, ""
	}
	return nil
}
