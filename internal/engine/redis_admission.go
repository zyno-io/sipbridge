package engine

import (
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

func redisInstanceAdmission(key []byte, instance *pb.ProcessRef, class rediscore.AdmissionClass, source string) rediscore.Admission {
	admission := rediscore.Admission{
		Instance: nativeIdentity(key, "admission-instance", instance.GetInstanceId()),
		Class:    class,
	}
	if class == rediscore.AdmissionPending {
		if address := normalizedAdmissionSource(source); address != "" {
			admission.Source = nativeIdentity(key, "admission-source", address)
		}
	}
	return admission
}

func redisInviteCompletionCredit(transport string) int64 {
	proofs := stateTransactionEmissions
	if transport == "udp" {
		proofs *= 2
	}
	return int64(8*stateWireLimit + proofs*stateProofMetadataBytes)
}

const redisAdditionalForkCredit = 4*stateWireLimit + 4*stateProofMetadataBytes
const redisEstablishedInviteCredit = 8*stateWireLimit + 4*stateProofMetadataBytes

func releaseRedisSettledSetupCredit(current *nativepb.Context, procedure *nativepb.Procedure) int64 {
	if current.Ended || !current.Answered || !current.InitialAck || len(current.Dialogs) != 1 ||
		len(current.InitialProcedures) != 1 || len(current.NoninitialProcedures) != 0 ||
		len(procedure.Outputs) != 0 || len(procedure.Events) != 0 || procedure.SuccessfulAck != nil ||
		procedure.CreditContribution <= redisEstablishedInviteCredit {
		return 0
	}
	delta := redisEstablishedInviteCredit - int64(procedure.CreditContribution)
	procedure.CreditContribution = redisEstablishedInviteCredit
	return delta
}
