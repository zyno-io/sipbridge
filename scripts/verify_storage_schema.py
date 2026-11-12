"""Verify the private persisted codec independently of the public control receipt."""
import argparse
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--write', action='store_true')
args = parser.parse_args()
files = [
    'proto/sipbridge-storage.proto', 'proto/sipbridge-storage.fields.json',
    'proto/sipbridge-control.proto', 'dependencies/sipgo.sha256',
    'internal/storagepb/sipbridge-storage.pb.go', 'scripts/gen-state-storage.py',
    'scripts/gen_state_storage_direct.py', 'internal/engine/storage_direct_generated.go',
    'internal/storagecodec/validated_frame.go',
    'internal/engine/storage_generate_test.go', 'internal/engine/storage_generated.go',
    'internal/state/storage_generated.go', 'internal/storagecodec/codec.go',
    'internal/storagecodec/receipt.go', 'internal/state/metadata.go', 'internal/state/view.go',
    'internal/engine/state_engine_binary.go', 'internal/state/binary.go',
    'internal/engine/state_engine_fixture_inspection.go', 'cmd/sipbridge-state-inspect/main.go',
    'internal/engine/state_engine.go', 'internal/engine/state_engine_reduce.go',
    'internal/engine/state_engine_receipts.go', 'internal/engine/state_engine_terminal_timers.go',
    'internal/engine/state_engine_selectors.go', 'internal/engine/wire.go',
    'internal/engine/state_engine_effects.go', 'internal/engine/state_engine_native_completion.go',
    'internal/engine/state_engine_lifecycle.go', 'internal/engine/state_engine_owner.go',
    'internal/engine/state_engine_inspect.go',
    'internal/engine/state_engine_capacity.go', 'internal/engine/state_engine_causality.go',
    'internal/engine/state_engine_admission.go', 'internal/engine/state_engine_subscription.go',
    'internal/engine/state_engine_auth.go', 'internal/engine/state_engine_response.go',
    'internal/engine/state_engine_policy_admission.go', 'internal/engine/state_engine_request_fence.go',
    'internal/state/types.go', 'internal/state/application_contexts.go', 'internal/state/application_lease.go',
    'internal/state/application_expiry.go', 'internal/state/application_lease_guard.go', 'internal/state/input_binding.go',
    'internal/state/timer_workers.go', 'internal/state/dispatch.go', 'internal/state/scheduler.go',
    'internal/state/scripts.go', 'internal/state/store.go', 'internal/state/update.go',
    'internal/state/receipt_result.go',
    'internal/state/memory.go', 'internal/state/memory_application_expiry.go',
    'internal/state/memory_application_recovery.go', 'internal/state/memory_dispatch.go',
    'internal/state/memory_timer_workers.go', 'internal/state/memory_scheduler.go',
    'internal/execution/cas.go', 'internal/execution/result.go', 'internal/execution/statistics.go',
    'internal/engine/state_engine_native_owner.go', 'internal/engine/state_engine_owner_expiry.go',
    'internal/engine/state_engine_scheduler.go', 'internal/engine/state_engine_scheduler_physical.go',
    'internal/engine/state_engine_waiters.go', 'internal/engine/ingress.go', 'internal/engine/ingress_drain.go',
    'internal/engine/state_engine_execution_metrics.go', 'internal/engine/state_engine_scheduler_metrics.go'
]
receipt = {'encoding': 'protobuf-v1', 'source': 'proto/sipbridge-storage.proto',
           'protoc': '3.21.12', 'protoc_gen_go': '1.36.12',
           'sha256': {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in files}}
path = root / 'internal/storagecodec/receipt.json'
if args.write:
    path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
elif not path.exists() or json.loads(path.read_text()) != receipt:
    raise SystemExit('private storage schema/generated codec drift; regenerate and review the storage receipt')
print('private retained protobuf-v1 codec receipt verified')
