#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
python3 scripts/verify_dependency.py
native_test_dir="$(mktemp -d)"
trap 'rm -rf "$native_test_dir"' EXIT HUP INT TERM
cp -a third_party/sipgo "$native_test_dir/sipgo"
python3 scripts/prepare-native-fixtures.py "$native_test_dir/sipgo"
cd "$native_test_dir/sipgo"
go generate ./...
go test ./...
go test -race ./sip/portable
go test -race . -run 'TestDialog(Directional|ByeAndAck|IndependentRemote|FirstRemote|LocalSequence|ServerAckAfter|ByeRemote|RemoteCSeq|LocalBye)'
go test -race ./sip -run 'Test(ServerTxCancel|TransactionLayerCancelBorrowedBranch|TransactionIngress|TransactionLayerMatchClientResponse)'
