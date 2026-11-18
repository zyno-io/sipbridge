#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
protoc_bin=${PROTOC:-protoc}
[ "$($protoc_bin --version)" = 'libprotoc 3.21.12' ]
[ "$(protoc-gen-go --version)" = 'protoc-gen-go v1.36.12' ]
if [ "${1:-}" = '--check' ]; then
  output=$(mktemp -d)
  trap 'rm -rf "$output"' EXIT
  "$protoc_bin" -I proto --go_out="$output" --go_opt=module=sipbridge proto/sipbridge-native-records.proto
  cmp "$output/internal/nativepb/sipbridge-native-records.pb.go" internal/nativepb/sipbridge-native-records.pb.go
else
  "$protoc_bin" -I proto --go_out=. --go_opt=module=sipbridge proto/sipbridge-native-records.proto
fi
