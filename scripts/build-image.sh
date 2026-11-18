#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/verify_dependency.py
python3 scripts/verify_control_schema.py
: "${SIPBRIDGE_IMAGE:?Set an immutable qualification image tag}"
docker build --build-arg BUILD_VERSION="${BUILD_VERSION:-dev}" --build-arg BUILD_REVISION="$(git rev-parse HEAD)" -t "$SIPBRIDGE_IMAGE" .
