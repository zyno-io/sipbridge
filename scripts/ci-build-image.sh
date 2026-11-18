#!/usr/bin/env bash
set -euo pipefail
: "${CI_REGISTRY_IMAGE:?required}"
: "${CI_COMMIT_SHA:?required}"
: "${IMAGE_CHANNEL:?required}"
case "$IMAGE_CHANNEL" in
    main|canary) ;;
    *) printf 'unsupported image channel\n' >&2; exit 1 ;;
esac
build_date="$(TZ=UTC0 git show -s --format=%cd --date=format-local:'%y.%-m%d.%-H%M' "$CI_COMMIT_SHA")"
test -n "$build_date"
BUILD_VERSION="canary-${build_date}"
commit_image="${CI_REGISTRY_IMAGE}:${CI_COMMIT_SHA}"
buildah bud \
    --build-arg "BUILD_VERSION=${BUILD_VERSION}" \
    --build-arg "BUILD_REVISION=${CI_COMMIT_SHA}" \
    -t "$commit_image" \
    -t "${CI_REGISTRY_IMAGE}:${IMAGE_CHANNEL}" .
verify_container="$(buildah from "$commit_image")"
trap 'buildah rm "$verify_container" > /dev/null 2>&1 || true' EXIT
buildah run "$verify_container" -- /sipbridge --version > image-version.txt
cat image-version.txt
grep -Fx "sipbridge ${BUILD_VERSION}" image-version.txt
buildah rm "$verify_container"
trap - EXIT
buildah push --digestfile image.digest "$commit_image"
buildah push "${CI_REGISTRY_IMAGE}:${IMAGE_CHANNEL}"
image_digest="$(cat image.digest)"
case "$image_digest" in
    sha256:*) ;;
    *) printf 'registry returned an invalid image digest\n' >&2; exit 1 ;;
esac
printf 'SIPBRIDGE_IMAGE=%s@%s\nSIPBRIDGE_IMAGE_DIGEST=%s\nBUILD_VERSION=%s\n' \
    "$commit_image" "$image_digest" "$image_digest" "$BUILD_VERSION" > image.env
