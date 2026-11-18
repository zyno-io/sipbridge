#!/usr/bin/env bash
# Publish the reviewed source commit to the public repository.
set -euo pipefail

# Keep publication on the explicitly configured repository-scoped SSH identity.
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

root=$(git rev-parse --show-toplevel)
revision=$(git -C "$root" rev-parse HEAD)
remote=${SIPBRIDGE_DOCS_PUBLISH_REMOTE:-git@github.com:zyno-io/sipbridge.git}
temporary=$(mktemp -d)
trap 'rm -rf -- "$temporary"' EXIT

if [[ -n ${SIPBRIDGE_DOCS_GITHUB_SSH_KEY_B64:-} ]]; then
    printf '%s' "$SIPBRIDGE_DOCS_GITHUB_SSH_KEY_B64" | base64 -d > "$temporary/key"
    chmod 600 "$temporary/key"
    export GIT_SSH_COMMAND="ssh -i $temporary/key -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=$root/scripts/github-known-hosts"
fi

# Both repositories share the same clean initial commit and subsequent history.
# Publish only the reviewed commit; never manufacture another snapshot or force
# an unexpected public history change.
git -C "$root" push "$remote" "$revision:refs/heads/main"
