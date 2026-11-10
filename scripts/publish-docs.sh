#!/usr/bin/env bash
# Publish the reviewed current source tree without copying private Git history.
set -euo pipefail

# Keep publication on the explicitly configured repository-scoped SSH identity.
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

root=$(git rev-parse --show-toplevel)
revision=$(git -C "$root" rev-parse HEAD)
remote=${SIPBRIDGE_DOCS_PUBLISH_REMOTE:-git@github.com:zyno-io/sipbridge.git}
temporary=$(mktemp -d)
trap 'rm -rf -- "$temporary"' EXIT
mkdir "$temporary/export" "$temporary/repository"

if [[ -n ${SIPBRIDGE_DOCS_GITHUB_SSH_KEY_B64:-} ]]; then
    printf '%s' "$SIPBRIDGE_DOCS_GITHUB_SSH_KEY_B64" | base64 -d > "$temporary/key"
    chmod 600 "$temporary/key"
    export GIT_SSH_COMMAND="ssh -i $temporary/key -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=$root/scripts/github-known-hosts"
fi

# Archive only committed files. This includes the service, canonical protocol,
# generated contracts, pinned SIP library, build files and documentation.
# A separate public history receives ordinary fast-forward snapshot commits.
git -C "$root" archive "$revision" | tar -x -C "$temporary/export"
printf '%s\n' "$revision" > "$temporary/export/SOURCE_REVISION"

git -C "$temporary/repository" init -q -b main
git -C "$temporary/repository" remote add origin "$remote"
git -C "$temporary/repository" ls-remote origin refs/heads/main > "$temporary/ref"
if [[ -s "$temporary/ref" ]]; then
    git -C "$temporary/repository" fetch -q --depth=1 origin main
    git -C "$temporary/repository" reset -q --hard FETCH_HEAD
    git -C "$temporary/repository" rm -qr --ignore-unmatch .
fi
cp -R "$temporary/export/." "$temporary/repository/"
git -C "$temporary/repository" add -A
if git -C "$temporary/repository" diff --cached --quiet; then
    echo 'Current source snapshot already published'
    exit 0
fi
git -C "$temporary/repository" -c user.name='sipbridge publisher' \
    -c user.email='codex@openai.com' commit -q \
    -m "Publish source snapshot $revision" \
    -m 'Co-authored-by: Codex <codex@openai.com>'
git -C "$temporary/repository" push origin HEAD:refs/heads/main
