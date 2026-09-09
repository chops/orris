#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd -P)"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/bin" "$fixture/docs"
cp "$root/bin/check-documentation-format" "$fixture/bin/"
git -C "$fixture" init -q
: > "$fixture/docs/markdown-exceptions.tsv"
printf '* Ordinary documentation\n' > "$fixture/README.org"
check() { bash "$fixture/bin/check-documentation-format"; }
reject() {
  if check > "$fixture/output" 2>&1; then
    printf 'documentation guard accepted invalid fixture: %s\n' "$1" >&2
    exit 1
  fi
}
check
printf '# Unexpected Markdown\n' > "$fixture/docs/untracked note.MD"
reject 'untracked Markdown with spaces and uppercase extension'
git -C "$fixture" add 'docs/untracked note.MD'
reject 'tracked Markdown'
rm "$fixture/docs/untracked note.MD"
check
printf '# Tool instructions\n' > "$fixture/AGENTS.md"
printf 'AGENTS.md\tConsumed by the agent harness under this exact filename\n' > "$fixture/docs/markdown-exceptions.tsv"
check
printf '# Unexpected alternate extension\n' > "$fixture/docs/unlisted.markdown"
reject 'unlisted alternate Markdown extension despite another exception'
rm "$fixture/docs/unlisted.markdown"
printf 'AGENTS.md\t\n' > "$fixture/docs/markdown-exceptions.tsv"
reject 'exception without consumer reason'
printf 'missing.md\tStale consumer requirement\n' > "$fixture/docs/markdown-exceptions.tsv"
reject 'stale exception'
printf 'documentation format tests: PASS\n'
