#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
scanner="$repo_root/bin/check-product-name"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/check-product-name.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

fail() {
  printf 'product-name test failed: %s\n' "$1" >&2
  exit 1
}

run_capture() {
  local expected=$1
  shift
  set +e
  output=$("$@" 2>&1)
  status=$?
  set -e
  [[ $status -eq $expected ]] || fail "expected exit $expected, got $status: $output"
}

mkdir -p "$tmp/repo/docs" "$tmp/repo/deps" "$tmp/repo/_build" "$tmp/repo/.git" "$tmp/no-tools"
root="$tmp/repo"
printf 'ai-orchestrator protocol v2\n' >"$root/source.txt"
retired='ai-orchestrator''-v2'
for path in docs/history.org deps/old.txt _build/old.txt .git/old.txt; do
  printf '%s\n' "$retired" >"$root/$path"
done
run_capture 0 "$BASH" "$scanner" "$root"
[[ -z "$output" ]] || fail "clean scan emitted output"

printf 'safe\n%s\n' "$retired" >"$root/source.txt"
run_capture 1 "$BASH" "$scanner" "$root"
expected=$(printf 'forbidden prototype product-version marker:\nsource.txt:2:%s' "$retired")
[[ "$output" == "$expected" ]] || fail "finding or leading path normalization changed"

reverse='V2_AI''_ORCHESTRATOR'
printf '%s\n' "$reverse" >"$root/.hidden.txt"
run_capture 1 "$BASH" "$scanner" "$root"
[[ "$output" == *"source.txt:2:$retired"* ]] || fail "multiple findings lost the source hit"
[[ "$output" == *".hidden.txt:1:$reverse"* ]] || fail "reverse case-insensitive hidden hit missing"
[[ "$output" != *"./source.txt:"* && "$output" != *"./.hidden.txt:"* ]] || fail "leading ./ survived"

run_capture 2 "$BASH" "$scanner" "$tmp/missing"
[[ "$output" == 'usage: bin/check-product-name [root]' ]] || fail "invalid-root diagnostic changed"
run_capture 2 "$BASH" "$scanner" "$root" extra
[[ "$output" == 'usage: bin/check-product-name [root]' ]] || fail "extra argument was not refused"

run_capture 2 env PATH="$tmp/no-tools" "$BASH" "$scanner" "$root"
[[ "$output" == *'product-name scan failed (rg exit 127)'* ]] || fail "missing search executable did not fail closed"

printf 'product-name tests: ok\n'
