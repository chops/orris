#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
validator="$root/bin/validate-dialyzer-baseline"
tmp="$(mktemp -d "${TMPDIR:-/tmp}/dialyzer-baseline.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

expect() {
  local expected=$1
  local file=$2
  local actual

  set +e
  "$validator" "$file" >"$tmp/stdout" 2>"$tmp/stderr"
  actual=$?
  set -e

  [[ "$actual" -eq "$expected" ]] || {
    printf 'dialyzer baseline test failed: expected %s, got %s for %s\n' \
      "$expected" "$actual" "$file" >&2
    cat "$tmp/stderr" >&2
    exit 1
  }
}

printf '[{"lib/example.ex", "Function example/0 has no local return."}]\n' >"$tmp/valid.exs"
printf '[{"lib/example.ex", "same"}, {"lib/example.ex", "same"}]\n' >"$tmp/duplicate.exs"
printf '[{"test/example.exs", "wrong tree"}]\n' >"$tmp/non-lib.exs"
printf '[{"lib/../test/example.exs", "path escape"}]\n' >"$tmp/path-escape.exs"
printf '[{"lib/example.ex", :no_return}]\n' >"$tmp/warning-class.exs"
printf '[{"lib/example.ex"}]\n' >"$tmp/path-only.exs"
printf '[~r/example/]\n' >"$tmp/regex.exs"
printf 'File.write!("%s", "bad")\n' "$tmp/not-data" >"$tmp/executable.exs"
printf '[{"lib/example.ex", "unterminated"}\n' >"$tmp/malformed.exs"

expect 0 "$tmp/valid.exs"
for rejected in duplicate non-lib path-escape warning-class path-only regex executable malformed; do
  expect 2 "$tmp/$rejected.exs"
done

[[ ! -e "$tmp/not-data" ]] || {
  printf 'dialyzer baseline test failed: validator executed baseline code\n' >&2
  exit 1
}

printf 'dialyzer baseline tests: ok\n'
