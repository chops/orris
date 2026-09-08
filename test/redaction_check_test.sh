#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
scanner="$repo_root/bin/redaction-check"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/redaction-check.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

fail() {
  printf 'redaction-check test failed: %s\n' "$1" >&2
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

mkdir "$tmp/repo"
cd "$tmp/repo"
git init -q
git config user.name test
git config user.email test@example.invalid
printf 'safe\n' >tracked.txt
git add tracked.txt
git commit -qm baseline

run_capture 0 "$scanner"

pane_sample="pane ""%""196 ready"
printf '%s\n' "$pane_sample" >tracked.txt
run_capture 0 "$scanner" --staged
run_capture 1 "$scanner"
[[ "$output" == *"tracked.txt:1:tmux_pane_id"* ]] || fail "tracked finding lacks its masked locator"
[[ "$output" != *"$pane_sample"* ]] || fail "tracked finding printed matched content"

git add tracked.txt
run_capture 1 "$scanner" --staged
[[ "$output" == *"tracked.txt:1:tmux_pane_id"* ]] || fail "staged finding lacks its masked locator"

mkdir nested
cd nested
run_capture 1 "$scanner"
[[ "$output" == *"tracked.txt:1:tmux_pane_id"* ]] || fail "nested invocation did not scan from the Git root"
cd ..

printf 'format %%42s\n' >tracked.txt
git add tracked.txt
run_capture 0 "$scanner" --staged

external="$tmp/evidence"
mkdir "$external"

pattern_names=(
  macos_home
  linux_home
  user_profile
  agent_inbox
  tmux_pane_id
  private_key
  anthropic_token
  provider_token
  github_token
  github_pat
  aws_access_key
  slack_token
  gitlab_token
  google_api_key
  jwt_token
  bearer_token
  url_basic_auth
)

samples=(
  "/""Users/alice/project/"
  "/""home/alice/project/"
  "/etc/profiles/per-""user/alice/bin/tool"
  ".""ai-agent-inbox/example-deadbeef/inbox"
  "pane ""%""204 ready"
  "-----""BEGIN PGP PRIVATE KEY BLOCK-----"
  "s""k-ant-abcdefghijklmnopqrstuvwxyz123456"
  "s""k-abcdefghijklmnopqrstuvwxyz1234567890"
  "g""hp_abcdefghijklmnopqrstuvwxyz123456"
  "github_""pat_abcdefghijklmnopqrstuvwxyz123456"
  "A""SIAABCDEFGHIJKLMNOP"
  "x""oxr-abcdefghijklmnopqrstuvwxyz123456"
  "g""lpat-abcdefghijklmnopqrstuvwxyz123456"
  "A""Izaabcdefghijklmnopqrstuvwxyz123456789"
  "e""yJabcdefghijklmno.eyJpqrstuvwxyz123."
  "B""earer abcdefghijklmnopqrstuvwxyz123456"
  "http""s://user:password@example.invalid/path"
)

for index in "${!pattern_names[@]}"; do
  sample=${samples[$index]}
  pattern=${pattern_names[$index]}
  printf '%s\n' "$sample" >"$external/sample.txt"
  run_capture 1 "$scanner" --paths "$external"
  [[ "$output" == *"<external-1>/sample.txt:1:$pattern"* ]] || fail "$pattern was not reported"
  [[ "$output" != *"$sample"* ]] || fail "$pattern printed matched content"
done

generic_token="s""k-abcdefghijklmnopqrstuvwxyz1234567890"
printf '%s\n' "$generic_token" >"$external/sample.txt"
run_capture 1 env PATH=/bin:/usr/bin "$scanner" --paths "$external"
[[ "$output" == *"<external-1>/sample.txt:1:provider_token"* ]] || fail "grep fallback missed a provider token"

anthropic_token="s""k-ant-abcdefghijklmnopqrstuvwxyz123456"
printf '%s\n' "$anthropic_token" >"$external/sample.txt"
run_capture 1 "$scanner" --paths "$external"
[[ $(grep -c ':anthropic_token$' <<<"$output") -eq 1 ]] || fail "Anthropic token was not reported once"
[[ "$output" != *":provider_token"* ]] || fail "Anthropic token was reported twice"

printf '%s\n' "$generic_token" >"$external/sample.txt"
run_capture 1 "$scanner" --locators --paths "$external"
locator=$(grep '^provider_token:<external-1>/sample.txt:' <<<"$output")
[[ -n "$locator" ]] || fail "locator mode did not emit a provider-token locator"
[[ "$locator" != *"$generic_token"* ]] || fail "locator mode disclosed matched content"
printf '%s # reviewed synthetic test value\n' "$locator" >allow
run_capture 0 "$scanner" --allowlist allow --paths "$external"

printf '\n%s\n' "$generic_token" >"$external/sample.txt"
run_capture 0 "$scanner" --allowlist allow --paths "$external"

changed_token="${generic_token}x"
printf '%s\n' "$changed_token" >"$external/sample.txt"
run_capture 1 "$scanner" --allowlist allow --paths "$external"
[[ "$output" != *"$changed_token"* ]] || fail "changed allowlisted value was disclosed"

printf '%s\n' "$locator" >allow
run_capture 2 "$scanner" --allowlist allow --paths "$external"
[[ "$output" == *"needs an exact locator"* ]] || fail "malformed allowlist was not explained"

printf 'redaction-check tests: ok\n'
