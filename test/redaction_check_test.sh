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
mkdir "$tmp/grep-fallback"
ln -s "$(command -v sha256sum)" "$tmp/grep-fallback/sha256sum"
run_capture 1 env PATH="$tmp/grep-fallback:/bin:/usr/bin" "$scanner" --paths "$external"
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
expected_digest=c363c84107e7366590728f1a3570e8311d6fe1df1912d9f07ad3afb2eaff8979
[[ "$locator" == "provider_token:<external-1>/sample.txt:$expected_digest" ]] || fail "locator digest changed"
printf '%s # reviewed synthetic test value\n' "$locator" >allow
run_capture 0 "$scanner" --allowlist allow --paths "$external"

printf '%s # reviewed without terminal newline' "$locator" >allow
run_capture 0 "$scanner" --allowlist allow --paths "$external"
printf '%sx # not the exact locator\n' "$locator" >allow
run_capture 1 "$scanner" --allowlist allow --paths "$external"
printf '%s # reviewed synthetic test value\n' "$locator" >allow

printf '\n%s\n' "$generic_token" >"$external/sample.txt"
run_capture 0 "$scanner" --allowlist allow --paths "$external"

changed_token="${generic_token}x"
printf '%s\n' "$changed_token" >"$external/sample.txt"
run_capture 1 "$scanner" --allowlist allow --paths "$external"
[[ "$output" != *"$changed_token"* ]] || fail "changed allowlisted value was disclosed"

printf '%s\n' "$locator" >allow
run_capture 2 "$scanner" --allowlist allow --paths "$external"
[[ "$output" == *"needs an exact locator"* ]] || fail "malformed allowlist was not explained"

mkdir "$tmp/no-hash"
ln -s "$(command -v grep)" "$tmp/no-hash/grep"
ln -s "$(command -v basename)" "$tmp/no-hash/basename"
run_capture 2 env PATH="$tmp/no-hash" "$BASH" "$scanner" --paths "$external/sample.txt"
[[ "$output" == *'sha256sum is required'* ]] || fail "missing native hash tool did not fail closed"
[[ "$output" != *'redaction-check: clean'* ]] || fail "missing native hash tool reported clean"

# Renamed multicall coreutils binaries would select sha256sum instead of false.
printf '#!%s\nexit 1\n' "$BASH" >"$tmp/no-hash/sha256sum"
chmod +x "$tmp/no-hash/sha256sum"
run_capture 2 env PATH="$tmp/no-hash" "$BASH" "$scanner" --paths "$external/sample.txt"
[[ "$output" == *'sha256sum failed'* ]] || fail "native hash failure did not fail closed"
[[ "$output" != *'redaction-check: clean'* ]] || fail "native hash failure reported clean"

run_labeled() {
  local label=$1 expected=$2
  shift 2
  set +e
  output=$("$@" 2>&1)
  status=$?
  set -e
  [[ $status -eq $expected ]] || fail "$label: expected exit $expected, got $status: $output"
}

# ---- NS-30.M.002 artifact classes (R12 S6) ----
#
# The row's acceptance is "Scan fixtures/evidence/prompts/logs/packages" and its failure control is
# "Seed secret into each artifact class; broad exclusions cannot hide it". The cells above prove one
# pattern at a time against one file at the root of the scanned tree; they do not prove that a seeded
# secret is found in each named class, nor that an allowlist entry cannot be widened to hide one.
#
# Each class carries a DISTINCT synthetic secret of a DISTINCT pattern, so a finding names its class
# by path and no two classes can be satisfied by the same match.

classes=(
  fixtures/journals/seeded/events.jsonl
  evidence/record.org
  prompts/assignment.json
  logs/run.log
  packages/manifest.json
)

class_patterns=(
  macos_home
  agent_inbox
  anthropic_token
  tmux_pane_id
  bearer_token
)

class_samples=(
  "/""Users/artifact-fixture/run/"
  ".""ai-agent-inbox/artifact-evidence-0badf00d/inbox"
  "s""k-ant-artifactpromptAAAAAAAAAAAAAAAAAAA"
  "pane ""%""907 artifact-log"
  "B""earer artifactpackageAAAAAAAAAAAAAAAAAAAA"
)

artifacts="$tmp/artifacts"
rm -rf "$artifacts"

for index in "${!classes[@]}"; do
  class_path="$artifacts/${classes[$index]}"
  mkdir -p "$(dirname "$class_path")"
  printf 'entry one\n%s\nentry three\n' "${class_samples[$index]}" >"$class_path"
done

# every class is reported, on its own line, by its own pattern, without echoing the value
run_labeled "seeded artifact classes" 1 "$scanner" --paths "$artifacts"

for index in "${!classes[@]}"; do
  [[ "$output" == *"<external-1>/${classes[$index]}:2:${class_patterns[$index]}"* ]] ||
    fail "artifact class ${classes[$index]} was not reported as ${class_patterns[$index]}: $output"
  [[ "$output" != *"${class_samples[$index]}"* ]] || fail "artifact class ${classes[$index]} printed matched content"
done

[[ "$output" == *"redaction-check: ${#classes[@]} finding(s)"* ]] ||
  fail "the seeded artifact classes did not produce ${#classes[@]} findings: $output"

# the same five classes as TRACKED files: the mode bin/verify actually runs (bin/verify, redaction stage)
tracked_repo="$tmp/artifact-repo"
mkdir "$tracked_repo"
cp -R "$artifacts/." "$tracked_repo/"
cd "$tracked_repo"
git init -q
git config user.name test
git config user.email test@example.invalid
git add "${classes[@]}"
git commit -qm artifacts
run_labeled "tracked artifact classes" 1 "$scanner"

for index in "${!classes[@]}"; do
  [[ "$output" == *"${classes[$index]}:2:${class_patterns[$index]}"* ]] ||
    fail "tracked artifact class ${classes[$index]} was not reported: $output"
done

cd "$tmp/repo"

# the exclusion half: an allowlist entry is exact in pattern, path AND value, and nothing in it globs
run_capture 1 "$scanner" --locators --paths "$artifacts"
class_locators=()

for index in "${!classes[@]}"; do
  class_locator=$(grep "^${class_patterns[$index]}:<external-1>/${classes[$index]}:" <<<"$output") ||
    fail "no locator for ${classes[$index]}"
  class_locators+=("$class_locator")
done

# one exact entry allows exactly its own finding and no other class
printf '%s # reviewed synthetic artifact-class value\n' "${class_locators[0]}" >allow
run_labeled "one exact allowlist entry" 1 "$scanner" --allowlist allow --paths "$artifacts"
[[ "$output" != *"<external-1>/${classes[0]}:2:${class_patterns[0]}"* ]] || fail "an exact locator did not allow its own finding"

[[ "$output" == *"redaction-check: $((${#classes[@]} - 1)) finding(s)"* ]] ||
  fail "one allowlist entry suppressed more than its own finding: $output"

: >allow
for class_locator in "${class_locators[@]}"; do
  printf '%s # reviewed synthetic artifact-class value\n' "$class_locator" >>allow
done
run_capture 0 "$scanner" --allowlist allow --paths "$artifacts"

# the SAME allowlisted value in a different class is a new finding: the entry is path-exact
cp "$artifacts/${classes[0]}" "$artifacts/logs/copied.jsonl"
run_labeled "allowlisted value at a new path" 1 "$scanner" --allowlist allow --paths "$artifacts"

[[ "$output" == *"<external-1>/logs/copied.jsonl:2:${class_patterns[0]}"* ]] ||
  fail "an allowlisted value stayed hidden at a new path: $output"

rm "$artifacts/logs/copied.jsonl"

# no broader spelling of that entry hides the finding: not a path glob, not a pattern-wide entry,
# not a bare path without the pattern and digest
for broad in \
  "${class_patterns[0]}:<external-1>/fixtures/journals/seeded/*" \
  "${class_patterns[0]}:<external-1>/*" \
  "${class_patterns[0]}:*" \
  "${class_patterns[0]}" \
  "*:*:*" \
  "*" \
  "<external-1>/${classes[0]}"; do
  printf '%s # broad exclusion probe\n' "$broad" >allow
  run_labeled "broad exclusion probe $broad" 1 "$scanner" --allowlist allow --paths "$artifacts"

  [[ "$output" == *"<external-1>/${classes[0]}:2:${class_patterns[0]}"* ]] ||
    fail "a broad exclusion hid a real finding: $broad"
done

# the repository's own allowlist carries no entry of any broader shape
if [[ -s "$repo_root/.redaction-allow" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^[[:space:]]*$ || "$line" =~ ^[[:space:]]*# ]] && continue
    entry_locator=${line%% \# *}
    [[ "$entry_locator" != *"*"* ]] || fail "the repository allowlist carries a glob: $entry_locator"
    entry_digest=${entry_locator##*:}
    [[ "$entry_digest" =~ ^[0-9a-f]{64}$ ]] || fail "the repository allowlist entry is not value-exact: $entry_locator"
  done <"$repo_root/.redaction-allow"
fi

printf 'redaction-check tests: ok\n'
