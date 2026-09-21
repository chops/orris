#!/usr/bin/env bash
# ci/c107-mutations/runner.bash
# FOCUSED C1-07c HOSTED MUTATION CONTROLLER, ONE VARIANT PER INVOCATION.
# PREPARED, NEVER RUN. Nothing in this file has been executed anywhere.
#
# GOVERNING RECORD: C107-HOSTED-PREPARATION-SCOPE-20260921.org, sha256
#   59e7bab568d7820408c5932c9cdda507f98b76990d1463bff9e8f95f8f1b350f, 75 lines,
#   read in full. It wins wherever more specific than this file.
#
# WHAT THIS IS, AND WHAT IT DELIBERATELY IS NOT
#   IS:  the bounded routines the r4 review left SOURCE-CLOSED -- the fractional
#        summary parser, its eleven-fixture self-test, the escaped-base regex
#        control and the thirteen verdict controls -- carried across unchanged in
#        behaviour, plus the overlay/identity checks a two-checkout hosted job needs.
#   NOT: the macOS driver. No clone, no /private/tmp lane, no gate lock, no browser
#        pin, no process-group ownership reasoning, no descendant-stop claim, no
#        finalizer/caller split. Scope :59 permits extracting the bounded routines
#        and forbids carrying the clone/lock runner; that separation is literal here.
#
# PROVENANCE MODEL (scope :14-17). The controller commit is NOT the product tree.
#   product/ is a FRESH checkout of exactly 7554f623d6695244aa3abc1fec259a14e886390e,
#   whose HEAD *and* tree are asserted against literals before anything is copied, so
#   the original baseline provenance is citable. Every later state is named as
#   "base 7554f623 plus overlay <sha256>", never as a new commit.
#
# EXIT DISPOSITION. This file runs ONCE per job. There is NO RETRY on any path.
#
# REFUSAL CODES, each distinct; the meanings are carried from the r4 driver so a
# reviewer who read that file does not have to relearn them:
#   60 frozen controller payload hash mismatch   61 product checkout identity wrong
#   62 product pre-state wrong                   63 evidence root mkdir refused
#   64 controller checkout identity wrong        (r2 amendment 4, added here)
#   65 required tool missing or not executable   66 candidate overlay hash mismatch
#   67 variant swap hash mismatch                68 changed-path set wrong
#   70 parser or verdict control failed          71 environment receipt refused
#   72 baseline not 6/6                          73 summary unrecognised
#   74 receipt write failed                      76 EXIT RECEIPT WRITE FAILED
#   77 exit receipt disagreed with rc            78 the stage ran no tests
#   79 moved-row count wrong                     81 a required read produced nothing
#   82 status neither 0 nor the pinned mix failure status
#   83 the variant passed                        84 witness location or row wrong
#   85 a setup stage returned non-zero           86 fractional form without companion
#   90 physical cwd wrong                        91 timeout, kill or setup status
#   93 contradictory or inconsistent counts
#
# R2 AMENDMENTS APPLIED, from C107-HOSTED-PREPARATION-ROOT-REVIEW-20260921.org sha256
#   523351004ffc94b94fceea076da35c5743ebed7c3227fd05755fbd308127799a, 91 lines, four
#   required amendments, read in full. Exactly the literal amendments; nothing widened:
#     1 qualification attribution -- a RECORD correction, no source change here, and no
#       substitute scanner or scanner control is added to this file or this lane.
#     2 enter and verify the PHYSICAL product cwd BEFORE nix develop, in run_stage,
#       because flake.nix :55-62 takes projectRoot from the CURRENT cwd.
#     3 changed_set asserts the EXACT non-ignored set INCLUDING untracked paths.
#     4 the ACTUAL controller HEAD is recorded against the workflow SHA (code 64).
#
# WHAT IS EXPECTED AND UNEXECUTED, stated so no reader mistakes it for measurement:
#   - that the focused module runs at all on Linux;
#   - that it is 6/6 green on the candidate over pristine product;
#   - that the pinned variant then yields exactly 5/6 with its C1-07c witness;
#   - that 600s is enough for either stage;
#   - that C1_BROWSER may stay unset (SOURCE-INDICATED at 7554f623, never run).
#   None of these is asserted as fact anywhere in this file; each is a predicate the
#   job must satisfy, and the refusal codes above are what happens when it does not.

set -u
umask 022

# --- inputs from the workflow -------------------------------------------------------
: "${C107_CONTROL:?C107_CONTROL (controller checkout) required}"
: "${C107_PRODUCT:?C107_PRODUCT (pinned product checkout) required}"
: "${C107_OUT:?C107_OUT (evidence root) required}"
: "${C107_VARIANT:?C107_VARIANT (a|b) required}"
: "${C107_VARIANT_FILE:?C107_VARIANT_FILE required}"
: "${C107_VARIANT_SHA:?C107_VARIANT_SHA required}"
: "${C107_WITNESS_LINE:?C107_WITNESS_LINE required}"
: "${C107_OTHER_LINE:?C107_OTHER_LINE required}"
# r2 amendment 4: the workflow SHA the controller checkout was asked for. Required and
# non-empty here, so a dropped or blank expansion refuses at the input boundary rather
# than turning the identity comparison below into a comparison against nothing.
: "${C107_WORKFLOW_SHA:?C107_WORKFLOW_SHA (github.sha) required}"

CONTROL=$C107_CONTROL
PRODUCT=$C107_PRODUCT
OUT=$C107_OUT
# r2 amendment 2 makes run_stage change the process cwd, so the three roots must not be
# cwd-relative. The workflow supplies github.workspace and runner.temp paths, which are
# absolute; this refuses anything else rather than letting a relative root silently
# retarget after the first stage. FAILS WHEN any of the three is relative; reachable by
# invoking this file with C107_OUT=evidence. It can pass: absolute paths take the ": ;;"
# branch, which is what every workflow-supplied value does.
case $CONTROL in /*) : ;; *) exit 90 ;; esac
case $PRODUCT in /*) : ;; *) exit 90 ;; esac
case $OUT     in /*) : ;; *) exit 90 ;; esac

# --- pins -----------------------------------------------------------------------------
BASE_COMMIT=7554f623d6695244aa3abc1fec259a14e886390e
BASE_TREE=222d2f6f394cfba214ac8a13eef2697e7d7e7073

CAND_REL=console/test/c1/c1_06_07_privacy_test.exs
TGT_REL=console/lib/orris_console/run_index_live.ex
TEST_REL=test/c1/c1_06_07_privacy_test.exs
TEST_BASE=c1_06_07_privacy_test.exs
# The SAME base with its dots ESCAPED, for -E use. ctl_regex below proves the two
# agree on the real base and that the unescaped form does NOT discriminate.
TEST_BASE_RE='c1_06_07_privacy_test\.exs'
MODULE='C1.PrivacyTest'

# frozen, independently hashed inputs (scope :35-37)
CAND_SHA=456adea092f36ba12485faccb0b9bb9e2430475f5ede1d3ca3605cbbf0199f9a
CAND_LINES=256
VA_SHA=578cf377c93c3fbb012c9cea8a248d2601b321868828561125c093b0bfd14c75
VB_SHA=d75d7345857706013373ab5058b1bdea130e3a682411fad62ae94fe0a434f30f
# what a FRESH checkout of the base legitimately contains BEFORE any overlay
CAND_COMMITTED_SHA=8357e1c68942329e4e7573ab50739282d7e5f5a6b0e0c397cebfe562d51ce81b
PRISTINE_SHA=1822c0efb03dc507f947bb20de9ee3134a1918b673d2889ce098a51eef146390

# Mix.Tasks.Test defaults exit_status to 2 and uses it on assertion failures. A run
# that fails an assertion exits 2 AND NOTHING ELSE IS ACCEPTED AS THAT OUTCOME.
MIX_TEST_FAIL_RC=2
KILL_CODES=(124 125 126 127 134 137 139 143)

ROW_DECL=163
A_LINE=208
B_LINE=232
ROW_NAME='C1-07c an event or refresh after revocation is refused before Query and the view terminates'
BASE_PASSED=6
VAR_PASSED=5
VAR_TOTAL=6
VAR_FAILED=1

# CHOSEN, NOT MEASURED (scope :50-51). No hosted timing for this module exists.
STAGE_BOUND=600s      # 10 minutes per focused baseline/variant stage
SETUP_BOUND=600s      # 10 minutes per dependency stage
KILL_AFTER=10s
# The console's own toolchain tag, literal at console/bin/verify:8 of the base commit.
TAG=elixir-1.20.4-otp-29.0.5

RE_ALL='^Result: ([0-9]+) passed$'
RE_ALL_BD='^Result: ([0-9]+) passed \([0-9][^)]*\)$'
RE_FRAC='^Result: ([0-9]+)/([0-9]+) passed$'
RE_FAILED='^Failed: ([0-9]+) tests?$'

# --- evidence root --------------------------------------------------------------------
mkdir -p "$OUT" || exit 63
[[ -d $OUT ]] || exit 63

w() { # w <code> <file> <text...>
  local code=$1 file=$2
  shift 2
  printf '%s\n' "$*" > "$file" || exit "$code"
}
# FAILS WHEN the evidence root is unwritable or full. Reachable: point C107_OUT at a
# read-only path and the first w() refuses. It CANNOT silently succeed: the redirection
# status is the function status and every call site supplies a refusal code.

# --- small readers: head, tail and grep -n only, no stream editor anywhere -------------
hash_of() { # hash_of <path>; prints the hash, or nothing and returns 1
  local o
  o=$(sha256sum "$1" 2>/dev/null) || { printf ''; return 1; }
  printf '%s' "${o%% *}"
}

assert_hash() { # assert_hash <code> <label> <path> <expected>
  local code=$1 label=$2 path=$3 want=$4 got
  got=$(hash_of "$path") || got=UNREADABLE
  [[ -n $got ]] || got=UNREADABLE
  w 74 "$OUT/hash.$label.txt" "$label PATH $path WANT $want GOT $got"
  [[ $got == "$want" ]] || exit "$code"
}
# The OBSERVED hash is written on BOTH paths before the comparison, so a mismatch leaves
# the actual value in evidence rather than only a code. FAILS WHEN the file is absent,
# unreadable or different. Reachable in both directions: the positive direction is taken
# on every correct run (the run cannot proceed without it), and the negative direction is
# what an edited frozen payload produces.

lines_in() { # lines_in <file>
  local n
  n=$(wc -l < "$1" 2>/dev/null) || n=-1
  printf '%s' "${n// /}"
}

count_fixed() { # count_fixed <file> <fixed string>
  local n=0
  if [[ -f $1 ]]; then n=$(grep -c -F -e "$2" "$1") || n=0; fi
  printf '%s' "$n"
}

count_re() { # count_re <file> <ERE>
  local n=0
  if [[ -f $1 ]]; then n=$(grep -c -E -e "$2" "$1") || n=0; fi
  printf '%s' "$n"
}

first_line_fixed() { # first_line_fixed <file> <fixed string>; LINE NUMBER, empty when absent
  local o
  [[ -f $1 ]] || { printf ''; return 0; }
  o=$(grep -n -m 1 -F -e "$2" "$1") || { printf ''; return 0; }
  printf '%s' "${o%%:*}"
}

first_line_re() { # first_line_re <file> <ERE>; LINE NUMBER, empty when absent
  local o
  [[ -f $1 ]] || { printf ''; return 0; }
  o=$(grep -n -m 1 -E -e "$2" "$1") || { printf ''; return 0; }
  printf '%s' "${o%%:*}"
}

nth_line() { # nth_line <file> <n>; head/tail only
  [[ -f $1 ]] || { printf ''; return 0; }
  head -n "$2" "$1" | tail -n 1
}

# --- the reviewed fractional-summary parser, behaviour unchanged from r4 ---------------
parse_summary() { # parse_summary <file>
  local file=$1 line last='' lastf=''
  P_PASSED=-1; P_TOTAL=-1; P_FAILED=-1; P_SEEN=0; P_FSEEN=0; P_UNREC=0
  [[ -f $file ]] || return 0
  while IFS= read -r line; do
    case $line in
      Result:*) P_SEEN=$((P_SEEN + 1)); last=$line ;;
      Failed:*) P_FSEEN=$((P_FSEEN + 1)); lastf=$line ;;
    esac
  done < "$file"
  [[ $P_SEEN -ge 1 ]] || return 0
  if [[ $last =~ $RE_ALL ]] || [[ $last =~ $RE_ALL_BD ]]; then
    # AN ALL-PASSED LINE DOES NOT IGNORE A CONTRADICTORY FAILURE RECORD.
    if [[ $P_FSEEN -ge 1 ]]; then
      P_UNREC=3
    else
      P_PASSED=${BASH_REMATCH[1]}; P_TOTAL=${BASH_REMATCH[1]}; P_FAILED=0
    fi
  elif [[ $last =~ $RE_FRAC ]]; then
    P_PASSED=${BASH_REMATCH[1]}; P_TOTAL=${BASH_REMATCH[2]}
    if [[ $P_FSEEN -eq 1 ]] && [[ $lastf =~ $RE_FAILED ]]; then
      P_FAILED=${BASH_REMATCH[1]}
    else
      P_UNREC=2
    fi
  else
    P_UNREC=1
  fi
  return 0
}
# THE COUNTERS INITIALISE TO REFUSING, NOT TO ZERO. An absent file, an empty file, a
# truncated stream, "Result: 0 tests" and any suffixed form all leave P_FAILED at -1.
# SILENCE IS NEVER READ AS ZERO FAILURES.
# MEASURED EMITTED FORMS (carried, not re-measured here): "Result: 6 passed" and the
# pair "Result: 2/3 passed" + "Failed: 1 test". A TOP-LEVEL "Result: 5/6 passed" with
# its companion is EXPECTED AND UNEXECUTED; the fractional pair was measured from a
# nested ExUnit.run emission.

consistent() { # total - passed must equal the reported failures
  [[ $P_TOTAL -ge 0 && $P_PASSED -ge 0 && $P_FAILED -ge 0 ]] || return 1
  [[ $((P_TOTAL - P_PASSED)) -eq $P_FAILED ]] || return 1
  return 0
}

is_kill_code() { # timeout, kill, signal and setup statuses
  local c=$1 k
  for k in "${KILL_CODES[@]}"; do
    if [[ $c -eq $k ]]; then return 0; fi
  done
  return 1
}

# --- parser self-test, eleven fixtures, through the SAME parser this run trusts -------
selftest() {
  local d=$1
  printf 'Result: 6 passed\n'                      > "$d/good.txt"        || exit 70
  printf 'Result: 6 passed (2 async)\n'            > "$d/good_bd.txt"     || exit 70
  printf 'Result: 0 tests\n'                       > "$d/zero.txt"        || exit 70
  printf 'Result: 6 passed, 1 excluded\n'          > "$d/suffixed.txt"    || exit 70
  printf 'Result: 5/6 passed\nFailed: 1 test\n'    > "$d/frac.txt"        || exit 70
  printf 'Result: 5/6 passed\n'                    > "$d/frac_nofail.txt" || exit 70
  printf 'Result: 0/0 passed\nFailed: 0 tests\n'   > "$d/frac_zero.txt"   || exit 70
  : > "$d/empty.txt"                                                      || exit 70
  printf 'Result: 6 passed\nFailed: 1 test\n'      > "$d/contra_all.txt"  || exit 70
  printf 'Result: 6 passed\nFailed: 0 tests\n'     > "$d/contra_zero.txt" || exit 70
  printf 'Result: 6/7 passed\nFailed: 0 tests\n'   > "$d/contra_frac.txt" || exit 70

  parse_summary "$d/good.txt";        [[ $P_PASSED -eq 6 && $P_TOTAL -eq 6 && $P_FAILED -eq 0 && $P_UNREC -eq 0 ]] || exit 70
  parse_summary "$d/good_bd.txt";     [[ $P_PASSED -eq 6 && $P_FAILED -eq 0 && $P_UNREC -eq 0 ]] || exit 70
  parse_summary "$d/zero.txt";        [[ $P_UNREC -eq 1 && $P_FAILED -eq -1 ]] || exit 70
  parse_summary "$d/suffixed.txt";    [[ $P_UNREC -eq 1 && $P_FAILED -eq -1 ]] || exit 70
  parse_summary "$d/frac.txt";        [[ $P_PASSED -eq 5 && $P_TOTAL -eq 6 && $P_FAILED -eq 1 && $P_UNREC -eq 0 ]] || exit 70
  parse_summary "$d/frac_nofail.txt"; [[ $P_UNREC -eq 2 && $P_FAILED -eq -1 ]] || exit 70
  parse_summary "$d/frac_zero.txt";   [[ $P_TOTAL -eq 0 && $P_PASSED -eq 0 && $P_FAILED -eq 0 ]] || exit 70
  parse_summary "$d/empty.txt";       [[ $P_SEEN -eq 0 && $P_PASSED -eq -1 && $P_FAILED -eq -1 ]] || exit 70
  parse_summary "$d/contra_all.txt";  [[ $P_UNREC -eq 3 && $P_FAILED -eq -1 && $P_PASSED -eq -1 ]] || exit 70
  parse_summary "$d/contra_zero.txt"; [[ $P_UNREC -eq 3 && $P_FAILED -eq -1 ]] || exit 70
  parse_summary "$d/contra_frac.txt"; [[ $P_PASSED -eq 6 && $P_TOTAL -eq 7 && $P_FAILED -eq 0 ]] || exit 70
  parse_summary "$d/contra_frac.txt"; if consistent; then exit 70; fi
  parse_summary "$d/frac.txt";        consistent || exit 70
  w 74 "$d/selftest.txt" 'PARSER-SELFTEST 11/11 all-passed frac zero-refused suffixed-refused companion-required empty-minus-1 all-passed-with-Failed-refused 6-of-7-with-Failed-0-inconsistent'
}
# FAILS WHEN any of the eleven behaves differently in this shell. Reachable: loosen one
# anchor, drop the companion requirement, or read a contradictory Failed line as a clean
# pass, and at least one case flips. The consistent() pair is what refuses "6/7 plus 0".
# It ALSO cannot trivially pass: eight of the eleven fixtures REQUIRE a refusing value
# (-1 or a non-zero P_UNREC), so a parser that accepted everything would fail here.

# --- the escaped-base regex controls ---------------------------------------------------
ctl_regex() {
  local d=$1
  printf '%s\n' "$TEST_BASE"                                  > "$d/base.txt"      || exit 70
  printf '%s\n' 'c1_06_07_privacy_testXexs'                   > "$d/baseneg.txt"   || exit 70
  printf '%s\n' 'c1X06X07XprivacyXtestXexs'                   > "$d/basefar.txt"   || exit 70
  printf '%s\n' '       test/c1/c1_06_07_privacy_test.exs:2080: (test)' > "$d/bound.txt" || exit 70
  # the escaped pattern accepts the real base and refuses the dot-substituted impostor
  [[ $(count_re "$d/base.txt"    "^${TEST_BASE_RE}$") -eq 1 ]] || exit 70
  [[ $(count_re "$d/baseneg.txt" "^${TEST_BASE_RE}$") -eq 0 ]] || exit 70
  # THE FIXTURE IS PROVED DISCRIMINATING: the UNESCAPED base, used as an ERE, treats its
  # dots as wildcards and therefore DOES match the impostor. This assertion fails the
  # moment the fixture stops differing in the dot position, which is what would make it
  # inert -- it is the check that stops the check above from being unfailable.
  [[ $(count_re "$d/baseneg.txt" "^${TEST_BASE}$") -eq 1 ]] || exit 70
  # the far impostor is RETAINED with its limit stated: NEITHER pattern matches it, so it
  # proves gross mismatch only and cannot detect a dropped escape.
  [[ $(count_re "$d/basefar.txt" "^${TEST_BASE_RE}$") -eq 0 ]] || exit 70
  [[ $(count_re "$d/basefar.txt" "^${TEST_BASE}$")    -eq 0 ]] || exit 70
  # the numeric boundary
  [[ $(count_re "$d/bound.txt" "${TEST_BASE_RE}:${A_LINE}"'([^0-9]|$)') -eq 0 ]] || exit 70
  [[ $(count_re "$d/bound.txt" "${TEST_BASE_RE}:2080"'([^0-9]|$)') -eq 1 ]] || exit 70
  w 74 "$d/regex.txt" 'REGEX-CONTROL dots-escaped impostor-c1_06_07_privacy_testXexs-refused-by-escaped-and-ACCEPTED-by-unescaped far-impostor-retained-as-gross-mismatch-only 208-does-not-match-2080 2080-matches-itself'
}
# FAILS WHEN the escape is dropped (the discriminating impostor then matches the escaped
# pattern), when the fixture stops discriminating (the unescaped pattern stops matching
# it), or when the numeric boundary is dropped. The 2080 pair is what makes the boundary
# check able to fail in BOTH directions: 208 must not match 2080, and 2080 must match.

# --- stage runner ----------------------------------------------------------------------
STAGE_RC=-1
run_stage() { # run_stage <label> <bound> <inner bash program>
  local lbl=$1 bound=$2 inner=$3 rc=0 erc=0 back here
  # r2 AMENDMENT 2. ENTER AND VERIFY THE PHYSICAL PRODUCT CWD BEFORE NIX IS INVOKED.
  # flake.nix :55-62 takes projectRoot from "git rev-parse --show-toplevel 2>/dev/null
  # || pwd" of the CURRENT cwd, NOT from the flake argument, and then exports
  # MIX_BUILD_ROOT/MIX_DEPS_PATH under it. The step starts in the workspace that holds
  # control/ and product/ as SIBLINGS and is not itself a checkout, so without this the
  # shellHook would resolve the workspace and export workspace-level roots; only the
  # console inners re-export their own, so INNER_ROOT_DEPS would have used them.
  # THIS IS A CWD CORRECTION, NOT A CLAIM THAT THE STAGES WOULD OTHERWISE FAIL.
  cd "$PRODUCT_P" || exit 90
  here=$(pwd -P) || exit 90
  w 74 "$OUT/$lbl.cwd" "PHYSICAL-CWD $here WANT $PRODUCT_P"
  [[ $here == "$PRODUCT_P" ]] || exit 90
  w 74 "$OUT/$lbl.command" "cd $PRODUCT_P && timeout --kill-after=$KILL_AFTER $bound $NIXBIN develop $PRODUCT --command /bin/bash --noprofile --norc -c <inner>"
  w 74 "$OUT/$lbl.inner" "$inner"
  w 74 "$OUT/$lbl.started" "$(date -u)"
  "$TIMEOUT" --kill-after="$KILL_AFTER" "$bound" \
    "$NIXBIN" develop "$PRODUCT" --command /bin/bash --noprofile --norc -c "$inner" \
    > "$OUT/$lbl.stdout" 2> "$OUT/$lbl.stderr" < /dev/null || rc=$?
  printf '%s\n' "$rc" > "$OUT/$lbl.exit" || erc=$?
  [[ $erc -eq 0 ]] || exit 76
  back=$(< "$OUT/$lbl.exit")
  [[ -n $back ]] || exit 77
  [[ $back == "$rc" ]] || exit 77
  STAGE_RC=$rc
  w 74 "$OUT/$lbl.finished" "$(date -u)"
}
# The cwd entry and its verification happen BEFORE the receipts and therefore before the
# bounded invocation; NOTHING stands between the bounded invocation and the capture of its
# status, exactly as before -- the stage status capture is untouched by this amendment.
# The cwd control FAILS WHEN the cd cannot put the process at the product root: the cd
# itself refuses 90 (a nonexistent or unenterable path), and the pwd -P comparison refuses
# 90 if the physical cwd after the cd is not the physical product root.
# CORRECTION, pre-freeze: an earlier version of this comment claimed the pwd -P comparison
# refuses a path that merely resolves through a symlink. THAT WAS FALSE. Both sides of the
# comparison are produced by pwd -P and both resolve to the same physical product root, so
# a symlinked C107_PRODUCT RESOLVES IDENTICALLY AND PASSES -- which is the intended
# behaviour, not a gap. Rejecting a WRONG BASE is not this control's job at all; that
# belongs to the identity checks (HEAD and tree against the pins, refusal 61, and the
# clean-pre-state check, refusal 62). This control answers one question only: is the
# process physically at the product root before nix develop is entered.
# It is reachable in both directions: a correct job enters and matches (it cannot reach
# nix otherwise), and pointing C107_PRODUCT at a nonexistent or unenterable path takes the
# refusing branch.
# No command follows the one whose status matters. The exit receipt is written from rc
# before any timestamp, is refused under its own RESERVED code 76, and is read back and required to
# equal rc (77). started/finished are written AFTER the capture so a run that produced a
# complete summary and then hung is visible as a wall-clock fact.
# STATED LIMIT, not hidden: two layers sit between this shell and mix -- GNU timeout and
# "nix develop --command". Each propagates its child status, and the inner program ends in
# "exec mix", so the mix process REPLACES the inner shell rather than being waited on by
# it. A failure of either layer therefore surfaces as a status that is neither 0 nor 2,
# and judge_variant refuses every such status (82 or 91). Non-zero alone is never read as
# the expected assertion failure.

judge_baseline() { # judge_baseline <label>
  local lbl=$1
  # STATUS IS CLASSIFIED BEFORE ANYTHING IS READ OUT OF STDOUT.
  if is_kill_code "$STAGE_RC"; then exit 91; fi
  [[ $STAGE_RC -eq 0 ]] || exit 72
  parse_summary "$OUT/$lbl.stdout"
  [[ $P_SEEN -eq 1 ]] || exit 73
  [[ $P_UNREC -ne 3 ]] || exit 93
  [[ $P_UNREC -eq 0 ]] || exit 73
  [[ $P_TOTAL -gt 0 ]] || exit 78
  consistent || exit 93
  [[ $P_FAILED -eq 0 ]] || exit 72
  [[ $P_PASSED -eq $BASE_PASSED ]] || exit 72
  [[ $P_TOTAL -eq $BASE_PASSED ]] || exit 72
  w 74 "$OUT/$lbl.sentinel" "BASELINE RC $STAGE_RC PASSED $P_PASSED TOTAL $P_TOTAL FAILURES $P_FAILED SUMMARY-LINES $P_SEEN FAILED-LINES $P_FSEEN"
}
# The focused module declares ZERO nested ExUnit.run summaries, so exactly one Result line
# is expected. FAILS WHEN the candidate over pristine product is not 6 of 6 green, when
# the stream is unparsable, when a Failed line contradicts an all-passed Result (93), when
# the counts do not agree (93), or when it ran no tests (78). A 5/6 CANNOT pass here: the
# fractional form is recognised and P_FAILED then reads 1.

FIRED_LINE=''
judge_variant() { # judge_variant <label> <expected line> <other variant line>
  local lbl=$1 want=$2 other=$3
  local f=$OUT/$lbl.stdout blk=$OUT/$lbl.block h e n dec s wl ol obs=''
  # A TIMEOUT OR SIGNAL IS NEVER THE EXPECTED ASSERTION FAILURE, EVEN WITH A COMPLETE
  # SUMMARY ALREADY ON STDOUT. The status is classified FIRST and every kill code refuses
  # 91 before a single byte of stdout is judged; only the pinned mix failure status 2 is
  # accepted as a parsed assertion failure.
  if is_kill_code "$STAGE_RC"; then exit 91; fi
  [[ $STAGE_RC -ne 0 ]] || exit 83
  [[ $STAGE_RC -eq $MIX_TEST_FAIL_RC ]] || exit 82
  [[ -f $f ]] || exit 81
  parse_summary "$f"
  [[ $P_SEEN -eq 1 ]] || exit 73
  [[ $P_UNREC -ne 1 ]] || exit 73
  [[ $P_UNREC -ne 2 ]] || exit 86
  [[ $P_UNREC -ne 3 ]] || exit 93
  [[ $P_TOTAL -gt 0 ]] || exit 78
  consistent || exit 93
  [[ $P_TOTAL -eq $VAR_TOTAL ]] || exit 79
  [[ $P_PASSED -eq $VAR_PASSED ]] || exit 79
  [[ $P_FAILED -eq $VAR_FAILED ]] || exit 79

  # ONE NUMBERED BLOCK, BOUND BY LINE RANGE, so the header, the row name, the declaration
  # and the assertion frame cannot be satisfied by four different places in the stream.
  [[ $(count_fixed "$f" '  1) test ') -eq 1 ]] || exit 79
  [[ $(count_fixed "$f" '  2) test ') -eq 0 ]] || exit 79
  h=$(first_line_fixed "$f" '  1) test ')
  [[ -n $h ]] || exit 81
  # The block ends at the first trailer line. "Finished in " is EXPECTED AND UNEXECUTED
  # for this module; "Result: " is a MEASURED form, so the alternation does not depend on
  # the unmeasured one alone.
  e=$(first_line_re "$f" '^(Finished in |Result: )')
  [[ -n $e ]] || exit 81
  [[ $e -gt $((h + 2)) ]] || exit 84
  n=$((e - h))
  head -n "$((e - 1))" "$f" | tail -n "$n" > "$blk" || exit 74
  [[ $(lines_in "$blk") -eq $n ]] || exit 74

  # the header line itself carries BOTH the row name and the module
  [[ $(count_fixed "$blk" "$ROW_NAME") -ge 1 ]] || exit 84
  case $(nth_line "$blk" 1) in
    *"$ROW_NAME"*"($MODULE)"*) : ;;
    *) exit 84 ;;
  esac
  # the DECLARATION line is the line ADJACENT to the header, not any line in the stream
  dec=$(nth_line "$blk" 2)
  case $dec in
    *"$TEST_BASE:$ROW_DECL") : ;;
    *"$TEST_BASE:$ROW_DECL "*) : ;;
    *) exit 84 ;;
  esac
  # the ASSERTION frame must sit AFTER the stacktrace marker, inside this block
  s=$(first_line_fixed "$blk" 'stacktrace:')
  [[ -n $s ]] || exit 84
  wl=$(first_line_re "$blk" "${TEST_BASE_RE}:${want}"'([^0-9]|$)')
  [[ -n $wl ]] || exit 84
  [[ $wl -gt $s ]] || exit 84
  [[ $(count_re "$blk" "${TEST_BASE_RE}:${want}"'([^0-9]|$)') -eq 1 ]] || exit 84
  ol=$(count_re "$blk" "${TEST_BASE_RE}:${other}"'([^0-9]|$)')
  [[ $ol -eq 0 ]] || exit 84
  [[ $(count_re "$f" "${TEST_BASE_RE}:${other}"'([^0-9]|$)') -eq 0 ]] || exit 84

  # THE FIRED LINE IS READ BACK OUT OF THE STREAM, from inside the bound block, rather
  # than restated from the expectation this function was handed. Comparing the two
  # EXPECTED literals instead could never fail.
  obs=$(grep -m 1 -o -E "${TEST_BASE_RE}:(${want}|${other})"'([^0-9]|$)' "$blk") || obs=''
  [[ -n $obs ]] || exit 81
  obs=${obs%%[!0-9]}
  FIRED_LINE=${obs##*:}
  [[ -n $FIRED_LINE ]] || exit 81
  w 74 "$OUT/$lbl.sentinel" "VARIANT $C107_VARIANT RC $STAGE_RC PASSED $P_PASSED TOTAL $P_TOTAL FAILURES $P_FAILED BLOCK $h-$((e - 1)) DECL $ROW_DECL MODULE $MODULE WANT $want AT-BLOCK-LINE $wl AFTER-STACKTRACE $s OTHER $other SEEN $ol OBSERVED $FIRED_LINE"
}
# FAILS WHEN the variant passes (83); the status is a timeout, kill or setup code (91);
# the status is any other non-zero, which is what a compile error, a mix refusal and
# 126/127 produce (82); the fractional form lacks its measured companion line (86); a
# Failed line contradicts an all-passed Result or the counts disagree (93); the run
# executed nothing (78); more or fewer than one row moved (79); or the single block is
# not the C1-07c row, its declaration is not adjacent to its header, its assertion frame
# is missing, sits before the stacktrace marker, appears more than once, or the OTHER
# variant's line appears anywhere in the block or in the stream (84).
# NON-ZERO ALONE IS NOT THE PREDICATE, AND NEITHER IS A CORRECT SUMMARY.
# NOT CHECKED HERE, DELIBERATELY: the ExUnit message text and the mailbox listing.
# Variant B fails because NO MESSAGE MATCHING :refresh is present, NOT because the
# mailbox is empty; nothing here reads or asserts mailbox emptiness.
# REGEX DIALECT, STATED: the numeric boundary is "([^0-9]|$)". If this grep treated the
# $ inside the group as a literal, a frame ending exactly at the number would not match
# and the check would REFUSE (84) rather than accept. The failure direction is
# conservative; the measured frame ends ": (test)", so the [^0-9] branch is the one in use.

# --- verdict controls, through the REAL judges -----------------------------------------
ctl_stream() { # ctl_stream <out> <decl line> <assert line> <row name> <companion:yes|no>
  {
    printf '%s\n' 'Running ExUnit with seed: 0, max_cases: 1'
    printf '%s\n' ''
    printf '  1) test %s (%s)\n' "$4" "$MODULE"
    printf '     test/c1/%s:%s\n' "$TEST_BASE" "$2"
    printf '%s\n' '     ** (ExUnit.AssertionError)'
    printf '%s\n' '     stacktrace:'
    printf '       test/c1/%s:%s: (test)\n' "$TEST_BASE" "$3"
    printf '%s\n' ''
    printf '%s\n' 'Finished in 0.1 seconds (0.00s async, 0.1s sync)'
    printf '%s\n' 'Result: 5/6 passed'
    if [[ $5 == yes ]]; then printf '%s\n' 'Failed: 1 test'; fi
  } > "$1" || exit 70
}

ctl_variant() { STAGE_RC=$1; judge_variant "$2" "$3" "$4"; }
ctl_base()    { STAGE_RC=$1; judge_baseline "$2"; }

expect_code() { # expect_code <wanted code> <cmd...>; the subshell contains the refusal
  local want=$1 crc=0
  shift
  ( "$@" ) || crc=$?
  [[ $crc -eq $want ]] || exit 70
}

verdict_controls() {
  ctl_stream "$OUT/ctlok.stdout"          "$ROW_DECL" "$A_LINE" "$ROW_NAME" yes
  ctl_stream "$OUT/ctlother.stdout"       "$ROW_DECL" "$B_LINE" "$ROW_NAME" yes
  ctl_stream "$OUT/ctlrow.stdout"         999         "$A_LINE" 'C1-01a some other row entirely' yes
  ctl_stream "$OUT/ctlnc.stdout"          "$ROW_DECL" "$A_LINE" "$ROW_NAME" no
  printf 'Result: 6 passed\n'                    > "$OUT/ctlbase.stdout"       || exit 70
  printf 'Result: 6 passed\nFailed: 1 test\n'    > "$OUT/ctlbasecontra.stdout" || exit 70
  printf 'Result: 6/7 passed\nFailed: 0 tests\n' > "$OUT/ctlbasefrac.stdout"   || exit 70

  # THE POSITIVE CONTROLS FIRST: the verdict path must be able to ACCEPT, or every
  # refusal below would be meaningless and the whole verdict would be a check that
  # cannot pass.
  expect_code 0  ctl_variant "$MIX_TEST_FAIL_RC" ctlok "$A_LINE" "$B_LINE"
  expect_code 0  ctl_base    0                   ctlbase
  # a complete and correct fractional report whose process was timed out or killed,
  # exercising the ACTUAL verdict path, not parse_summary
  expect_code 91 ctl_variant 124 ctlok "$A_LINE" "$B_LINE"
  expect_code 91 ctl_variant 137 ctlok "$A_LINE" "$B_LINE"
  expect_code 91 ctl_variant 125 ctlok "$A_LINE" "$B_LINE"
  expect_code 82 ctl_variant 1   ctlok "$A_LINE" "$B_LINE"
  expect_code 83 ctl_variant 0   ctlok "$A_LINE" "$B_LINE"
  # wrong line and wrong row, through the verdict
  expect_code 84 ctl_variant "$MIX_TEST_FAIL_RC" ctlother "$A_LINE" "$B_LINE"
  expect_code 84 ctl_variant "$MIX_TEST_FAIL_RC" ctlrow   "$A_LINE" "$B_LINE"
  expect_code 86 ctl_variant "$MIX_TEST_FAIL_RC" ctlnc    "$A_LINE" "$B_LINE"
  # both contradictions, through the verdict
  expect_code 93 ctl_base 0   ctlbasecontra
  expect_code 93 ctl_base 0   ctlbasefrac
  expect_code 91 ctl_base 124 ctlbase
  STAGE_RC=-1
  FIRED_LINE=''
  # LABEL CORRECTION (scope :60). The r4 label opened "13/13 accept-2 ...", which named
  # ONE control -- the variant accept -- as though it were two, so the token list and the
  # count could not both be read as true. The thirteen tokens below stand one-to-one, in
  # call order, with the thirteen expect_code lines above.
  w 74 "$OUT/controls.txt" 'VERDICT-CONTROLS 13/13 accept-variant accept-baseline variant-timeout-124 variant-kill-137 variant-setup-125 variant-other-nonzero-1 variant-green-0 variant-other-line variant-wrong-row variant-missing-companion baseline-all-passed-with-Failed baseline-6-of-7-with-Failed-0 baseline-timeout-124'
}
# EVERY ONE OF THESE RUNS THE REAL judge_variant OR judge_baseline IN A SUBSHELL, so a
# refusal is OBSERVED as an exit code rather than asserted about. The two accept cases are
# what prove the verdict path is not a check that cannot pass; the eleven refusals are
# what prove it is not a check that cannot fail. Both directions are exercised here, on
# every run, before any product state is touched.

# --- tools ------------------------------------------------------------------------------
# These resolve on the hosted image rather than being pinned to an absolute store path:
# pinning a path this preparation never measured would be a check that cannot pass. What
# IS asserted is that each resolved to an executable; the resolved paths are RECORDED so
# a reviewer sees exactly what ran.
need_tool() { # need_tool <command>; prints the resolved absolute path
  local p
  p=$(command -v "$1") || return 1
  [[ -n $p && -x $p ]] || return 1
  printf '%s' "$p"
}
TIMEOUT=$(need_tool timeout)   || exit 65
NIXBIN=$(need_tool nix)        || exit 65
GITBIN=$(need_tool git)        || exit 65
SHABIN=$(need_tool sha256sum)  || exit 65
[[ -n $TIMEOUT && -n $NIXBIN && -n $GITBIN && -n $SHABIN ]] || exit 65
# FAILS WHEN any of the four is absent from the image or is not executable. Reachable:
# remove the Nix install step and nix does not resolve. The emptiness check is not
# redundant with the || exit: a command-v hit whose file was since removed would print a
# path and still be refused by the -x test inside need_tool.

# --- environment snapshot, REPORTED AS A SNAPSHOT ---------------------------------------
{
  date -u
  printf 'variant=%s witness=%s other=%s\n' "$C107_VARIANT" "$C107_WITNESS_LINE" "$C107_OTHER_LINE"
  printf 'timeout=%s nix=%s git=%s sha256sum=%s\n' "$TIMEOUT" "$NIXBIN" "$GITBIN" "$SHABIN"
  printf 'control=%s product=%s out=%s\n' "$CONTROL" "$PRODUCT" "$OUT"
  printf 'C1_BROWSER is deliberately unset; a missing browser must REFUSE qualification,\n'
  printf 'never trigger an unreviewed browser acquisition (scope :19-21).\n'
  id
  uname -a
  cat /etc/os-release
} > "$OUT/environment.txt" 2>&1 || exit 71
# This is a SNAPSHOT of the machine this job happened to get. It is not evidence about
# any other machine, and it is not a measurement of VM isolation or descendant stop.

unset C1_BROWSER || true

# --- controls first, before any product state is touched --------------------------------
selftest "$OUT"
ctl_regex "$OUT"
verdict_controls

# --- frozen controller payloads ----------------------------------------------------------
CAND_SRC=$CONTROL/ci/c107-mutations/candidate.exs
VAR_SRC=$CONTROL/ci/c107-mutations/$C107_VARIANT_FILE
assert_hash 60 control-candidate "$CAND_SRC" "$CAND_SHA"
assert_hash 60 control-variant-a "$CONTROL/ci/c107-mutations/variant-a.ex" "$VA_SHA"
assert_hash 60 control-variant-b "$CONTROL/ci/c107-mutations/variant-b.ex" "$VB_SHA"
[[ $(lines_in "$CAND_SRC") -eq $CAND_LINES ]] || exit 60
# the variant this job was handed must be one of the two frozen ones, and the workflow
# must have named its hash correctly
case $C107_VARIANT_SHA in
  "$VA_SHA"|"$VB_SHA") : ;;
  *) exit 60 ;;
esac
assert_hash 60 control-selected-variant "$VAR_SRC" "$C107_VARIANT_SHA"
# the witness pair must be the pinned pair, in one of its two orders
case "$C107_WITNESS_LINE:$C107_OTHER_LINE" in
  "$A_LINE:$B_LINE"|"$B_LINE:$A_LINE") : ;;
  *) exit 60 ;;
esac
# FAILS WHEN a frozen payload was edited, truncated or swapped, when the workflow names a
# hash that is neither frozen variant, when the named hash does not match the file it
# selected, or when the witness pair is not the pinned 208/232 pair. Reachable in both
# directions: a correct job passes all five (it cannot proceed otherwise), and changing
# one byte of any payload, or one digit of any literal, refuses 60.

# --- the controller checkout identity (r2 AMENDMENT 4) ------------------------------------
# Scope :17 requires BOTH identities in the evidence. The workflow already binds the
# controller checkout and the artifact name to github.sha, so provenance is not wholly
# absent on the Actions side -- but the extracted evidence tree alone lost it. This
# records the ACTUAL controller HEAD, as observed by git in the controller checkout,
# against the workflow SHA it was asked for, in a file SEPARATE from product-identity.txt
# and from every overlay hash. It is an identity record, NOT a hash of this file or of
# the tree containing the evidence: there is no self-hash cycle here and none is possible,
# because nothing under $OUT is an input to anything recorded here.
CTL_HEAD=$("$GITBIN" -C "$CONTROL" rev-parse HEAD) || exit 64
CTL_TREE=$("$GITBIN" -C "$CONTROL" rev-parse 'HEAD^{tree}') || exit 64
w 74 "$OUT/controller-identity.txt" "CONTROLLER-HEAD $CTL_HEAD CONTROLLER-TREE $CTL_TREE WORKFLOW-SHA $C107_WORKFLOW_SHA CONTROL-PATH $CONTROL"
[[ $CTL_HEAD == "$C107_WORKFLOW_SHA" ]] || exit 64
# The OBSERVED head is written BEFORE the comparison, so a mismatch leaves the actual
# value in evidence rather than only code 64. FAILS WHEN the controller checkout is not
# at the commit the workflow named, or is not a git checkout at all (rev-parse refuses).
# Reachable in both directions: a correct push run has checkout ref github.sha and passes
# it, and changing the controller checkout ref -- or a run whose event SHA is not the
# commit checked out -- refuses 64. It cannot pass vacuously: C107_WORKFLOW_SHA is
# required non-empty at the input boundary and CTL_HEAD comes from git, not from the
# workflow. STATED LIMIT: this proves the controller checkout matches the SHA the event
# reported; it is not independent proof about the remote repository state.

# --- the pinned product checkout ---------------------------------------------------------
PROD_HEAD=$("$GITBIN" -C "$PRODUCT" rev-parse HEAD) || exit 61
PROD_TREE=$("$GITBIN" -C "$PRODUCT" rev-parse 'HEAD^{tree}') || exit 61
w 74 "$OUT/product-identity.txt" "HEAD $PROD_HEAD TREE $PROD_TREE WANT-HEAD $BASE_COMMIT WANT-TREE $BASE_TREE"
[[ $PROD_HEAD == "$BASE_COMMIT" ]] || exit 61
[[ $PROD_TREE == "$BASE_TREE" ]] || exit 61
# THIS is how the pinned product checkout is proven. Both the commit and the tree it
# names are compared to literals, so a checkout that resolved a moved ref, a different
# commit with the same message, or a commit whose tree was rewritten all refuse 61.
# FAILS WHEN either differs. Reachable: change the workflow ref by one character.

PRE_FULL=$("$GITBIN" -C "$PRODUCT" status --porcelain) || exit 62
w 74 "$OUT/product-prestate.txt" "$PRE_FULL"
[[ -z $PRE_FULL ]] || exit 62
assert_hash 62 product-candidate-committed "$PRODUCT/$CAND_REL" "$CAND_COMMITTED_SHA"
assert_hash 62 product-target-pristine     "$PRODUCT/$TGT_REL"  "$PRISTINE_SHA"
# The pre-state check runs BEFORE any build, so an empty porcelain INCLUDING untracked
# files is the correct expectation and is reachable. FAILS WHEN the checkout is dirty, or
# when the committed candidate or the committed target is not what the base commit holds.

# --- changed-path set helper --------------------------------------------------------------
changed_set() { # changed_set <code> <label> <expected EXACT non-ignored set>
  local code=$1 label=$2 want=$3 got tracked
  # r2 AMENDMENT 3. THE ASSERTED SET IS THE EXACT NON-IGNORED SET, UNTRACKED INCLUDED.
  # --untracked-files=all lists every untracked file individually, so a collapsed
  # directory line cannot hide its contents; ignored paths stay excluded because
  # --ignored is NOT passed. The tracked-only view is still recorded beside it, as a
  # narrower cross-check, but it is no longer what the transition is judged on.
  tracked=$("$GITBIN" -C "$PRODUCT" status --porcelain --untracked-files=no) || exit "$code"
  w 74 "$OUT/changed.tracked-only.$label.txt" "$tracked"
  got=$("$GITBIN" -C "$PRODUCT" status --porcelain --untracked-files=all) || exit "$code"
  w 74 "$OUT/changed.$label.txt" "$got"
  [[ $got == "$want" ]] || exit "$code"
}
# WHAT IS ASSERTED, stated rather than blurred: the ENTIRE non-ignored working-tree delta
# at each transition, which scope :42 requires to be the exact changed-path set. An
# UNEXPECTED NON-IGNORED ARTIFACT REFUSES; it gets no blanket exemption. The earlier
# rationale for excluding untracked paths -- that asserting them would make the check
# unpassable once mix wrote anything -- is WITHDRAWN as unsupported: /_build/ and /deps/
# are excluded by the repository .gitignore, console/.gitignore excludes console/_build/
# and console/deps/, MIX_BUILD_ROOT and MIX_DEPS_PATH are set beneath exactly those, and
# the harness creates its fixtures under Mix.Project.build_path(). That is
# DOCUMENTATION-AND-SOURCE-REASONED here, not measured on a hosted runner: no stage has
# ever run, so the post-baseline non-ignored set is EXPECTED AND UNEXECUTED.
# FAILS WHEN a copy landed on the wrong path, on more paths than intended, or on none,
# AND ALSO when any non-ignored untracked file appears -- a crash dump at the product
# root, an artifact written outside the ignored build roots, or a stray copy.
# Reachable in both directions: the exact one-line and two-line sets below are what a
# correct run produces (the run cannot continue otherwise), and adding a single
# non-ignored untracked file to the product checkout refuses 68. It cannot pass
# vacuously: the expected value is a non-empty literal set, so an empty porcelain --
# a job in which no copy landed at all -- refuses too.

# These are now the EXPECTED WHOLE non-ignored sets, not just the tracked ones: each
# says that the named modifications are present AND that no non-ignored untracked path
# exists anywhere in the product checkout at that transition.
SET_AFTER_CAND=" M console/test/c1/c1_06_07_privacy_test.exs"
SET_AFTER_VAR=" M console/lib/orris_console/run_index_live.ex
 M console/test/c1/c1_06_07_privacy_test.exs"

# --- inner programs ----------------------------------------------------------------------
export C107_PRODUCT_DIR=$PRODUCT
export C107_TAG=$TAG
export C107_TEST_REL=$TEST_REL
C107_CONSOLE_P=$(cd "$PRODUCT/console" && pwd -P) || exit 90
export C107_CONSOLE_P
# r2 AMENDMENT 2. The PHYSICAL product root, resolved once here and required by every
# run_stage call before it invokes nix. Resolved with pwd -P so the comparison in
# run_stage is physical-to-physical and can pass.
PRODUCT_P=$(cd "$PRODUCT" && pwd -P) || exit 90
[[ -n $PRODUCT_P ]] || exit 90

# With r2 amendment 2 in place, the shellHook has already resolved projectRoot to the
# PRODUCT root before this inner program runs, so the MIX_BUILD_ROOT and MIX_DEPS_PATH
# this stage inherits are product/_build/<tag> and product/deps/<tag> -- both under the
# repository .gitignore entries /_build/ and /deps/. Before the amendment they were
# workspace-level. This inner program is unchanged; only where its inherited roots point
# has changed, and that is SOURCE-REASONED from flake.nix :55-62, not measured.
INNER_ROOT_DEPS='cd "$C107_PRODUCT_DIR" || exit 90
exec mix deps.get --check-locked'

INNER_CONSOLE_DEPS='cd "$C107_CONSOLE_P" || exit 90
export MIX_BUILD_ROOT="$C107_CONSOLE_P/_build/$C107_TAG" MIX_DEPS_PATH="$C107_CONSOLE_P/deps/$C107_TAG"
unset MIX_BUILD_PATH
exec mix deps.get --check-locked'

INNER_TEST='cd "$C107_CONSOLE_P" || exit 90
[ "$(pwd -P)" = "$C107_CONSOLE_P" ] || exit 90
export MIX_BUILD_ROOT="$C107_CONSOLE_P/_build/$C107_TAG" MIX_DEPS_PATH="$C107_CONSOLE_P/deps/$C107_TAG"
unset MIX_BUILD_PATH
exec mix test "$C107_TEST_REL"'
# The MIX_BUILD_ROOT/MIX_DEPS_PATH pair and the literal tag mirror console/bin/verify:8-9
# of the base commit; nothing new is invented about the environment. The cwd assertion
# compares two PHYSICAL paths, both produced by pwd -P, so it can pass.

setup_stage() { # setup_stage <label> <inner>
  run_stage "$1" "$SETUP_BOUND" "$2"
  if is_kill_code "$STAGE_RC"; then exit 91; fi
  [[ $STAGE_RC -eq 0 ]] || exit 85
}
# Dependency acquisition is SETUP, never a witness. A non-zero here refuses 85 and a
# timeout refuses 91; neither is ever read as an assertion outcome.

# --- stage 1: locked dependencies -----------------------------------------------------
setup_stage 01-root-deps "$INNER_ROOT_DEPS"
setup_stage 02-console-deps "$INNER_CONSOLE_DEPS"

# --- transition 1: the candidate overlay, before the baseline --------------------------
cp -p "$CAND_SRC" "$PRODUCT/$CAND_REL" || exit 66
assert_hash 66 overlay-candidate "$PRODUCT/$CAND_REL" "$CAND_SHA"
assert_hash 66 overlay-target-still-pristine "$PRODUCT/$TGT_REL" "$PRISTINE_SHA"
changed_set 68 after-candidate "$SET_AFTER_CAND"
w 74 "$OUT/transition-1.txt" "TRANSITION 1 AT $(date -u) BASE $BASE_COMMIT TREE $BASE_TREE CANDIDATE $CAND_SHA TARGET $PRISTINE_SHA SET $SET_AFTER_CAND"
# ONLY the candidate is copied here (scope :40-41). The target is asserted to be STILL
# pristine, so a job that overlaid a variant early cannot reach the baseline.

# --- stage 2: the baseline ---------------------------------------------------------------
run_stage 03-baseline "$STAGE_BOUND" "$INNER_TEST"
judge_baseline 03-baseline

# --- transition 2: exactly one variant, after its own baseline ---------------------------
cp -p "$VAR_SRC" "$PRODUCT/$TGT_REL" || exit 67
assert_hash 67 swap-target "$PRODUCT/$TGT_REL" "$C107_VARIANT_SHA"
assert_hash 67 swap-candidate-unchanged "$PRODUCT/$CAND_REL" "$CAND_SHA"
changed_set 68 after-variant "$SET_AFTER_VAR"
w 74 "$OUT/transition-2.txt" "TRANSITION 2 AT $(date -u) BASE $BASE_COMMIT TREE $BASE_TREE CANDIDATE $CAND_SHA TARGET $C107_VARIANT_SHA VARIANT $C107_VARIANT SET $SET_AFTER_VAR"
# EXACTLY ONE variant, and only after this job's own baseline was judged. The other
# variant file is never copied into the product tree by this job.

# --- stage 3: the variant -----------------------------------------------------------------
run_stage 04-variant "$STAGE_BOUND" "$INNER_TEST"
judge_variant 04-variant "$C107_WITNESS_LINE" "$C107_OTHER_LINE"

# --- verdict -------------------------------------------------------------------------------
[[ $FIRED_LINE == "$C107_WITNESS_LINE" ]] || exit 84
w 74 "$OUT/VERDICT.txt" "C107 HOSTED MUTATION VARIANT $C107_VARIANT AT $(date -u) BASE $BASE_COMMIT TREE $BASE_TREE CANDIDATE $CAND_SHA VARIANT-SHA $C107_VARIANT_SHA BASELINE 6/6 RC 0 VARIANT 5/6 RC $MIX_TEST_FAIL_RC OBSERVED-WITNESS $FIRED_LINE OTHER-LINE-ABSENT $C107_OTHER_LINE"
w 74 "$OUT/SUMMARY.txt" "PASS variant=$C107_VARIANT witness=$FIRED_LINE base=$BASE_COMMIT tree=$BASE_TREE candidate=$CAND_SHA variant-sha=$C107_VARIANT_SHA"
# FIRED_LINE was READ OUT of the stream, so this last comparison has a real input on both
# sides. It FAILS WHEN the block reported the other variant's line; it cannot be made
# unfailable by construction because the value is observed, not restated.
# WHAT THIS VERDICT DOES NOT SAY: nothing about the OTHER variant (a different job),
# nothing about the local macOS acceptance, nothing about descendant stop or VM cleanup,
# and nothing about tree 222d2f6f carrying the experiment -- it never does.
exit 0
