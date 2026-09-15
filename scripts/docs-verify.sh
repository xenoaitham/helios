#!/usr/bin/env bash
# docs-verify (ADR-016 D3) — the mechanical honesty sweep over README.md + docs/*.md.
# Pure bash+grep over repo files: no SQL, no HTTP, no docker. Aborts non-zero on
# the FIRST miss (the smoke/chaos/bench loud-assert precedent): a vacuous PASS is
# worse than a FAIL, so every check also asserts it examined a non-zero number of
# items. Transcript: EVIDENCE/docs-verify.log (via make docs-verify).
set -uo pipefail
cd "$(dirname "$0")/.." || { echo "[docs-verify] FAIL: cannot cd to repo root"; exit 3; }

fail() { echo "[docs-verify] FAIL: $*"; exit 1; }
ok()   { echo "[docs-verify] ok: $*"; }

DOC_FILES=(README.md docs/RUNBOOK.md docs/DATA_DICTIONARY.md docs/INTERVIEW_DEFENSE.md docs/index.html)

for f in "${DOC_FILES[@]}"; do
  [ -s "$f" ] || fail "doc file missing or empty: $f"
done
ok "all ${#DOC_FILES[@]} doc files present and non-empty"

# ---- check 1: every `make <target>` referenced in the docs exists in the Makefile
declare -A SEEN_T
for f in "${DOC_FILES[@]}"; do
  while IFS= read -r t; do SEEN_T["$t"]=1; done \
    < <(grep -ohE '\bmake [a-z][a-z*-]*' "$f" | awk '{print $2}' \
        | grep -vxE 'the|a|an|it|this|that|these|those|sure|your|their|its|all|any|every|one|no|not|you|we|they|them|us|our|my|here|there|what|which|how|than|then|me|him|her|out|sense|clear|docs|certain|target|targets')
done
[ "${#SEEN_T[@]}" -ge 15 ] || fail "target sweep found only ${#SEEN_T[@]} make refs — sweep is vacuous"
for t in "${!SEEN_T[@]}"; do
  case "$t" in
    *) if [[ "$t" == *'*' ]]; then
         grep -qE "^${t%\*}" Makefile || fail "docs reference make target glob '${t}' — no Makefile target matches"
       else
         grep -qE "^${t}:( |$)" Makefile || fail "docs reference make target '${t}' — not in Makefile"
       fi ;;
  esac
done
ok "all ${#SEEN_T[@]} referenced make targets exist in the Makefile"

# ---- check 2: markdown links resolve (relative to the referencing file)
LINKS=0
for f in "${DOC_FILES[@]}"; do
  dir="$(dirname "$f")"
  while IFS= read -r link; do
    [ -n "$link" ] || continue
    case "$link" in
      '#'*|http://*|https://*|mailto:*) continue ;;
    esac
    target="${link%%#*}"
    [ -n "$target" ] || continue
    case "$target" in /*) fail "absolute link in $f: $link" ;; esac
    resolved="$(realpath -m "$dir/$target")"
    [ -e "$resolved" ] || fail "broken link in $f: ($link) -> $resolved"
    LINKS=$((LINKS + 1))
  done < <(grep -ohE '\]\([^)]+\)' "$f" | sed -E 's/^\]\(//; s/\)$//')
done
[ "$LINKS" -ge 30 ] || fail "link sweep found only $LINKS markdown links — vacuous"
ok "all $LINKS markdown links resolve on disk"

# ---- check 3: backticked repo paths exist (known roots; skip globs/schematics)
ROOTS='(EVIDENCE|DECISIONS|docs|scripts|observability|dags|dbt|dq|ingest|infra|soap-service|rest-mock|file-drop|oltp|cdc-sink|airflow)/'
PATHS=0
for f in "${DOC_FILES[@]}"; do
  while IFS= read -r tok; do
    case "$tok" in *'*'*|*'<'*|*'>'*|*..*|*' '*) continue ;; esac
    case "$tok" in
      EVIDENCE/*|DECISIONS/*|docs/*|scripts/*|observability/*|dags/*|dbt/*|dq/*|ingest/*|infra/*|soap-service/*|rest-mock/*|file-drop/*|oltp/*|cdc-sink/*|airflow/*)
        [ -e "$tok" ] || fail "docs reference missing path: '$tok' (in $f)"
        PATHS=$((PATHS + 1)) ;;
    esac
  done < <(grep -ohE '`[^`]+`' "$f" | sed -E 's/^`//; s/`$//')
done
# named non-root-prefixed files the docs lean on
for tok in docker-compose.yml Makefile STATE.md BACKLOG.md .env.example MASTER_PROMPT.md; do
  grep -l -e "$tok" "${DOC_FILES[@]}" >/dev/null 2>&1 || true
done
[ -f docker-compose.yml ] && [ -f Makefile ] && [ -f .env.example ] || fail "core repo files missing"
[ "$PATHS" -ge 20 ] || fail "path sweep found only $PATHS rooted paths — vacuous"
ok "all $PATHS backticked repo paths exist on disk"

# ---- check 3b: EVIDENCE references specifically (the mandate's named check)
EV=0
for f in "${DOC_FILES[@]}"; do
  dir="$(dirname "$f")"
  while IFS= read -r tok; do
    case "$tok" in *'*'*|*..*) continue ;; esac
    case "$tok" in EVIDENCE/*) [ -e "$tok" ] || fail "missing evidence file: '$tok' (in $f)"; EV=$((EV + 1)) ;; esac
  done < <(grep -ohE '`[^`]+`' "$f" | sed -E 's/^`//; s/`$//')
  while IFS= read -r link; do
    target="${link%%#*}"; [ -n "$target" ] || continue
    case "$target" in *'*'*|*..*) continue ;; esac
    resolved="$(realpath -m "$dir/$target" 2>/dev/null)" || continue
    case "$resolved" in "$PWD"/EVIDENCE/*) [ -e "$resolved" ] || fail "missing evidence file (link): $target in $f"; EV=$((EV + 1)) ;; esac
  done < <(grep -ohE '\]\([^)]+\)' "$f" | sed -E 's/^\]\(//; s/\)$//')
done
[ "$EV" -ge 10 ] || fail "evidence sweep found only $EV EVIDENCE refs — vacuous"
ok "all $EV EVIDENCE/ references resolve to real files"

# ---- check 4: no credential-shaped literals in any doc
CRED_PATS=(
  '(password|passwd|secret|token|api[_-]?key)[A-Za-z0-9_ -]{0,20}[=:]["'"'"']?[A-Za-z0-9/+_-]{8,}'
  '-----BEGIN'
  'AKIA[0-9A-Z]{16}'
  'xox[baprs]-'
  'ghp_[A-Za-z0-9]{20,}'
  'sk-[A-Za-z0-9]{20,}'
  'AIza[0-9A-Za-z_-]{30,}'
  '[a-z]+://[A-Za-z0-9_]+:[A-Za-z0-9/+-]{4,}@'
)
CHECKED=0
for f in "${DOC_FILES[@]}"; do
  for pat in "${CRED_PATS[@]}"; do
    hit="$(grep -inE -e "$pat" -- "$f" | head -3)"
    rc=$?
    if [ -n "$hit" ]; then
      fail "credential-shaped literal in $f (pattern ${pat:0:28}…): $hit"
    fi
    [ "$rc" -le 1 ] || fail "credential grep errored (rc=$rc) for pattern ${pat:0:28}… on $f — vacuous check refused"
    CHECKED=$((CHECKED + 1))
  done
done
git ls-files --cached --error-unmatch .env >/dev/null 2>&1 \
  && fail ".env is TRACKED by git — secrets contract violated"
ok "no credential-shaped literals ($CHECKED file×pattern greps); .env not tracked"

# ---- check 5: resume-bullet numbers anchor to EVIDENCE/metrics.md (ADR-016 D5)
SEC="$(awk '/^## 14\. Resume bullets/{flag=1} flag' docs/INTERVIEW_DEFENSE.md)"
[ -n "$SEC" ] || fail "resume-bullets section (## 14.) not found in INTERVIEW_DEFENSE.md"
check_pair() { # <token that must be in the resume section> <anchor that must be in metrics.md>
  echo "$SEC" | grep -qF "$1" || fail "resume bullets lost the metrics.md token '$1'"
  grep -qF "$2" EVIDENCE/metrics.md || fail "resume token '$1' anchor '$2' NOT found in EVIDENCE/metrics.md"
}
check_pair "382,179"      "382,179"
check_pair "2,637–2,773"  "2,637.1"
check_pair "2,637–2,773"  "2,773.2"
check_pair "7.4–7.7"      "7.4–7.7"
check_pair "14.1M"        "14,095,849"
check_pair "65–82 s"      "65.109"
check_pair "65–82 s"      "82.030"
check_pair "172k–217k"    "172k–217k"
check_pair "~8.5M"        "8,530,237"
check_pair "608k–766k"    "607,653.3"
check_pair "608k–766k"    "766,376.8"
check_pair "150.7–163.4"  "150.661"
check_pair "150.7–163.4"  "163.407"
check_pair "195 pytest"   "pytest 195"
check_pair "144 dbt"      "dbt 144"
check_pair "32 dq"        "dq 32"
# the chaos latency band cited in §8/README anchors to the roll-up AND the transcripts
grep -qF "69–80 s" docs/INTERVIEW_DEFENSE.md || fail "alert-latency band '69–80 s' missing from INTERVIEW_DEFENSE §8"
grep -qF "69–80 s measured" README.md || fail "alert-latency band '69–80 s' missing from README chaos section"
grep -qF "at **69–80 s**" EVIDENCE/phase-5-chaos.md || fail "anchor '69–80 s' band not in phase-5-chaos.md roll-up"
grep -qF "latency 69s" EVIDENCE/chaos-05-poison-csv.log || fail "anchor 69s not in chaos-05 log"
grep -qF "latency 73s" EVIDENCE/chaos-04-poison-cdc.log || fail "anchor 73s not in chaos-04 log"
PCOUNT="$(echo "$SEC" | grep -c 'Personal project')"
[ "$PCOUNT" -ge 3 ] || fail "resume bullets must carry 'Personal project' framing (found $PCOUNT, need ≥3)"
ok "resume bullets: every number anchors to EVIDENCE/metrics.md (or the cited chaos logs); personal-project framing ×$PCOUNT"

echo "[docs-verify] PASS — targets ${#SEEN_T[@]} · links $LINKS · paths $PATHS · evidence $EV · credential greps $CHECKED · resume anchors 16+2"
exit 0
