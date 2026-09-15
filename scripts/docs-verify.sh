#!/usr/bin/env bash
# docs-verify (ADR-016 D3) - mechanical honesty sweep over README.md + docs/*.
# LOCAL GUARD: validates make targets, links, repo paths, credential hygiene, and
# that every headline number in the public docs anchors to the locally-measured
# records (EVIDENCE/metrics.md + the chaos transcripts on this machine - kept
# local, not published). Pure bash+grep: no SQL, no HTTP, no docker. Aborts
# non-zero on the FIRST miss: a vacuous PASS is worse than a FAIL.
set -uo pipefail
cd "$(dirname "$0")/.." || { echo "[docs-verify] FAIL: cannot cd to repo root"; exit 3; }

fail() { echo "[docs-verify] FAIL: $*"; exit 1; }
ok()   { echo "[docs-verify] ok: $*"; }

DOC_FILES=(README.md docs/RUNBOOK.md docs/DATA_DICTIONARY.md docs/DESIGN_NOTES.md docs/index.html)

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
[ "${#SEEN_T[@]}" -ge 15 ] || fail "target sweep found only ${#SEEN_T[@]} make refs - sweep is vacuous"
for t in "${!SEEN_T[@]}"; do
  if [[ "$t" == *'*' ]]; then
    grep -qE "^${t%\*}" Makefile || fail "docs reference make target glob '${t}' - no Makefile target matches"
  else
    grep -qE "^${t}:( |$)" Makefile || fail "docs reference make target '${t}' - not in Makefile"
  fi
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
[ "$LINKS" -ge 25 ] || fail "link sweep found only $LINKS markdown links - vacuous"
ok "all $LINKS markdown links resolve on disk"

# ---- check 3: backticked repo paths exist (known roots; skip globs/schematics)
PATHS=0
for f in "${DOC_FILES[@]}"; do
  while IFS= read -r tok; do
    case "$tok" in *'*'*|*'<'*|*'>'*|*..*|*' '*) continue ;; esac
    case "$tok" in
      EVIDENCE/*|DECISIONS/*|docs/*|scripts/*|observability/*|dags/*|dbt/*|dq/*|ingest/*|infra/*|soap-service/*|rest-mock/*|file-drop/*|oltp/*|cdc-sink/*|airflow/*|screenshots/*)
        [ -e "$tok" ] || fail "docs reference missing path: '$tok' (in $f)"
        PATHS=$((PATHS + 1)) ;;
    esac
  done < <(grep -ohE '`[^`]+`' "$f" | sed -E 's/^`//; s/`$//')
done
[ -f docker-compose.yml ] && [ -f Makefile ] && [ -f .env.example ] || fail "core repo files missing"
[ "$PATHS" -ge 15 ] || fail "path sweep found only $PATHS rooted paths - vacuous"
ok "all $PATHS backticked repo paths exist on disk"

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
      fail "credential-shaped literal in $f (pattern ${pat:0:28}...): $hit"
    fi
    [ "$rc" -le 1 ] || fail "credential grep errored (rc=$rc) for pattern ${pat:0:28}... on $f - vacuous check refused"
    CHECKED=$((CHECKED + 1))
  done
done
git ls-files --cached --error-unmatch .env >/dev/null 2>&1 \
  && fail ".env is TRACKED by git - secrets contract violated"
ok "no credential-shaped literals ($CHECKED file x pattern greps); .env not tracked"

# ---- check 5: headline numbers in the public docs anchor to the local
# measured records (provenance guard - the records stay on this machine)
[ -f EVIDENCE/metrics.md ] || fail "local measured record EVIDENCE/metrics.md missing - cannot anchor doc numbers"
check_pair() { # <doc file> <token in doc> <anchor in the local records>
  grep -qF "$2" "$1" || fail "'$2' missing from $1"
  grep -qF "$3" EVIDENCE/metrics.md || fail "token '$2' ($1) anchor '$3' NOT found in EVIDENCE/metrics.md"
}
check_pair README.md "14,095,849-14,136,532" "14,095,849"
check_pair README.md "65-82 s"      "65.109"
check_pair README.md "65-82 s"      "82.030"
check_pair README.md "172k-217k"    "dbt total 172k"
check_pair README.md "382,179"      "382,179"
check_pair README.md "2,637-2,773"  "2,637.1"
check_pair README.md "2,637-2,773"  "2,773.2"
check_pair README.md "7.4-7.7"      "CDC 7.4"
check_pair README.md "608k-766k"    "607,653.3"
check_pair README.md "608k-766k"    "766,376.8"
check_pair README.md "150.7-163.4"  "150.661"
check_pair README.md "150.7-163.4"  "163.407"
# chaos numbers anchor to the local chaos record (not metrics.md):
grep -qF "69-80 s" README.md            || fail "'69-80 s' missing from README.md"
grep -qF "69-80 s" docs/DESIGN_NOTES.md || fail "'69-80 s' missing from docs/DESIGN_NOTES.md"
grep -qF "3,204 s" docs/DESIGN_NOTES.md || fail "'3,204 s' missing from docs/DESIGN_NOTES.md"
grep -qF "at **69–80 s**" EVIDENCE/phase-5-chaos.md || fail "anchor '69–80 s' band not in the local chaos record"
grep -qF "3204 s" EVIDENCE/phase-5-chaos.md || fail "anchor '3204 s' not in the local chaos record"
ok "every headline number in the public docs anchors to the local measured records"

echo "[docs-verify] PASS - targets ${#SEEN_T[@]} - links $LINKS - paths $PATHS - credential greps $CHECKED - number anchors 15"
exit 0
