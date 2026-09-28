#!/usr/bin/env bash
# endpoint-sweep.sh — automated endpoint-matrix health/latency sweep for amp-analyzer.
#
# Enumerates every live REST endpoint x its parameter combinations (the matrix is derived
# from the controller source by scripts/gen-endpoint-matrix.py — the source of truth, since
# docs/api-render-map.json is known stale), hits each combination against a running app, and
# writes a status + latency report: a slowest-first table on stdout, a timestamped CSV under
# $HOME for run-over-run comparison, and an SLO summary (counts by status, p50/p95/max, the
# >200ms list).
#
# Usage:
#   endpoint-sweep.sh [cookie-file] [options]
#
#   cookie-file            session cookie (default: ~/staging-cookie.txt; may also be
#                          passed via $AMP_COOKIE)
# Options:
#   --base-url URL         app base URL (default: $AMP_BASE_URL or the staging URL)
#   --matrix FILE          use a pre-built matrix instead of discovering + regenerating
#   --all                  include MUTATING endpoints (POST/PUT/PATCH/DELETE). DEFAULT IS
#                          READ-ONLY: only GET/HEAD are swept, because the matrix contains
#                          real destructive handlers (DELETE /api/parser/analyses/{name},
#                          POST /api/.../cache/clear, .../kill-sessions, ...). Passing --all
#                          against a live app WILL mutate/delete real data — only use it on a
#                          disposable environment.
#   --include-streams      also hit SSE endpoints (they never close; each burns the timeout)
#   --concurrency N        parallel curls (default 4 — fast but gentle)
#   --timeout S            per-request timeout in seconds (default 120)
#   --budget S             overall wall-clock budget in seconds (default 1800)
#   --limit N              only run the first N combinations (smoke test / dry run)
#   --out FILE             CSV output path (default ~/endpoint-sweep-results.csv)
#   --slo-ms N             slow-request threshold for the report (default 200)
#   --dry-run              print what would be swept, make no requests
#
# Exit: 0 completed; 3 discovery/connectivity blocked (auth gate) — matrix still written.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
GEN="$HERE/gen-endpoint-matrix.py"

BASE_URL="${AMP_BASE_URL:-https://amp-analyzer.amp-delivery.staging.corp.mongodb.com}"
COOKIE_FILE="$HOME/staging-cookie.txt"
MATRIX=""
ALL=0
STREAMS=0
CONC=4
REQ_TIMEOUT=120
BUDGET=1800
LIMIT=0
OUT_CSV="$HOME/endpoint-sweep-results.csv"
SLO_MS=200
DRY=0

csv_escape() { printf '%s' "$1" | sed 's/"/""/g; s/^/"/; s/$/"/'; }

# first positional arg may be a cookie file
if [ $# -gt 0 ] && [ "${1#--}" = "$1" ]; then
  COOKIE_FILE="$1"; shift
fi

while [ $# -gt 0 ]; do
  case "$1" in
    --base-url) BASE_URL="$2"; shift 2;;
    --matrix) MATRIX="$2"; shift 2;;
    --all) ALL=1; shift;;
    --include-streams) STREAMS=1; shift;;
    --concurrency) CONC="$2"; shift 2;;
    --timeout) REQ_TIMEOUT="$2"; shift 2;;
    --budget) BUDGET="$2"; shift 2;;
    --limit) LIMIT="$2"; shift 2;;
    --out) OUT_CSV="$2"; shift 2;;
    --slo-ms) SLO_MS="$2"; shift 2;;
    --dry-run) DRY=1; shift;;
    -h|--help) grep -E '^#   ' "$0" | sed 's/^#   //'; exit 0;;
    *) echo "unknown option: $1" >&2; exit 2;;
  esac
done

COOKIE=""
if [ -f "$COOKIE_FILE" ]; then
  COOKIE="$(cat "$COOKIE_FILE")"
elif [ -n "${AMP_COOKIE:-}" ]; then
  COOKIE="$AMP_COOKIE"
fi

command -v curl >/dev/null || { echo "curl not found" >&2; exit 2; }
command -v python3 >/dev/null || { echo "python3 not found" >&2; exit 2; }

TS="$(date +%Y%m%d_%H%M%S)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/endpoint-sweep.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

echo "═══ amp-analyzer endpoint sweep ═══"
echo "base-url   : $BASE_URL"
echo "cookie     : $([ -n "$COOKIE" ] && echo "loaded from $COOKIE_FILE" || echo "NONE (requests will likely 401)")"
echo "mutating   : $([ $ALL = 1 ] && echo "INCLUDED (--all)" || echo "excluded (read-only GET/HEAD)")"
echo "concurrency: $CONC   timeout: ${REQ_TIMEOUT}s   budget: ${BUDGET}s"
echo

# ---- 1. discovery + matrix ---------------------------------------------------------
BLOCKED=0
if [ -n "$MATRIX" ]; then
  echo "── using pre-built matrix: $MATRIX"
  cp "$MATRIX" "$WORK/matrix.tsv"
else
  echo "── discovery: fetching real ids, then generating the matrix from controller source"
  if [ -z "$COOKIE" ]; then
    echo "   (no cookie; skipping discovery calls — templates stay as {tokens})"
    python3 "$GEN" --repo "$REPO" --no-substitute --out "$WORK/matrix.tsv" 2> "$WORK/gen.err"
    grep -E 'endpoints=' "$WORK/gen.err" >&2 || true
  else
    python3 "$GEN" --repo "$REPO" --discover --base-url "$BASE_URL" \
        --cookie-file "$COOKIE_FILE" --out "$WORK/matrix.tsv" 2> "$WORK/gen.err" || true
    grep -E 'discovery status|endpoints=' "$WORK/gen.err" >&2 || true
    if grep -qE '=(401|403)' "$WORK/gen.err"; then
      BLOCKED=1
    fi
  fi
fi

# ---- 2. select rows ----------------------------------------------------------------
tail -n +2 "$WORK/matrix.tsv" > "$WORK/rows.tsv"

SELECT="$WORK/selected.tsv"
: > "$SELECT"
while IFS='|' read -r method path query variant stream mutating controller; do
  [ -z "${method:-}" ] && continue
  if [ "$STREAMS" != 1 ] && [ "${stream:-0}" = "1" ]; then
    printf 'SKIPSTREAM|%s|%s|%s|0|0|%s\n' "$method" "$path" "$variant" "$controller" >> "$SELECT"
    continue
  fi
  if [ "$ALL" != 1 ] && [ "${mutating:-0}" = "1" ]; then
    printf 'SKIPMUTATE|%s|%s|%s|0|1|%s\n' "$method" "$path" "$variant" "$controller" >> "$SELECT"
    continue
  fi
  printf '%s|%s|%s|%s|%s|%s|%s\n' "$method" "$path" "$query" "$variant" "$stream" "$mutating" "$controller" >> "$SELECT"
done < "$WORK/rows.tsv"

TOTAL_COMBOS="$(wc -l < "$WORK/rows.tsv" | tr -d ' ')"
RUN_ROWS="$(grep -cvE '^SKIP' "$SELECT" || true)"
SKIP_STREAM="$(grep -c '^SKIPSTREAM' "$SELECT" || true)"
SKIP_MUT="$(grep -c '^SKIPMUTATE' "$SELECT" || true)"
ENDPOINTS="$(awk -F'|' '!seen[$1" "$2]++{n++} END{print n+0}' "$WORK/rows.tsv")"

echo "── matrix: $ENDPOINTS endpoints, $TOTAL_COMBOS combinations"
echo "   to run: $RUN_ROWS   skipped: $SKIP_STREAM streaming, $SKIP_MUT mutating"
if [ "$LIMIT" -gt 0 ] 2>/dev/null; then
  grep -vE '^SKIP' "$SELECT" | head -n "$LIMIT" > "$WORK/to_run.tsv" || true
else
  grep -vE '^SKIP' "$SELECT" > "$WORK/to_run.tsv" || true
fi
echo "   running: $(wc -l < "$WORK/to_run.tsv" | tr -d ' ') combinations"
echo

if [ "$DRY" = 1 ]; then
  echo "(dry run — no requests made)"
  exit 0
fi

# ---- 3. the worker ----------------------------------------------------------------
cat > "$WORK/worker.sh" <<'WORKER'
#!/usr/bin/env bash
# $1 = one '|'-delimited matrix row; emits: status|time|bytes|method|path|variant|controller
row="$1"
IFS='|' read -r method path query variant stream mutating controller <<< "$row"
if printf '%s' "$path" | grep -q '{'; then
  printf 'UNRESOLVED|-|-|%s|%s|%s|%s\n' "$method" "$path" "$variant" "$controller"
  exit 0
fi
if [ -n "$query" ]; then url="$BASE_URL$path?$query"; else url="$BASE_URL$path"; fi
out=$(curl -s -o /dev/null \
  -w '%{http_code}|%{time_total}|%{size_download}' \
  --max-time "$REQ_TIMEOUT" --connect-timeout 15 \
  -H "Cookie: ${COOKIE:-}" -H 'Accept: application/json' \
  -X "$method" "$url" 2>/dev/null) || out="000|$REQ_TIMEOUT|0"
printf '%s|%s|%s|%s|%s\n' "$out" "$method" "$path" "$variant" "$controller"
WORKER
chmod +x "$WORK/worker.sh"
export BASE_URL COOKIE REQ_TIMEOUT

RAW="$WORK/raw.tsv"
: > "$RAW"

# wall-clock watchdog — bound the total runtime (stdout/stderr detached so it can never
# hold a pipe open and hang the caller)
( sleep "$BUDGET"; pkill -f "$WORK/worker.sh" 2>/dev/null ) >/dev/null 2>&1 &
WATCH=$!

echo "── sweeping $(wc -l < "$WORK/to_run.tsv" | tr -d ' ') combinations with $CONC parallel curls ..."
# Portable bounded-concurrency pool (macOS/BSD xargs has no -d): dispatch in batches of
# $CONC and wait for the batch. Each matrix line is passed as a single argument, so tabs
# and spaces inside it survive.
batch=()
while IFS= read -r row; do
  [ -z "$row" ] && continue
  "$WORK/worker.sh" "$row" >> "$RAW" &
  batch+=("$!")
  if [ "${#batch[@]}" -ge "$CONC" ]; then
    wait "${batch[@]}" 2>/dev/null || true
    batch=()
  fi
done < "$WORK/to_run.tsv"
[ "${#batch[@]}" -gt 0 ] && { wait "${batch[@]}" 2>/dev/null || true; }
kill "$WATCH" 2>/dev/null || true
wait "$WATCH" 2>/dev/null || true

# raw columns: status  time  bytes  method  path  variant  controller
RESULT="$WORK/result.tsv"
: > "$RESULT"
while IFS='|' read -r code t b method path variant controller; do
  [ -z "${code:-}" ] && continue
  note=""
  case "$code" in
    UNRESOLVED) note="path template had an unresolved {id} (discovery blocked)"; t="-";;
    000) note="no response / timeout";;
    401|403) note="auth gate";;
    404) note="not found";;
    405) note="method not allowed";;
  esac
  printf '%s|%s|%s|%s|%s|%s|%s|%s\n' "$TS" "$method" "$path" "$variant" "$code" "$t" "$b" "$note" >> "$RESULT"
done < "$RAW"

# ---- 4. CSV output ----------------------------------------------------------------
{
  echo "timestamp,method,path,query,variant,status,time_total,bytes,note"
  while IFS='|' read -r ts method path variant status t b note; do
    q="$(awk -F'|' -v m="$method" -v p="$path" -v v="$variant" \
          '$1==m && $2==p && $4==v {print $3; exit}' "$WORK/rows.tsv")"
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$ts" "$method" "$path" "$(csv_escape "$q")" "$(csv_escape "$variant")" \
      "$status" "$t" "$b" "$(csv_escape "$note")"
  done < "$RESULT"
} > "$OUT_CSV"

# ---- 5. report --------------------------------------------------------------------
{
  echo "═══ endpoint sweep report ($TS) ═══"
  echo "base: $BASE_URL   endpoints: $ENDPOINTS   combinations run: $(wc -l < "$RESULT" | tr -d ' ')"
  echo
  echo "── slowest first ──"
  printf '%-8s %-10s %-52s %-22s %s\n' STATUS TIME PATH VARIANT BYTES
  awk -F'|' '{print $6"|"$2"|"$3"|"$4"|"$7}' "$RESULT" \
    | sort -t'|' -k1,1nr \
    | while IFS='|' read -r t m p v b; do
        printf '%-8s %-10s %-52s %-22s %s\n' "$m" "${t:-—}" "$p" "${v:0:22}" "${b:-—}"
      done
  echo
  echo "── status counts ──"
  awk -F'|' '{c[$5]++} END{for(k in c) printf "  %-12s %d\n", k, c[k]}' "$RESULT" | sort
  echo
  echo "── latency (successful HTTP < 400 only) ──"
  awk -F'|' '$5+0>0 && $5+0<400 && $6+0>0{print $6}' "$RESULT" | sort -n > "$WORK/times.txt"
  N=$(wc -l < "$WORK/times.txt" | tr -d ' ')
  if [ "$N" -gt 0 ]; then
    awk -v n="$N" -v slo="$SLO_MS" '
      {v[NR]=$1}
      END{
        p50=int((n*0.50)+0.999); p95=int((n*0.95)+0.999);
        printf "  n=%d  p50=%.3fs  p95=%.3fs  max=%.3fs  mean=%.3fs\n", n, v[p50], v[p95], v[n], s/n
        slow=0; for(i=1;i<=n;i++) if(v[i]*1000>slo) slow++;
        printf "  over SLO (%dms): %d\n", slo, slow
      }' "$WORK/times.txt"
    echo
    echo "── > ${SLO_MS}ms list (the SLO report) ──"
    awk -F'|' -v slo="$SLO_MS" '$5+0<400 && ($6*1000)>slo{printf "  %6.3fs  %-8s %s  [%s]\n",$6,$2,$3,$4}' "$RESULT" | sort -rn | head -40
  else
    echo "  (no successful responses — see status counts above)"
  fi
  echo
  echo "── failures / notes ──"
  awk -F'|' '$5+0>=400 || $5+0==0 || $5=="UNRESOLVED"{printf "  %-10s %-52s %s\n",$5,$3,$8}' "$RESULT" | sort | uniq -c | sort -rn | head -30
} | tee "$WORK/report.txt"

echo
echo "CSV written: $OUT_CSV"
if [ "$BLOCKED" = 1 ]; then
  echo
  echo "NOTE: discovery returned 401/403 — the corp auth gate is blocking this host."
  echo "      Path-template ids could not be resolved (rows shown as UNRESOLVED); run this"
  echo "      from the owner's signed-in terminal (or supply a fresh cookie) for real-data"
  echo "      coverage. The tool + CSV format are unaffected."
  exit 3
fi
exit 0
