#!/usr/bin/env bash
set -euo pipefail

# Re-download MBP-10 and OHLCV (1s + derived bars) for every (day,symbol) pair
# discovered by Data/list_downloaded.py.
#
# Requirements:
# - Conda env: TradingProject (we call its Python directly)
# - DATABENTO_API_KEY must be set in your shell
#
# Usage:
#   export DATABENTO_API_KEY="..."
#   bash Data/redownload_mbp10_ohlcv_skip_trades.sh
#
# Optional overrides:
#   PAIRS_CSV=Data/downloaded_pairs.all.csv OUT_DIR=./databento_out START_TIME=07:00:00 END_TIME=10:30:00 bash Data/redownload_mbp10_ohlcv_skip_trades.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PYTHON="${PYTHON:-/Users/zizizink/miniforge3/envs/TradingProject/bin/python}"
DOWNLOAD_PY="${DOWNLOAD_PY:-$ROOT_DIR/Data/download_day_databento.py}"

PAIRS_CSV="${PAIRS_CSV:-$ROOT_DIR/Data/downloaded_pairs.all.csv}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/databento_out}"
TZ="${TZ:-America/New_York}"
START_TIME="${START_TIME:-07:00:00}"
END_TIME="${END_TIME:-10:30:00}"

if [[ ! -x "$PYTHON" ]]; then
  echo "ERROR: PYTHON is not executable: $PYTHON" >&2
  exit 1
fi
if [[ ! -f "$DOWNLOAD_PY" ]]; then
  echo "ERROR: download script not found: $DOWNLOAD_PY" >&2
  exit 1
fi
if [[ ! -f "$PAIRS_CSV" ]]; then
  echo "ERROR: PAIRS_CSV not found: $PAIRS_CSV" >&2
  exit 1
fi
if [[ -z "${DATABENTO_API_KEY:-}" ]]; then
  echo "ERROR: DATABENTO_API_KEY is not set in your environment." >&2
  echo "       Example: export DATABENTO_API_KEY='db-...'" >&2
  exit 1
fi

TMP_GROUPED="$(mktemp)"
trap 'rm -f "$TMP_GROUPED"' EXIT

# Group pairs by day so we make 1 API call per day (more efficient than per symbol).
"$PYTHON" - "$PAIRS_CSV" > "$TMP_GROUPED" <<'PY'
import csv
import sys
from collections import defaultdict

path = sys.argv[1]
by_day: dict[str, set[str]] = defaultdict(set)

with open(path, newline="") as f:
    r = csv.DictReader(f)
    if not r.fieldnames or "day" not in r.fieldnames or "symbol" not in r.fieldnames:
        raise SystemExit(f"Bad CSV header in {path}. Expected columns: day,symbol")
    for row in r:
        day = (row.get("day") or "").strip()
        sym = (row.get("symbol") or "").strip()
        if not day or not sym:
            continue
        by_day[day].add(sym)

for day in sorted(by_day.keys()):
    syms = sorted(by_day[day])
    print(day, *syms)
PY

echo "Pairs CSV: $PAIRS_CSV"
echo "Out dir  : $OUT_DIR"
echo "Window   : $TZ $START_TIME .. $END_TIME"
echo ""

while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  day="${line%% *}"
  syms_str="${line#* }"
  if [[ "$day" == "$syms_str" ]]; then
    echo "WARN: No symbols for day=$day, skipping." >&2
    continue
  fi
  IFS=' ' read -r -a syms <<<"$syms_str"
  echo "=== $day (${#syms[@]} symbols) ==="
  "$PYTHON" "$DOWNLOAD_PY" \
    --day "$day" \
    --symbols "${syms[@]}" \
    --out-dir "$OUT_DIR" \
    --tz "$TZ" \
    --start-time "$START_TIME" \
    --end-time "$END_TIME" \
    --overwrite \
    --skip-trades
  echo ""
done < "$TMP_GROUPED"

echo "Done."

