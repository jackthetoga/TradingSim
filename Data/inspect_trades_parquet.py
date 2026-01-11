"""
inspect_trades_parquet.py

Quick sanity-check tool for Databento `trades` parquet files.

Examples:
  /Users/zizizink/miniforge3/envs/TradingProject/bin/python Data/inspect_trades_parquet.py \
    --day 2026-01-06 --symbol ALMS --start-time 07:35:00 --end-time 07:38:00 \
    --data-dir /Users/zizizink/Documents/TradingProject/databento_out
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.compute as pc
import pyarrow.parquet as pq


@dataclass(frozen=True)
class Window:
    start_utc: datetime
    end_utc: datetime


def _parse_window(day: str, start_hms: str, end_hms: str, tz_name: str) -> Window:
    tz = ZoneInfo(tz_name)
    utc = ZoneInfo("UTC")
    start = datetime.fromisoformat(f"{day} {start_hms}").replace(tzinfo=tz).astimezone(utc)
    end = datetime.fromisoformat(f"{day} {end_hms}").replace(tzinfo=tz).astimezone(utc)
    if end <= start:
        raise ValueError(f"end-time must be > start-time (got {start_hms}..{end_hms} in {tz_name})")
    return Window(start_utc=start, end_utc=end)


def _inspect(trades_parquet: Path, *, symbol: str, window: Window) -> None:
    pf = pq.ParquetFile(trades_parquet)
    cols = [c for c in ["ts_event", "symbol", "price", "size"] if c in pf.schema.names]
    missing = [c for c in ["ts_event", "symbol"] if c not in cols]
    if missing:
        raise ValueError(f"{trades_parquet.name} missing required columns: {missing}. Has: {pf.schema.names}")

    t = pf.read(columns=cols)
    if t.num_rows == 0:
        print(f"{trades_parquet.name}: empty parquet")
        return

    m_sym = pc.equal(t["symbol"], symbol)
    tt = t.filter(m_sym)
    print(f"file: {trades_parquet}")
    print(f"rows_total: {t.num_rows}")
    print(f"rows_symbol({symbol}): {tt.num_rows}")
    if tt.num_rows == 0:
        return

    # time bounds (per symbol)
    ts_min = pc.min(tt["ts_event"]).as_py()
    ts_max = pc.max(tt["ts_event"]).as_py()
    print(f"symbol_ts_event_min_utc: {ts_min}")
    print(f"symbol_ts_event_max_utc: {ts_max}")

    # window filter
    m_w = pc.and_(pc.greater_equal(tt["ts_event"], window.start_utc), pc.less(tt["ts_event"], window.end_utc))
    w = tt.filter(m_w)
    print(f"window_start_utc: {window.start_utc}")
    print(f"window_end_utc:   {window.end_utc}")
    print(f"rows_in_window: {w.num_rows}")
    if w.num_rows == 0:
        return

    if "price" in w.column_names:
        print(f"price_min: {pc.min(w['price']).as_py()}")
        print(f"price_max: {pc.max(w['price']).as_py()}")
    if "size" in w.column_names:
        print(f"size_sum: {pc.sum(w['size']).as_py()}")
        print(f"size_max: {pc.max(w['size']).as_py()}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Inspect Databento trades parquet for a symbol/time window.")
    ap.add_argument("--day", required=True, help="YYYY-MM-DD")
    ap.add_argument("--symbol", required=True, help="Raw symbol string (must match parquet 'symbol' values)")
    ap.add_argument("--start-time", required=True, help="HH:MM:SS in --tz (e.g. 07:35:00)")
    ap.add_argument("--end-time", required=True, help="HH:MM:SS in --tz (e.g. 07:38:00)")
    ap.add_argument("--tz", default="America/New_York", help="Timezone for start/end times (default: America/New_York)")
    ap.add_argument(
        "--data-dir",
        default="/Users/zizizink/Documents/TradingProject/databento_out",
        help="Directory containing EQUS.MINI.<day>.trades.parquet",
    )
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    trades_p = data_dir / f"EQUS.MINI.{args.day}.trades.parquet"
    if not trades_p.exists():
        raise SystemExit(f"Missing trades parquet: {trades_p}")

    window = _parse_window(args.day, args.start_time, args.end_time, args.tz)
    _inspect(trades_p, symbol=str(args.symbol), window=window)


if __name__ == "__main__":
    main()

