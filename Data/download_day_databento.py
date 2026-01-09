# download_day_databento.py
# pip install databento pandas pyarrow

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence

import databento as db
import os
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


@dataclass(frozen=True)
class SessionWindow:
    # Default to US equities regular session in Eastern Time.
    # (Extended hours vary by venue; if you want 04:00–20:00 ET, pass those explicitly.)
    start_time: str = "09:30:00"
    end_time: str = "16:00:00"
    tz: str = "America/New_York"

    def bounds(self, day: str) -> tuple[datetime, datetime]:
        """
        Returns timezone-aware datetimes (converted to UTC) for the requested session window.

        We avoid a hard pandas dependency so this file can run in minimal environments.
        """
        tz = ZoneInfo(self.tz)
        start_local = datetime.fromisoformat(f"{day}T{self.start_time}").replace(tzinfo=tz)
        end_local = datetime.fromisoformat(f"{day}T{self.end_time}").replace(tzinfo=tz)
        return start_local.astimezone(ZoneInfo("UTC")), end_local.astimezone(ZoneInfo("UTC"))


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def fetch_to_parquet(
    client: db.Historical,
    *,
    dataset: str,
    schema: str,
    symbols: Sequence[str],
    start: datetime,
    end: datetime,
    out_path: Path,
) -> Path:
    """
    Streams Databento data directly to Parquet without building a giant in-memory DataFrame.
    """
    store = client.timeseries.get_range(
        dataset=dataset,
        schema=schema,
        stype_in="raw_symbol",
        symbols=list(symbols),
        start=start,
        end=end,
    )

    # Parquet transcoding supports symbol mapping (instrument_id -> symbol) and price conversion.
    store.to_parquet(
        out_path.as_posix(),
        map_symbols=True,
        # Keep timestamps as proper tz-aware UTC datetimes when possible.
        pretty_ts=True,
        # Use float prices (recommended for analysis/backtests). "fixed" stores 1e-9 units as ints.
        price_type="float",
    )
    return out_path


def build_bars_from_ohlcv_1s(
    ohlcv_1s_parquet: Path,
    out_path: Path,
    *,
    freq: str,
    tz: str = "America/New_York",
    symbols: Sequence[str] | None = None,
) -> Path:
    """
    Build higher-timeframe OHLCV bars from Databento ohlcv-1s.

    This implementation intentionally avoids pandas, since some environments (e.g. NumPy 2.x
    with an older binary pandas) may not be able to import it.

    Rules per bucket (size N seconds):
    - open  = open at bucket start timestamp
    - high  = max(high)
    - low   = min(low)
    - close = close at bucket end timestamp (start + (N-1)s)
    - volume = sum(volume)

    Buckets are **dropped** unless they contain exactly N one-second rows for a given symbol.
    Output parquet has columns: symbol, ts, open, high, low, close, volume
    """
    freq_map = {"10s": 10, "1min": 60, "5min": 300}
    if freq not in freq_map:
        raise ValueError(f"Unsupported freq={freq!r}. Supported: {sorted(freq_map)}")
    bucket_sec = freq_map[freq]
    bucket_ns = bucket_sec * 1_000_000_000
    end_offset_ns = (bucket_sec - 1) * 1_000_000_000

    cols = ["symbol", "ts_event", "open", "high", "low", "close", "volume"]
    table = pq.read_table(ohlcv_1s_parquet, columns=cols)

    # Normalize timestamp column name to ts_event, and ensure tz=UTC.
    if "ts_event" not in table.column_names:
        raise ValueError(f"Expected 'ts_event' column in {ohlcv_1s_parquet.name}. Columns: {table.column_names}")
    ts = table["ts_event"]
    if not pa.types.is_timestamp(ts.type):
        raise ValueError(f"Expected ts_event to be timestamp, got {ts.type}")
    if ts.type.tz != "UTC":
        ts = pc.assume_timezone(ts, "UTC")
        table = table.set_column(table.schema.get_field_index("ts_event"), "ts_event", ts)

    if "symbol" not in table.column_names:
        raise ValueError(f"Expected a 'symbol' column (map_symbols=True). Columns: {table.column_names}")

    # Optional symbol filter
    if symbols is not None:
        sym_set = set(symbols)
        sym_arr = table["symbol"]
        mask = pa.array([s in sym_set for s in sym_arr.to_pylist()])
        table = table.filter(mask)

    # Sort by symbol, ts_event so "first/last" within bucket is deterministic.
    sort_idx = pc.sort_indices(table, sort_keys=[("symbol", "ascending"), ("ts_event", "ascending")])
    table = pc.take(table, sort_idx)

    # Create bucket start as epoch ns integer
    ts_ns = pc.cast(table["ts_event"], pa.int64())
    bucket_start_ns = pc.multiply(pc.floor_divide(ts_ns, bucket_ns), bucket_ns)
    table = table.append_column("_bucket_start_ns", bucket_start_ns)

    # Aggregate high/low/volume + count per (symbol, bucket)
    gb = table.group_by(["symbol", "_bucket_start_ns"])
    agg = gb.aggregate(
        [
            ("high", "max"),
            ("low", "min"),
            ("volume", "sum"),
            ("ts_event", "count"),
        ]
    ).rename_columns(["symbol", "_bucket_start_ns", "high", "low", "volume", "_count"])

    # Drop buckets that don't have the full expected number of 1s rows.
    agg = agg.filter(pc.equal(agg["_count"], bucket_sec))

    # Join back to find open (row where ts == bucket_start) and close (row where ts == bucket_start + end_offset)
    want_open_ns = agg["_bucket_start_ns"]
    want_close_ns = pc.add(agg["_bucket_start_ns"], end_offset_ns)

    # Build a lookup table keyed by (symbol, ts_ns) -> open/close values
    # For 1s bars, symbol+ts is unique; if not, last write wins but should be stable due to sort above.
    base = pa.table(
        {
            "symbol": table["symbol"],
            "_ts_ns": ts_ns,
            "_open": table["open"],
            "_close": table["close"],
        }
    )

    open_keys = pa.table({"symbol": agg["symbol"], "_ts_ns": want_open_ns})
    close_keys = pa.table({"symbol": agg["symbol"], "_ts_ns": want_close_ns})

    open_joined = open_keys.join(base, keys=["symbol", "_ts_ns"], join_type="left outer")
    close_joined = close_keys.join(base, keys=["symbol", "_ts_ns"], join_type="left outer")

    # If a bucket is "full" but missing the exact boundary seconds (rare), drop it.
    has_open = pc.is_valid(open_joined["_open"])
    has_close = pc.is_valid(close_joined["_close"])
    keep = pc.and_(has_open, has_close)

    agg = agg.filter(keep)
    open_joined = open_joined.filter(keep)
    close_joined = close_joined.filter(keep)

    out = pa.table(
        {
            "symbol": agg["symbol"],
            # output ts is bucket start as timestamp[ns, tz=UTC]
            "ts": pc.cast(agg["_bucket_start_ns"], pa.timestamp("ns", tz="UTC")),
            "open": open_joined["_open"],
            "high": agg["high"],
            "low": agg["low"],
            "close": close_joined["_close"],
            "volume": agg["volume"],
        }
    )

    pq.write_table(out, out_path)
    return out_path


def build_10s_bars_from_ohlcv_1s(
    ohlcv_1s_parquet: Path,
    out_path: Path,
    tz: str = "America/New_York",
    symbols: Sequence[str] | None = None,
) -> Path:
    return build_bars_from_ohlcv_1s(ohlcv_1s_parquet, out_path, freq="10s", tz=tz, symbols=symbols)


def build_1m_bars_from_ohlcv_1s(
    ohlcv_1s_parquet: Path,
    out_path: Path,
    tz: str = "America/New_York",
    symbols: Sequence[str] | None = None,
) -> Path:
    return build_bars_from_ohlcv_1s(ohlcv_1s_parquet, out_path, freq="1min", tz=tz, symbols=symbols)


def build_5m_bars_from_ohlcv_1s(
    ohlcv_1s_parquet: Path,
    out_path: Path,
    tz: str = "America/New_York",
    symbols: Sequence[str] | None = None,
) -> Path:
    return build_bars_from_ohlcv_1s(ohlcv_1s_parquet, out_path, freq="5min", tz=tz, symbols=symbols)


def download_day(
    *,
    day: str,                  # "YYYY-MM-DD" (treated as US/Eastern session date)
    symbols: Sequence[str],
    out_dir: str = "./databento_out",
    window: SessionWindow = SessionWindow(),
    download_trades: bool = True,
    download_ohlcv1s: bool = True,
    download_mbp10: bool = True,
) -> None:
    out = Path(out_dir)
    _ensure_dir(out)

    start, end = window.bounds(day)

    client = db.Historical()  # uses DATABENTO_API_KEY env var

    # 1) Near-consolidated tape + bars
    trades_path = out / f"EQUS.MINI.{day}.trades.parquet"
    ohlcv1s_path = out / f"EQUS.MINI.{day}.ohlcv-1s.parquet"

    # 2) L2 depth (Nasdaq book)
    mbp10_path = out / f"XNAS.ITCH.{day}.mbp-10.parquet"

    # 3) Build 10-second bars from 1-second OHLCV
    bars10s_path = out / f"EQUS.MINI.{day}.ohlcv-10s.parquet"
    bars1m_path = out / f"EQUS.MINI.{day}.ohlcv-1m.parquet"
    bars5m_path = out / f"EQUS.MINI.{day}.ohlcv-5m.parquet"
    if download_trades:
        fetch_to_parquet(
            client,
            dataset="EQUS.MINI",
            schema="trades",
            symbols=symbols,
            start=start,
            end=end,
            out_path=trades_path,
        )
    if download_ohlcv1s:
        fetch_to_parquet(
            client,
            dataset="EQUS.MINI",
            schema="ohlcv-1s",
            symbols=symbols,
            start=start,
            end=end,
            out_path=ohlcv1s_path,
        )
        # Build derived bars (local resampling, no extra API usage)
        build_10s_bars_from_ohlcv_1s(ohlcv1s_path, bars10s_path, tz=window.tz)
        build_1m_bars_from_ohlcv_1s(ohlcv1s_path, bars1m_path, tz=window.tz)
        build_5m_bars_from_ohlcv_1s(ohlcv1s_path, bars5m_path, tz=window.tz)
    if download_mbp10:
        fetch_to_parquet(
            client,
            dataset="XNAS.ITCH",
            schema="mbp-10",
            symbols=symbols,
            start=start,
            end=end,
            out_path=mbp10_path,
        )

    print("Wrote:")
    if download_trades:
        print(" ", trades_path)
    if download_ohlcv1s:
        print(" ", ohlcv1s_path)
        print(" ", bars10s_path)
        print(" ", bars1m_path)
        print(" ", bars5m_path)
    if download_mbp10:
        print(" ", mbp10_path)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Download a window of Databento data to Parquet (safe for small tests).")
    p.add_argument("--day", required=True, help="YYYY-MM-DD (session date, interpreted in America/New_York by default)")
    p.add_argument("--symbols", required=True, nargs="+", help="One or more raw symbols (e.g. MNTS)")
    p.add_argument("--out-dir", default="./databento_out")
    p.add_argument("--tz", default="America/New_York")
    p.add_argument("--start-time", default="09:30:00", help="HH:MM:SS in --tz (e.g. 09:30:00)")
    p.add_argument("--end-time", default="09:33:00", help="HH:MM:SS in --tz (e.g. 09:33:00 for a 3-min test)")
    p.add_argument(
        "--build-derived-only",
        action="store_true",
        help="Do NOT download anything. Only build 10s/1m/5m bars from an existing ohlcv-1s parquet in --out-dir.",
    )
    p.add_argument("--skip-trades", action="store_true", help="Skip EQUS.MINI trades download")
    p.add_argument("--skip-ohlcv1s", action="store_true", help="Skip EQUS.MINI ohlcv-1s download (and 10s aggregation)")
    p.add_argument("--skip-mbp10", action="store_true", help="Skip XNAS.ITCH mbp-10 download (saves credits)")
    args = p.parse_args()

    window = SessionWindow(start_time=args.start_time, end_time=args.end_time, tz=args.tz)
    out = Path(args.out_dir)
    if args.build_derived_only:
        # Build from existing files; no API key required and no network calls.
        ohlcv1s_path = out / f"EQUS.MINI.{args.day}.ohlcv-1s.parquet"
        if not ohlcv1s_path.exists():
            raise FileNotFoundError(f"Missing {ohlcv1s_path}. Nothing to build from.")
        build_10s_bars_from_ohlcv_1s(ohlcv1s_path, out / f"EQUS.MINI.{args.day}.ohlcv-10s.parquet", tz=window.tz, symbols=args.symbols)
        build_1m_bars_from_ohlcv_1s(ohlcv1s_path, out / f"EQUS.MINI.{args.day}.ohlcv-1m.parquet", tz=window.tz, symbols=args.symbols)
        build_5m_bars_from_ohlcv_1s(ohlcv1s_path, out / f"EQUS.MINI.{args.day}.ohlcv-5m.parquet", tz=window.tz, symbols=args.symbols)
        print("Built derived bars from existing 1s parquet:")
        print(" ", out / f"EQUS.MINI.{args.day}.ohlcv-10s.parquet")
        print(" ", out / f"EQUS.MINI.{args.day}.ohlcv-1m.parquet")
        print(" ", out / f"EQUS.MINI.{args.day}.ohlcv-5m.parquet")
    else:
        # Prefer an environment variable (don't hardcode keys in source).
        if not os.environ.get("DATABENTO_API_KEY"):
            raise RuntimeError(
                "DATABENTO_API_KEY is not set. "
                "Set it in your shell before running, e.g. `export DATABENTO_API_KEY=...`"
            )
        download_day(
            day=args.day,
            symbols=args.symbols,
            out_dir=args.out_dir,
            window=window,
            download_trades=not args.skip_trades,
            download_ohlcv1s=not args.skip_ohlcv1s,
            download_mbp10=not args.skip_mbp10,
        )
