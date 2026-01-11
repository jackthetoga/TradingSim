# download_day_databento_consolidated.py
#
# This is a copy of `Data/download_day_databento.py`, but it targets Databento's
# consolidated US equities dataset for Trades + OHLCV-1s.
#
# Notes:
# - Databento entitlements vary by account. Some datasets may be "valid" but have no data for you
#   (e.g. reporting an available end around 1970). To avoid guesswork, this script supports
#   AUTO dataset selection: it will probe a short trades query and pick the first dataset that works.
# - The default is AUTO with a sensible candidate list. You can override via --eq-dataset.
# - We still optionally download Nasdaq L2 via XNAS.ITCH (mbp-10), since that is a separate feed.
#
# Usage example:
#   conda activate TradingProject
#   DATABENTO_API_KEY=... python /Users/zizizink/Documents/TradingProject/Data/download_day_databento_consolidated.py \
#     --day 2026-01-06 --symbols ALMS \
#     --tz America/New_York --start-time 07:00:00 --end-time 10:30:00 \
#     --out-dir /Users/zizizink/Documents/TradingProject/databento_out

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import os
from pathlib import Path
import tempfile
from typing import Sequence
from zoneinfo import ZoneInfo

import databento as db
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


def _existing_symbols_in_parquet(parquet_path: Path) -> set[str]:
    """
    Return the set of unique values in the 'symbol' column for an existing parquet file.

    Uses streaming batch reads to avoid loading the entire dataset into memory.
    """
    if not parquet_path.exists():
        return set()

    pf = pq.ParquetFile(parquet_path)
    # Some older/malformed outputs might not have symbol mapping enabled.
    if "symbol" not in pf.schema.names:
        raise ValueError(
            f"{parquet_path} is missing a 'symbol' column. "
            "Re-download with map_symbols=True (or delete the file and re-run)."
        )

    out: set[str] = set()
    for batch in pf.iter_batches(columns=["symbol"], batch_size=250_000):
        col = batch.column(0)
        # Ensure Python strings (handles dictionary encoding, etc.)
        out.update(pa.compute.unique(col).to_pylist())

    # Drop nulls if present
    out.discard(None)  # type: ignore[arg-type]
    return out


def _merge_parquets_concat(
    parquet_paths: Sequence[Path],
    out_path: Path,
    *,
    batch_size: int = 250_000,
) -> Path:
    """
    Concatenate multiple parquet files into one parquet file, streaming batches.

    Notes:
    - This performs a local rewrite of the output file (Parquet isn't safely "appendable" in-place),
      but it avoids *re-downloading* existing symbols from Databento.
    - Assumes schemas are compatible. If not, we raise with a clear error.
    """
    parquet_paths = [p for p in parquet_paths if p.exists()]
    if not parquet_paths:
        raise FileNotFoundError("No parquet inputs provided to merge.")

    first_pf = pq.ParquetFile(parquet_paths[0])
    schema = first_pf.schema_arrow

    # Validate compatibility up-front to fail fast.
    for p in parquet_paths[1:]:
        pf = pq.ParquetFile(p)
        if pf.schema_arrow.names != schema.names:
            raise ValueError(
                "Parquet schema column mismatch while merging:\n"
                f"- base: {parquet_paths[0].name}: {schema.names}\n"
                f"- other: {p.name}: {pf.schema_arrow.names}\n"
                "These need to match to safely merge. Consider re-downloading into a fresh file."
            )

    tmp_out = out_path.with_suffix(out_path.suffix + ".tmp")
    if tmp_out.exists():
        tmp_out.unlink()

    with pq.ParquetWriter(tmp_out, schema=schema) as writer:
        for p in parquet_paths:
            pf = pq.ParquetFile(p)
            # Best-effort type check (names checked above). If types differ, we try to cast.
            for batch in pf.iter_batches(batch_size=batch_size):
                tbl = pa.Table.from_batches([batch])
                if tbl.schema != schema:
                    tbl = tbl.select(schema.names).cast(schema, safe=False)
                writer.write_table(tbl)

    tmp_out.replace(out_path)
    return out_path


def fetch_to_parquet_incremental(
    client: db.Historical,
    *,
    dataset: str,
    schema: str,
    symbols: Sequence[str],
    start: datetime,
    end: datetime,
    out_path: Path,
    overwrite: bool = False,
) -> Path:
    """
    Ensure `out_path` contains data for `symbols` over [start, end].

    If `out_path` exists and overwrite=False, we only download symbols that are missing from the file,
    then merge locally.
    """
    desired = list(dict.fromkeys(symbols))  # stable unique
    if overwrite or not out_path.exists():
        return fetch_to_parquet(
            client,
            dataset=dataset,
            schema=schema,
            symbols=desired,
            start=start,
            end=end,
            out_path=out_path,
        )

    existing = _existing_symbols_in_parquet(out_path)
    missing = [s for s in desired if s not in existing]
    if not missing:
        print(f"Already present ({dataset}/{schema}) in {out_path.name}: {sorted(existing)}")
        return out_path

    print(f"Incremental download ({dataset}/{schema}): missing symbols {missing} -> {out_path.name}")

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        tmp_new = td_path / f"{out_path.stem}.new{out_path.suffix}"
        fetch_to_parquet(
            client,
            dataset=dataset,
            schema=schema,
            symbols=missing,
            start=start,
            end=end,
            out_path=tmp_new,
        )
        # Merge existing + new into the original out_path.
        _merge_parquets_concat([out_path, tmp_new], out_path)

    return out_path


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


def _parse_dataset_list(eq_dataset: str) -> list[str]:
    """
    Accept either:
      - "AUTO" (case-insensitive)
      - a single dataset code like "EQUS.PLUS"
      - a comma-separated list like "EQUS.PLUS,XNAS.NLS,XCIS.TRADESBBO"
    """
    s = str(eq_dataset or "").strip()
    if not s:
        return ["AUTO"]
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return parts or ["AUTO"]


def _probe_trades_dataset(
    client: db.Historical,
    *,
    dataset: str,
    symbol: str,
    start: datetime,
    end: datetime,
) -> tuple[bool, str]:
    """
    Return (ok, reason). Uses a tiny trades query to see if the dataset is actually usable.
    """
    try:
        store = client.timeseries.get_range(
            dataset=dataset,
            schema="trades",
            stype_in="raw_symbol",
            symbols=[symbol],
            start=start,
            end=end,
        )
        # If Databento returns a Store with 0 records, it's still a "working" dataset, but not useful.
        try:
            n = int(getattr(store, "n", 0) or 0)
        except Exception:
            n = 0
        if n <= 0:
            return False, "query returned 0 records"
        return True, f"ok (n={n})"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _choose_eq_dataset(
    client: db.Historical,
    *,
    eq_dataset: str,
    symbols: Sequence[str],
    start: datetime,
    end: datetime,
) -> str:
    """
    Choose an equities dataset for trades/ohlcv-1s downloads.

    If eq_dataset is AUTO, try a short list of candidates commonly used for consolidated-ish tape:
      - EQUS.PLUS (broader composite feed than EQUS.MINI on many accounts)
      - XNAS.NLS (Nasdaq Last Sale; strong for Nasdaq-listed symbols)
      - XCIS.TRADESBBO / XCIS.TRADES (consolidated-like trades feeds when entitled)
      - EQUS.MINI (fallback; you already use this)
    """
    want = _parse_dataset_list(eq_dataset)
    if want and want[0].upper() != "AUTO":
        return want[0]

    # Candidate order: broad composite first, then venue-specific last-sale, then consolidated variants, then mini fallback.
    candidates = [
        "EQUS.PLUS",
        "EQUS.ALL",
        "XNAS.NLS",
        "XCIS.TRADESBBO",
        "XCIS.TRADES",
        "XNYS.TRADESBBO",
        "XNYS.TRADES",
        "EQUS.MINI",
    ]
    sym0 = str(list(symbols)[0])

    # Use a small probe window (first 2 minutes of requested window) to avoid large charges.
    probe_end = start
    try:
        # Add 2 minutes to start without importing timedelta (keep deps minimal).
        probe_end = datetime.fromtimestamp(start.timestamp() + 120, tz=ZoneInfo("UTC"))
    except Exception:
        probe_end = end

    for ds in candidates:
        ok, reason = _probe_trades_dataset(client, dataset=ds, symbol=sym0, start=start, end=probe_end)
        print(f"[AUTO] Probe {ds}: {reason}")
        if ok:
            return ds

    raise RuntimeError(
        "AUTO dataset selection failed: none of the candidate datasets returned trades for "
        f"symbol={sym0!r} in the requested window. "
        "Try setting --eq-dataset explicitly from the dataset list Databento returned."
    )


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
    - open  = open of the earliest 1s row present in the bucket
    - high  = max(high)
    - low   = min(low)
    - close = close of the latest 1s row present in the bucket
    - volume = sum(volume)

    Notes:
    - Databento `ohlcv-1s` can be sparse (not every second trades). We therefore **do not**
      require a "full" bucket to emit a bar; any bucket with at least one 1s row is kept.
    - Output parquet has columns: symbol, ts, open, high, low, close, volume
    """
    freq_map = {"10s": 10, "1min": 60, "5min": 300}
    if freq not in freq_map:
        raise ValueError(f"Unsupported freq={freq!r}. Supported: {sorted(freq_map)}")
    bucket_sec = freq_map[freq]
    bucket_ns = bucket_sec * 1_000_000_000

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
    table = table.append_column("_ts_ns", ts_ns)
    # NOTE: for non-negative int64 timestamps, integer division is equivalent to floor division.
    bucket_ns_s = pa.scalar(bucket_ns, pa.int64())
    bucket_start_ns = pc.multiply(pc.divide(ts_ns, bucket_ns_s), bucket_ns_s)
    table = table.append_column("_bucket_start_ns", bucket_start_ns)

    # Aggregate high/low/volume + min/max ts per (symbol, bucket).
    gb = table.group_by(["symbol", "_bucket_start_ns"])
    agg = gb.aggregate(
        [
            ("high", "max"),
            ("low", "min"),
            ("volume", "sum"),
            ("_ts_ns", "min"),
            ("_ts_ns", "max"),
        ]
    ).rename_columns(["symbol", "_bucket_start_ns", "high", "low", "volume", "_ts_min", "_ts_max"])

    # Lookup (symbol, ts_ns) -> open/close values
    base = pa.table({"symbol": table["symbol"], "_ts_ns": table["_ts_ns"], "_open": table["open"], "_close": table["close"]})
    open_keys = pa.table({"symbol": agg["symbol"], "_ts_ns": agg["_ts_min"]})
    close_keys = pa.table({"symbol": agg["symbol"], "_ts_ns": agg["_ts_max"]})
    open_joined = open_keys.join(base, keys=["symbol", "_ts_ns"], join_type="left outer")
    close_joined = close_keys.join(base, keys=["symbol", "_ts_ns"], join_type="left outer")

    # Drop buckets where open/close lookups failed (best-effort; should be rare).
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
    day: str,  # "YYYY-MM-DD" (treated as US/Eastern session date)
    symbols: Sequence[str],
    out_dir: str = "./databento_out",
    window: SessionWindow = SessionWindow(),
    eq_dataset: str = "AUTO",
    download_trades: bool = True,
    download_ohlcv1s: bool = True,
    download_mbp10: bool = True,
    overwrite: bool = False,
) -> None:
    out = Path(out_dir)
    _ensure_dir(out)

    start, end = window.bounds(day)
    client = db.Historical()  # uses DATABENTO_API_KEY env var

    # Choose the best dataset we can actually query under this account.
    chosen_ds = _choose_eq_dataset(client, eq_dataset=eq_dataset, symbols=symbols, start=start, end=end)
    if str(eq_dataset or "").strip().upper() == "AUTO":
        print(f"[AUTO] Using eq dataset: {chosen_ds}")
    eq_dataset = chosen_ds

    # 1) Consolidated-ish tape + bars (dataset configurable; defaults to EQUS)
    trades_path = out / f"{eq_dataset}.{day}.trades.parquet"
    ohlcv1s_path = out / f"{eq_dataset}.{day}.ohlcv-1s.parquet"

    # 2) L2 depth (Nasdaq book)
    mbp10_path = out / f"XNAS.ITCH.{day}.mbp-10.parquet"

    # 3) Derived bars from 1-second OHLCV
    bars10s_path = out / f"{eq_dataset}.{day}.ohlcv-10s.parquet"
    bars1m_path = out / f"{eq_dataset}.{day}.ohlcv-1m.parquet"
    bars5m_path = out / f"{eq_dataset}.{day}.ohlcv-5m.parquet"

    if download_trades:
        fetch_to_parquet_incremental(
            client,
            dataset=eq_dataset,
            schema="trades",
            symbols=symbols,
            start=start,
            end=end,
            out_path=trades_path,
            overwrite=overwrite,
        )

    if download_ohlcv1s:
        try:
            fetch_to_parquet_incremental(
                client,
                dataset=eq_dataset,
                schema="ohlcv-1s",
                symbols=symbols,
                start=start,
                end=end,
                out_path=ohlcv1s_path,
                overwrite=overwrite,
            )
        except Exception as e:
            # Some datasets expose trades but not ohlcv-1s. Keep going; trades are the main point.
            print(f"WARNING: failed to download ohlcv-1s from {eq_dataset}: {type(e).__name__}: {e}")
            print("         You can still replay tape; consider building OHLCV from trades or using EQUS.MINI for OHLCV.")
            download_ohlcv1s = False

        # Build derived bars (local resampling, no extra API usage).
        existing_10s = _existing_symbols_in_parquet(bars10s_path) if bars10s_path.exists() else set()
        missing_10s = [s for s in symbols if s not in existing_10s] if not overwrite else list(symbols)
        existing_1m = _existing_symbols_in_parquet(bars1m_path) if bars1m_path.exists() else set()
        missing_1m = [s for s in symbols if s not in existing_1m] if not overwrite else list(symbols)
        existing_5m = _existing_symbols_in_parquet(bars5m_path) if bars5m_path.exists() else set()
        missing_5m = [s for s in symbols if s not in existing_5m] if not overwrite else list(symbols)

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            if overwrite or not bars10s_path.exists():
                build_10s_bars_from_ohlcv_1s(ohlcv1s_path, bars10s_path, tz=window.tz, symbols=symbols)
            elif missing_10s:
                tmp = td_path / f"{bars10s_path.stem}.new{bars10s_path.suffix}"
                build_10s_bars_from_ohlcv_1s(ohlcv1s_path, tmp, tz=window.tz, symbols=missing_10s)
                _merge_parquets_concat([bars10s_path, tmp], bars10s_path)

            if overwrite or not bars1m_path.exists():
                build_1m_bars_from_ohlcv_1s(ohlcv1s_path, bars1m_path, tz=window.tz, symbols=symbols)
            elif missing_1m:
                tmp = td_path / f"{bars1m_path.stem}.new{bars1m_path.suffix}"
                build_1m_bars_from_ohlcv_1s(ohlcv1s_path, tmp, tz=window.tz, symbols=missing_1m)
                _merge_parquets_concat([bars1m_path, tmp], bars1m_path)

            if overwrite or not bars5m_path.exists():
                build_5m_bars_from_ohlcv_1s(ohlcv1s_path, bars5m_path, tz=window.tz, symbols=symbols)
            elif missing_5m:
                tmp = td_path / f"{bars5m_path.stem}.new{bars5m_path.suffix}"
                build_5m_bars_from_ohlcv_1s(ohlcv1s_path, tmp, tz=window.tz, symbols=missing_5m)
                _merge_parquets_concat([bars5m_path, tmp], bars5m_path)

    if download_mbp10:
        fetch_to_parquet_incremental(
            client,
            dataset="XNAS.ITCH",
            schema="mbp-10",
            symbols=symbols,
            start=start,
            end=end,
            out_path=mbp10_path,
            overwrite=overwrite,
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
    p = argparse.ArgumentParser(description="Download a window of consolidated-ish Databento equities data to Parquet.")
    p.add_argument("--day", required=True, help="YYYY-MM-DD (session date, interpreted in --tz by default)")
    p.add_argument("--symbols", required=True, nargs="+", help="One or more raw symbols (e.g. ALMS)")
    p.add_argument("--out-dir", default="./databento_out")
    p.add_argument("--tz", default="America/New_York")
    p.add_argument("--start-time", default="09:30:00", help="HH:MM:SS in --tz (e.g. 07:00:00)")
    p.add_argument("--end-time", default="16:00:00", help="HH:MM:SS in --tz (e.g. 10:30:00)")
    p.add_argument(
        "--eq-dataset",
        default="AUTO",
        help=(
            "Databento equities dataset code to use for trades/ohlcv-1s. "
            "Use AUTO (default) to probe and pick the first working dataset. "
            "You may also provide a comma-separated list, e.g. 'EQUS.PLUS,XNAS.NLS,XCIS.TRADESBBO'."
        ),
    )
    p.add_argument("--overwrite", action="store_true", help="Force a full re-download for the provided symbols.")
    p.add_argument(
        "--build-derived-only",
        action="store_true",
        help="Do NOT download anything. Only build 10s/1m/5m bars from an existing <eq_dataset>.<day>.ohlcv-1s parquet.",
    )
    p.add_argument("--skip-trades", action="store_true", help="Skip trades download")
    p.add_argument("--skip-ohlcv1s", action="store_true", help="Skip ohlcv-1s download (and derived bars)")
    p.add_argument("--skip-mbp10", action="store_true", help="Skip XNAS.ITCH mbp-10 download (saves credits)")
    args = p.parse_args()

    window = SessionWindow(start_time=args.start_time, end_time=args.end_time, tz=args.tz)
    out = Path(args.out_dir)
    if args.build_derived_only:
        ohlcv1s_path = out / f"{args.eq_dataset}.{args.day}.ohlcv-1s.parquet"
        if not ohlcv1s_path.exists():
            raise FileNotFoundError(f"Missing {ohlcv1s_path}. Nothing to build from.")

        bars10s_path = out / f"{args.eq_dataset}.{args.day}.ohlcv-10s.parquet"
        bars1m_path = out / f"{args.eq_dataset}.{args.day}.ohlcv-1m.parquet"
        bars5m_path = out / f"{args.eq_dataset}.{args.day}.ohlcv-5m.parquet"

        existing_10s = _existing_symbols_in_parquet(bars10s_path) if bars10s_path.exists() else set()
        missing_10s = [s for s in args.symbols if s not in existing_10s] if not args.overwrite else list(args.symbols)
        existing_1m = _existing_symbols_in_parquet(bars1m_path) if bars1m_path.exists() else set()
        missing_1m = [s for s in args.symbols if s not in existing_1m] if not args.overwrite else list(args.symbols)
        existing_5m = _existing_symbols_in_parquet(bars5m_path) if bars5m_path.exists() else set()
        missing_5m = [s for s in args.symbols if s not in existing_5m] if not args.overwrite else list(args.symbols)

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            if args.overwrite or not bars10s_path.exists():
                build_10s_bars_from_ohlcv_1s(ohlcv1s_path, bars10s_path, tz=window.tz, symbols=args.symbols)
            elif missing_10s:
                tmp = td_path / f"{bars10s_path.stem}.new{bars10s_path.suffix}"
                build_10s_bars_from_ohlcv_1s(ohlcv1s_path, tmp, tz=window.tz, symbols=missing_10s)
                _merge_parquets_concat([bars10s_path, tmp], bars10s_path)

            if args.overwrite or not bars1m_path.exists():
                build_1m_bars_from_ohlcv_1s(ohlcv1s_path, bars1m_path, tz=window.tz, symbols=args.symbols)
            elif missing_1m:
                tmp = td_path / f"{bars1m_path.stem}.new{bars1m_path.suffix}"
                build_1m_bars_from_ohlcv_1s(ohlcv1s_path, tmp, tz=window.tz, symbols=missing_1m)
                _merge_parquets_concat([bars1m_path, tmp], bars1m_path)

            if args.overwrite or not bars5m_path.exists():
                build_5m_bars_from_ohlcv_1s(ohlcv1s_path, bars5m_path, tz=window.tz, symbols=args.symbols)
            elif missing_5m:
                tmp = td_path / f"{bars5m_path.stem}.new{bars5m_path.suffix}"
                build_5m_bars_from_ohlcv_1s(ohlcv1s_path, tmp, tz=window.tz, symbols=missing_5m)
                _merge_parquets_concat([bars5m_path, tmp], bars5m_path)

        print("Ensured derived bars exist for:", args.symbols)
        print(" ", bars10s_path)
        print(" ", bars1m_path)
        print(" ", bars5m_path)
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
            eq_dataset=args.eq_dataset,
            download_trades=not args.skip_trades,
            download_ohlcv1s=not args.skip_ohlcv1s,
            download_mbp10=not args.skip_mbp10,
            overwrite=args.overwrite,
        )

