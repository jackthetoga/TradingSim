# download_day_databento.py
# pip install databento pandas pyarrow

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import shutil
import tempfile
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


def _merge_parquets_concat_columns(
    parquet_paths: Sequence[Path],
    out_path: Path,
    *,
    columns: Sequence[str],
    batch_size: int = 250_000,
) -> Path:
    """
    Concatenate multiple parquet files into one parquet file, streaming batches, keeping only `columns`.

    This is safer than `_merge_parquets_concat` when inputs may come from different datasets that
    include extra columns or differ slightly in schema details. The simulator only needs a subset
    of MBP-10 columns; we write a stable merged schema for those columns.
    """
    parquet_paths = [p for p in parquet_paths if p.exists()]
    if not parquet_paths:
        raise FileNotFoundError("No parquet inputs provided to merge.")

    first_pf = pq.ParquetFile(parquet_paths[0])
    schema0 = first_pf.schema_arrow
    missing0 = [c for c in columns if c not in schema0.names]
    if missing0:
        raise ValueError(
            f"Base parquet {parquet_paths[0].name} is missing required columns {missing0}. "
            f"Columns present: {schema0.names}"
        )
    out_schema = pa.schema([schema0.field(c) for c in columns])

    tmp_out = out_path.with_suffix(out_path.suffix + ".tmp")
    if tmp_out.exists():
        tmp_out.unlink()

    with pq.ParquetWriter(tmp_out, schema=out_schema) as writer:
        for p in parquet_paths:
            pf = pq.ParquetFile(p)
            missing = [c for c in columns if c not in pf.schema_arrow.names]
            if missing:
                raise ValueError(
                    f"Parquet {p.name} is missing required columns {missing}. "
                    f"Columns present: {pf.schema_arrow.names}"
                )
            for batch in pf.iter_batches(columns=list(columns), batch_size=batch_size):
                tbl = pa.Table.from_batches([batch])
                if tbl.schema != out_schema:
                    tbl = tbl.cast(out_schema, safe=False)
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


def _parquet_has_rows(parquet_path: Path) -> bool:
    if not parquet_path.exists():
        return False
    pf = pq.ParquetFile(parquet_path)
    md = pf.metadata
    if md is None:
        return False
    return md.num_rows > 0


def fetch_trades_with_dataset_fallback(
    client: db.Historical,
    *,
    symbols: Sequence[str],
    start: datetime,
    end: datetime,
    out_path: Path,
    overwrite: bool = False,
    dataset_priority: Sequence[str] = ("XNAS.BASIC", "XNYS.PILLAR", "EDGX.PITCH", "EDGA.PITCH", "EQUS.MINI"),
) -> Path:
    """
    Trades-only downloader: try multiple datasets in priority order, but always write to `out_path`.

    This is used by `download_day_databento_not_EQUS.py` to prefer venue-specific feeds for trades
    when available, falling back to EQUS.MINI as a last resort.
    """
    desired = list(dict.fromkeys(symbols))  # stable unique
    if not desired:
        raise ValueError("No symbols provided.")

    if overwrite or not out_path.exists():
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            tmp = td_path / f"{out_path.stem}.candidate{out_path.suffix}"
            errors: list[tuple[str, str]] = []
            for dataset in dataset_priority:
                try:
                    if tmp.exists():
                        tmp.unlink()
                    fetch_to_parquet(
                        client,
                        dataset=dataset,
                        schema="trades",
                        symbols=desired,
                        start=start,
                        end=end,
                        out_path=tmp,
                    )
                    if _parquet_has_rows(tmp):
                        tmp.replace(out_path)
                        print(f"Trades dataset selected: {dataset} -> {out_path.name}")
                        return out_path
                    errors.append((dataset, "download succeeded but returned 0 rows"))
                except Exception as e:  # noqa: BLE001 - best-effort fallback across datasets
                    errors.append((dataset, f"{type(e).__name__}: {e}"))

        raise RuntimeError(
            "Failed to download trades from any dataset. Attempts:\n"
            + "\n".join([f"- {ds}: {msg}" for ds, msg in errors])
        )

    # Incremental path: keep existing out_path, only fetch missing symbols, then merge.
    existing = _existing_symbols_in_parquet(out_path)
    missing = [s for s in desired if s not in existing]
    if not missing:
        print(f"Already present (trades) in {out_path.name}: {sorted(existing)}")
        return out_path

    print(f"Incremental download (trades): missing symbols {missing} -> {out_path.name}")
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        tmp_new = td_path / f"{out_path.stem}.new{out_path.suffix}"
        errors: list[tuple[str, str]] = []
        for dataset in dataset_priority:
            try:
                if tmp_new.exists():
                    tmp_new.unlink()
                fetch_to_parquet(
                    client,
                    dataset=dataset,
                    schema="trades",
                    symbols=missing,
                    start=start,
                    end=end,
                    out_path=tmp_new,
                )
                if _parquet_has_rows(tmp_new):
                    _merge_parquets_concat([out_path, tmp_new], out_path)
                    print(f"Trades dataset selected (incremental): {dataset} -> {out_path.name}")
                    return out_path
                errors.append((dataset, "download succeeded but returned 0 rows"))
            except Exception as e:  # noqa: BLE001 - best-effort fallback across datasets
                errors.append((dataset, f"{type(e).__name__}: {e}"))

    raise RuntimeError(
        "Failed to incrementally download trades from any dataset. Attempts:\n"
        + "\n".join([f"- {ds}: {msg}" for ds, msg in errors])
    )


def fetch_with_dataset_fallback(
    client: db.Historical,
    *,
    schema: str,
    symbols: Sequence[str],
    start: datetime,
    end: datetime,
    out_path: Path,
    overwrite: bool = False,
    dataset_priority: Sequence[str],
) -> Path:
    """
    Generic downloader: try multiple datasets in priority order for the same schema, but always
    write to `out_path` (so naming conventions stay stable even when the selected dataset changes).
    """
    desired = list(dict.fromkeys(symbols))  # stable unique
    if not desired:
        raise ValueError("No symbols provided.")

    if overwrite or not out_path.exists():
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            tmp = td_path / f"{out_path.stem}.candidate{out_path.suffix}"
            errors: list[tuple[str, str]] = []
            for dataset in dataset_priority:
                try:
                    if tmp.exists():
                        tmp.unlink()
                    fetch_to_parquet(
                        client,
                        dataset=dataset,
                        schema=schema,
                        symbols=desired,
                        start=start,
                        end=end,
                        out_path=tmp,
                    )
                    if _parquet_has_rows(tmp):
                        tmp.replace(out_path)
                        print(f"{schema} dataset selected: {dataset} -> {out_path.name}")
                        return out_path
                    errors.append((dataset, "download succeeded but returned 0 rows"))
                except Exception as e:  # noqa: BLE001 - best-effort fallback across datasets
                    errors.append((dataset, f"{type(e).__name__}: {e}"))

        raise RuntimeError(
            f"Failed to download {schema} from any dataset. Attempts:\n"
            + "\n".join([f"- {ds}: {msg}" for ds, msg in errors])
        )

    # Incremental path: keep existing out_path, only fetch missing symbols, then merge.
    existing = _existing_symbols_in_parquet(out_path)
    missing = [s for s in desired if s not in existing]
    if not missing:
        print(f"Already present ({schema}) in {out_path.name}: {sorted(existing)}")
        return out_path

    print(f"Incremental download ({schema}): missing symbols {missing} -> {out_path.name}")
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        tmp_new = td_path / f"{out_path.stem}.new{out_path.suffix}"
        errors: list[tuple[str, str]] = []
        for dataset in dataset_priority:
            try:
                if tmp_new.exists():
                    tmp_new.unlink()
                fetch_to_parquet(
                    client,
                    dataset=dataset,
                    schema=schema,
                    symbols=missing,
                    start=start,
                    end=end,
                    out_path=tmp_new,
                )
                if _parquet_has_rows(tmp_new):
                    _merge_parquets_concat([out_path, tmp_new], out_path)
                    print(f"{schema} dataset selected (incremental): {dataset} -> {out_path.name}")
                    return out_path
                errors.append((dataset, "download succeeded but returned 0 rows"))
            except Exception as e:  # noqa: BLE001 - best-effort fallback across datasets
                errors.append((dataset, f"{type(e).__name__}: {e}"))

    raise RuntimeError(
        f"Failed to incrementally download {schema} from any dataset. Attempts:\n"
        + "\n".join([f"- {ds}: {msg}" for ds, msg in errors])
    )


def fetch_mbp10_multi_venue_combined(
    client: db.Historical,
    *,
    symbols: Sequence[str],
    start: datetime,
    end: datetime,
    out_dir: Path,
    day: str,
    overwrite: bool = False,
    datasets: Sequence[str] = ("XNAS.ITCH", "EDGX.PITCH", "BATS.PITCH"),
) -> Path:
    """
    Download MBP-10 from multiple venues and combine into a single simulator-compatible file.

    The simulator expects the filename pattern `XNAS.ITCH.<DAY>.mbp-10.parquet` regardless of
    where the depth was sourced from. We therefore download each venue into temporary parquet
    files, then concatenate into the compatibility filename.
    """
    out_dir = Path(out_dir)
    _ensure_dir(out_dir)

    combined = out_dir / f"XNAS.ITCH.{day}.mbp-10.parquet"

    # Simulator only reads these columns for MBP-10:
    mbp_cols = (
        ["ts_event", "symbol"]
        + [f"bid_px_{i:02d}" for i in range(10)]
        + [f"bid_sz_{i:02d}" for i in range(10)]
        + [f"ask_px_{i:02d}" for i in range(10)]
        + [f"ask_sz_{i:02d}" for i in range(10)]
    )

    desired = list(dict.fromkeys(symbols))  # stable unique
    if not desired:
        raise ValueError("No symbols provided.")

    # Fresh build: download all requested symbols from each venue, then merge into the combined file.
    if overwrite or not combined.exists():
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            per_venue_tmp: list[Path] = []
            for ds in datasets:
                p = td_path / f"{ds}.{day}.mbp-10.parquet"
                fetch_to_parquet(
                    client,
                    dataset=ds,
                    schema="mbp-10",
                    symbols=desired,
                    start=start,
                    end=end,
                    out_path=p,
                )
                per_venue_tmp.append(p)
            _merge_parquets_concat_columns(per_venue_tmp, combined, columns=mbp_cols)
        return combined

    # Incremental build: only download missing symbols, merge them, then merge with the existing combined file.
    existing = _existing_symbols_in_parquet(combined)
    missing = [s for s in desired if s not in existing]
    if not missing:
        print(f"Already present (mbp-10 combined) in {combined.name}: {sorted(existing)}")
        return combined

    print(f"Incremental download (mbp-10 combined): missing symbols {missing} -> {combined.name}")
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        per_venue_tmp: list[Path] = []
        for ds in datasets:
            p = td_path / f"{ds}.{day}.mbp-10.parquet"
            fetch_to_parquet(
                client,
                dataset=ds,
                schema="mbp-10",
                symbols=missing,
                start=start,
                end=end,
                out_path=p,
            )
            per_venue_tmp.append(p)

        merged_missing = td_path / f"XNAS.ITCH.{day}.mbp-10.missing.parquet"
        _merge_parquets_concat_columns(per_venue_tmp, merged_missing, columns=mbp_cols)
        _merge_parquets_concat_columns([combined, merged_missing], combined, columns=mbp_cols)

    return combined


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
    table = table.append_column("_ts_ns", ts_ns)
    # NOTE: pyarrow.compute does not expose floor_divide in some versions (e.g. pyarrow 21).
    # For non-negative int64 timestamps, integer division is equivalent to floor division.
    bucket_ns_s = pa.scalar(bucket_ns, pa.int64())
    bucket_start_ns = pc.multiply(pc.divide(ts_ns, bucket_ns_s), bucket_ns_s)
    table = table.append_column("_bucket_start_ns", bucket_start_ns)

    # Aggregate high/low/volume + min/max ts per (symbol, bucket).
    # We avoid ordered aggregations ("first"/"last") since they can be unsupported in some builds.
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

    # Lookup (symbol, ts_ns) -> open/close values (using the earliest and latest 1s rows in the bucket).
    # For 1s bars, (symbol, ts_ns) is typically unique; if not, the sort above makes selection stable.
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
    day: str,                  # "YYYY-MM-DD" (treated as US/Eastern session date)
    symbols: Sequence[str],
    out_dir: str = "./databento_out",
    window: SessionWindow = SessionWindow(),
    download_trades: bool = True,
    download_ohlcv1s: bool = True,
    download_mbp10: bool = True,
    overwrite: bool = False,
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
        # Per user requirement: trades from XNAS.BASIC only (no fallback).
        fetch_to_parquet_incremental(
            client,
            dataset="XNAS.BASIC",
            schema="trades",
            symbols=symbols,
            start=start,
            end=end,
            out_path=trades_path,  # keep filename EQUS.MINI.<DAY>.trades.parquet for simulator compatibility
            overwrite=overwrite,
        )
    if download_ohlcv1s:
        # Per user requirement: 1s OHLCV from BATS.PITCH only (no fallback).
        fetch_to_parquet_incremental(
            client,
            dataset="BATS.PITCH",
            schema="ohlcv-1s",
            symbols=symbols,
            start=start,
            end=end,
            out_path=ohlcv1s_path,  # keep filename EQUS.MINI.<DAY>.ohlcv-1s.parquet for simulator compatibility
            overwrite=overwrite,
        )
        # Build derived bars (local resampling, no extra API usage).
        # We only build bars for symbols that are missing in each derived file, then merge locally.
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
        # Per user requirement: download multiple venues and combine into a single
        # `XNAS.ITCH.<DAY>.mbp-10.parquet` file for simulator compatibility.
        mbp10_path = fetch_mbp10_multi_venue_combined(
            client,
            symbols=symbols,
            start=start,
            end=end,
            out_dir=out,
            day=day,
            overwrite=overwrite,
            datasets=("XNAS.ITCH", "EDGX.PITCH", "BATS.PITCH"),
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
        "--overwrite",
        action="store_true",
        help="Force a full re-download (and rebuild derived bars) for the provided symbols, even if present in existing parquet files.",
    )
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

        bars10s_path = out / f"EQUS.MINI.{args.day}.ohlcv-10s.parquet"
        bars1m_path = out / f"EQUS.MINI.{args.day}.ohlcv-1m.parquet"
        bars5m_path = out / f"EQUS.MINI.{args.day}.ohlcv-5m.parquet"

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
            download_trades=not args.skip_trades,
            download_ohlcv1s=not args.skip_ohlcv1s,
            download_mbp10=not args.skip_mbp10,
            overwrite=args.overwrite,
        )
