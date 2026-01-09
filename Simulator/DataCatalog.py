"""
Simulator/DataCatalog.py

Scan a Databento output directory (parquet) and build a list of practice "sessions":
- symbol
- day (YYYY-MM-DD)
- earliest bar timestamp for that symbol/day (from OHLCV parquet)

This is used by the Simulator UI to populate a dropdown that can quickly load replay days.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


_TF_PREF: Tuple[str, ...] = ("1s", "10s", "1m", "5m")


@dataclass(frozen=True)
class CatalogItem:
    symbol: str
    day: str  # YYYY-MM-DD
    start_ts_ns: int  # UTC epoch ns
    start_et: str  # YYYY-MM-DD HH:MM:SS (ET)
    label: str  # "SYMB YYYY-MM-DD H:MM"


def _ts_array_to_ns(ts_arr: pa.Array) -> "pa.lib.Array":
    """
    Convert an Arrow timestamp/int array into an int64 array of epoch ns.
    Returns an Arrow int64 array.
    """
    if pa.types.is_timestamp(ts_arr.type):
        # Normalize any unit to ns using numpy datetime64[ns], then convert to int64 ns.
        np = ts_arr.to_numpy(zero_copy_only=False)
        # np dtype can be datetime64[s|ms|us|ns]; normalize to ns
        np_ns = np.astype("datetime64[ns]").astype("int64")
        return pa.array(np_ns, type=pa.int64())
    if pa.types.is_integer(ts_arr.type):
        # Assume already ns (this matches the simulator's expected ts_ns usage).
        return ts_arr.cast(pa.int64())
    # Fallback: try cast (may work for some scalar timestamp representations)
    return ts_arr.cast(pa.int64())


def _format_et(ts_ns: int, tz_name: str) -> Tuple[str, str]:
    """
    Returns:
      - full: YYYY-MM-DD HH:MM:SS in ET (or tz_name)
      - short: H:MM (no leading zero hour)
    """
    tz = None
    if ZoneInfo is not None:
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = None
    dt = datetime.fromtimestamp(ts_ns / 1e9, tz=tz)
    full = dt.strftime("%Y-%m-%d %H:%M:%S")
    short = f"{dt.hour}:{dt.minute:02d}"
    return full, short


def _earliest_ts_by_symbol(
    ohl_path: Path, ts_col: str, symbol_col: str = "symbol"
) -> Dict[str, int]:
    pf = pq.ParquetFile(ohl_path)
    out: Dict[str, int] = {}
    for batch in pf.iter_batches(columns=[ts_col, symbol_col], batch_size=1 << 16):
        ts_arr = batch.column(0)
        sym_arr = batch.column(1)
        # Normalize types
        ts_ns_arr = _ts_array_to_ns(ts_arr)
        # Symbols: ensure strings (handles dictionary encoding)
        syms = sym_arr.cast(pa.string()).to_pylist()
        ts_ns = ts_ns_arr.to_pylist()
        for sym, ns in zip(syms, ts_ns):
            if sym is None or ns is None:
                continue
            sym_s = str(sym)
            n = int(ns)
            prev = out.get(sym_s)
            if prev is None or n < prev:
                out[sym_s] = n
    return out


def _pick_best_ohlcv_file(data_dir: Path, day: str) -> Optional[Tuple[Path, str]]:
    for tf in _TF_PREF:
        p = data_dir / f"EQUS.MINI.{day}.ohlcv-{tf}.parquet"
        if p.exists():
            return p, tf
    return None


def scan_catalog(data_dir: Path, tz_name: str = "America/New_York") -> List[CatalogItem]:
    """
    Build a list of CatalogItem entries from the directory.
    We group by day (derived from OHLCV filenames), then compute min ts per symbol for that day.
    """
    data_dir = Path(data_dir)
    if not data_dir.exists():
        return []

    # Discover days from OHLCV filenames.
    days: List[str] = []
    for p in data_dir.glob("EQUS.MINI.*.ohlcv-*.parquet"):
        name = p.name
        # EQUS.MINI.YYYY-MM-DD.ohlcv-<tf>.parquet
        parts = name.split(".")
        if len(parts) < 4:
            continue
        day = parts[2]
        if len(day) == 10 and day[4] == "-" and day[7] == "-":
            days.append(day)

    days = sorted(set(days))

    items: List[CatalogItem] = []
    for day in days:
        # Require the core simulator inputs to exist for this day; otherwise it's not loadable.
        mbp_path = data_dir / f"XNAS.ITCH.{day}.mbp-10.parquet"
        trd_path = data_dir / f"EQUS.MINI.{day}.trades.parquet"
        if not (mbp_path.exists() and trd_path.exists()):
            continue

        picked = _pick_best_ohlcv_file(data_dir, day)
        if not picked:
            continue
        ohl_path, _tf = picked
        try:
            pf = pq.ParquetFile(ohl_path)
            schema_names = set(pf.schema.names)
            ts_col = "ts_event" if "ts_event" in schema_names else ("ts" if "ts" in schema_names else None)
            if ts_col is None:
                continue
            mins = _earliest_ts_by_symbol(ohl_path, ts_col=ts_col, symbol_col="symbol")
        except Exception:
            continue

        for sym, ts_ns in mins.items():
            full, short = _format_et(ts_ns, tz_name)
            label = f"{sym} {day} {short}"
            items.append(
                CatalogItem(
                    symbol=sym,
                    day=day,
                    start_ts_ns=int(ts_ns),
                    start_et=full,
                    label=label,
                )
            )

    # Sort: newest day first, then symbol
    items.sort(key=lambda x: (x.day, x.symbol, x.start_ts_ns), reverse=True)
    return items

