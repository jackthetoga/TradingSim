"""
Data/list_downloaded.py

Offline inventory of what Databento data you have on disk.

It scans a databento output directory (e.g. databento_out/) and produces:
- list of days found
- list of symbols per day (best-effort from OHLCV parquet, fallback to trades parquet)
- flags indicating which core files exist per day (trades / mbp-10 / ohlcv-1s / derived bars)

This reuses the Simulator's parquet scanning logic but does NOT run the simulator.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pyarrow as pa
import pyarrow.parquet as pq


DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class DayInventory:
    day: str
    symbols: List[str]
    # file presence
    has_trades: bool
    has_mbp10: bool
    has_ohlcv_1s: bool
    has_ohlcv_10s: bool
    has_ohlcv_1m: bool
    has_ohlcv_5m: bool
    # paths (useful for debugging)
    trades_path: str
    mbp10_path: str
    ohlcv_1s_path: str
    ohlcv_10s_path: str
    ohlcv_1m_path: str
    ohlcv_5m_path: str


def _discover_days_from_filenames(data_dir: Path) -> List[str]:
    """
    Collect days that appear in any of the known naming patterns.
    """
    days: Set[str] = set()

    patterns = [
        "EQUS.MINI.*.trades.parquet",
        "EQUS.MINI.*.ohlcv-*.parquet",
        "XNAS.ITCH.*.mbp-10.parquet",
    ]
    for pat in patterns:
        for p in data_dir.glob(pat):
            parts = p.name.split(".")
            if len(parts) < 3:
                continue
            # EQUS.MINI.<DAY>.* or XNAS.ITCH.<DAY>.*
            day = parts[2]
            if DAY_RE.match(day):
                days.add(day)

    return sorted(days)


def _ts_array_to_ns(ts_arr: pa.Array) -> pa.Array:
    """
    Convert an Arrow timestamp/int array into an int64 array of epoch ns.
    Returns an Arrow int64 array.
    """
    if pa.types.is_timestamp(ts_arr.type):
        np = ts_arr.to_numpy(zero_copy_only=False)
        np_ns = np.astype("datetime64[ns]").astype("int64")
        return pa.array(np_ns, type=pa.int64())
    if pa.types.is_integer(ts_arr.type):
        return ts_arr.cast(pa.int64())
    return ts_arr.cast(pa.int64())


def _symbols_from_parquet(path: Path, *, symbol_col: str = "symbol") -> List[str]:
    """
    Best-effort unique symbol list from a parquet file.
    Uses streaming batches to avoid reading the whole file into memory.
    """
    pf = pq.ParquetFile(path)
    if symbol_col not in set(pf.schema.names):
        return []
    out: Set[str] = set()
    for batch in pf.iter_batches(columns=[symbol_col], batch_size=1 << 16):
        arr = batch.column(0).cast(pa.string())
        for s in arr.to_pylist():
            if s is None:
                continue
            ss = str(s).strip()
            if ss:
                out.add(ss)
    return sorted(out)


def _pick_best_ohlcv_file(data_dir: Path, day: str) -> Optional[Path]:
    # Prefer highest-res for symbol discovery.
    prefs = ("1s", "10s", "1m", "5m")
    for tf in prefs:
        p = data_dir / f"EQUS.MINI.{day}.ohlcv-{tf}.parquet"
        if p.exists():
            return p
    return None


def _symbols_for_day(data_dir: Path, day: str) -> List[str]:
    """
    Prefer OHLCV for symbol list (usually smaller than trades and already used by the Simulator).
    Fallback to trades if OHLCV is missing.
    """
    ohl = _pick_best_ohlcv_file(data_dir, day)
    if ohl is not None:
        return _symbols_from_parquet(ohl, symbol_col="symbol")
    trd = data_dir / f"EQUS.MINI.{day}.trades.parquet"
    if trd.exists():
        return _symbols_from_parquet(trd, symbol_col="symbol")
    return []


def scan_inventory(data_dir: Path) -> List[DayInventory]:
    data_dir = Path(data_dir)
    days = _discover_days_from_filenames(data_dir)
    out: List[DayInventory] = []
    for day in days:
        trades = data_dir / f"EQUS.MINI.{day}.trades.parquet"
        mbp10 = data_dir / f"XNAS.ITCH.{day}.mbp-10.parquet"
        o1s = data_dir / f"EQUS.MINI.{day}.ohlcv-1s.parquet"
        o10s = data_dir / f"EQUS.MINI.{day}.ohlcv-10s.parquet"
        o1m = data_dir / f"EQUS.MINI.{day}.ohlcv-1m.parquet"
        o5m = data_dir / f"EQUS.MINI.{day}.ohlcv-5m.parquet"

        syms = _symbols_for_day(data_dir, day)
        out.append(
            DayInventory(
                day=day,
                symbols=syms,
                has_trades=trades.exists(),
                has_mbp10=mbp10.exists(),
                has_ohlcv_1s=o1s.exists(),
                has_ohlcv_10s=o10s.exists(),
                has_ohlcv_1m=o1m.exists(),
                has_ohlcv_5m=o5m.exists(),
                trades_path=str(trades),
                mbp10_path=str(mbp10),
                ohlcv_1s_path=str(o1s),
                ohlcv_10s_path=str(o10s),
                ohlcv_1m_path=str(o1m),
                ohlcv_5m_path=str(o5m),
            )
        )
    return out


def _write_pairs_csv(items: Sequence[DayInventory], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["day", "symbol"])
        for it in items:
            for s in it.symbols:
                w.writerow([it.day, s])


def _write_days_csv(items: Sequence[DayInventory], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "day",
                "n_symbols",
                "has_trades",
                "has_mbp10",
                "has_ohlcv_1s",
                "has_ohlcv_10s",
                "has_ohlcv_1m",
                "has_ohlcv_5m",
            ]
        )
        for it in items:
            w.writerow(
                [
                    it.day,
                    len(it.symbols),
                    int(it.has_trades),
                    int(it.has_mbp10),
                    int(it.has_ohlcv_1s),
                    int(it.has_ohlcv_10s),
                    int(it.has_ohlcv_1m),
                    int(it.has_ohlcv_5m),
                ]
            )


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description="List downloaded symbols/dates from databento_out (offline).")
    p.add_argument(
        "--data-dir",
        default="/Users/zizizink/Documents/TradingProject/databento_out",
        help="Path to databento output directory",
    )
    p.add_argument(
        "--out-json",
        default="",
        help="If set, write full inventory as JSON to this path.",
    )
    p.add_argument(
        "--out-days-csv",
        default="",
        help="If set, write per-day summary CSV to this path.",
    )
    p.add_argument(
        "--out-pairs-csv",
        default="",
        help="If set, write (day,symbol) pairs CSV to this path.",
    )
    args = p.parse_args(argv)

    data_dir = Path(args.data_dir)
    items = scan_inventory(data_dir)

    # Console summary
    all_syms: Set[str] = set()
    for it in items:
        all_syms.update(it.symbols)

    print(f"data_dir: {data_dir}")
    print(f"days: {len(items)}")
    print(f"unique_symbols: {len(all_syms)}")
    if items:
        print(f"day_range: {items[0].day} .. {items[-1].day}")
    print("")
    for it in items:
        flags = []
        if it.has_trades:
            flags.append("trades")
        if it.has_mbp10:
            flags.append("mbp10")
        if it.has_ohlcv_1s:
            flags.append("ohlcv1s")
        if it.has_ohlcv_10s:
            flags.append("ohlcv10s")
        if it.has_ohlcv_1m:
            flags.append("ohlcv1m")
        if it.has_ohlcv_5m:
            flags.append("ohlcv5m")
        flag_s = ",".join(flags) if flags else "-"
        print(f"{it.day}  symbols={len(it.symbols):4d}  files={flag_s}")

    if args.out_json:
        out_path = Path(args.out_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "data_dir": str(data_dir),
            "days": [asdict(x) for x in items],
        }
        out_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
        print(f"\nWrote JSON: {out_path}")

    if args.out_days_csv:
        out_path = Path(args.out_days_csv)
        _write_days_csv(items, out_path)
        print(f"Wrote days CSV: {out_path}")

    if args.out_pairs_csv:
        out_path = Path(args.out_pairs_csv)
        _write_pairs_csv(items, out_path)
        print(f"Wrote pairs CSV: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

