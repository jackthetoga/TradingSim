# test_slice.py
# pip install databento pandas pyarrow

import os
from pathlib import Path
import pandas as pd
import databento as db
os.environ["DATABENTO_API_KEY"] = "db-8s3HfiJfeMWcuf4QHPWkmmEvspMNW"  # set your API key here
DATABENTO_API_KEY = "db-8s3HfiJfeMWcuf4QHPWkmmEvspMNW"

def fetch_parquet(dataset, schema, symbol, start, end, out_path):
    client = db.Historical()  # uses DATABENTO_API_KEY env var
    store = client.timeseries.get_range(
        dataset=dataset,
        schema=schema,
        stype_in="raw_symbol",
        symbols=[symbol],
        start=start,
        end=end,
    )
    store.to_parquet(
        out_path.as_posix(),
        map_symbols=True,
        pretty_ts=False,
        price_type="fixed",
    )

def bytes_and_rows(parquet_path: Path):
    size_bytes = parquet_path.stat().st_size
    df = pd.read_parquet(parquet_path, columns=["ts_event"] if "mbp" in parquet_path.name else None)
    return size_bytes, len(df)

def main():
    # pick a known active day + symbol you expect to have data
    symbol = "MNTS"
    day = "2026-01-05"  # change
    tz = "America/New_York"
    start = pd.Timestamp(f"{day} 09:30:00", tz=tz).tz_convert("UTC")
    end   = pd.Timestamp(f"{day} 09:35:00", tz=tz).tz_convert("UTC")

    out = Path("./databento_test"); out.mkdir(exist_ok=True)

    files = [
        ("EQUS.MINI", "trades",   out / f"{symbol}.{day}.eqsmini.trades.parquet"),
        ("EQUS.MINI", "ohlcv-1s", out / f"{symbol}.{day}.eqsmini.ohlcv1s.parquet"),
        ("XNAS.ITCH", "mbp-10",   out / f"{symbol}.{day}.xnas.mbp10.parquet"),
    ]

    for dataset, schema, path in files:
        fetch_parquet(dataset, schema, symbol, start, end, path)
        b, n = bytes_and_rows(path)
        secs = (end - start).total_seconds()
        print(f"{path.name}: {n:,} rows, {b/1e6:.1f} MB, {b/secs/1e6:.2f} MB/s")

    print("\nExtrapolate (rough): full 16h window = 57,600s. Multiply MB/s by 57,600.")

if __name__ == "__main__":
    # export DATABENTO_API_KEY="db-..."
    main()
