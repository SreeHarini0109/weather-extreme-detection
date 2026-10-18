"""CSV vs Parquet read time (and size) for the same stations: why the data lake is Parquet.

    python benchmark/format_benchmark.py --stations 10
"""
import argparse
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.config import load_config, resolve  # noqa: E402
from core.pipeline import LOAD_COLS, read_manifest, station_path  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data")
    ap.add_argument("--stations", type=int, default=10)
    ap.add_argument("--out", default="results/format_benchmark.csv")
    a = ap.parse_args()
    cfg = load_config()
    data = resolve(a.data or cfg["paths"]["parquet"])
    ids = sorted(read_manifest(data)["stations"])[: a.stations]
    rows = []
    with tempfile.TemporaryDirectory() as tmp:
        csvs = []
        for s in ids:
            df = pd.read_parquet(station_path(data, s))
            p = Path(tmp) / f"{s}.csv"
            df.to_csv(p, index=False)
            csvs.append(p)
        n = 0
        t0 = time.perf_counter()
        for p in csvs:
            n += len(pd.read_csv(p, parse_dates=["timestamp"]))
        t_csv_all = time.perf_counter() - t0
        t0 = time.perf_counter()
        for p in csvs:
            pd.read_csv(p, usecols=LOAD_COLS, parse_dates=["timestamp"])
        t_csv_cols = time.perf_counter() - t0
        t0 = time.perf_counter()
        for s in ids:
            pd.read_parquet(station_path(data, s))
        t_pq_all = time.perf_counter() - t0
        t0 = time.perf_counter()
        for s in ids:
            pd.read_parquet(station_path(data, s), columns=LOAD_COLS)
        t_pq_cols = time.perf_counter() - t0
        size_csv = sum(p.stat().st_size for p in csvs)
        size_pq = sum(f.stat().st_size for s in ids for f in station_path(data, s).glob("*.parquet"))
    rows = [
        {"format": "csv", "columns": "all", "rows": n, "seconds": t_csv_all, "mib": size_csv / 2**20},
        {"format": "csv", "columns": "6 needed", "rows": n, "seconds": t_csv_cols, "mib": size_csv / 2**20},
        {"format": "parquet", "columns": "all", "rows": n, "seconds": t_pq_all, "mib": size_pq / 2**20},
        {"format": "parquet", "columns": "6 needed (pruned)", "rows": n, "seconds": t_pq_cols, "mib": size_pq / 2**20},
    ]
    df = pd.DataFrame(rows)
    df["mrows_per_s"] = df["rows"] / df["seconds"] / 1e6
    out = resolve(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.round(3).to_csv(out, index=False)
    print(df.round(3).to_string(index=False))


if __name__ == "__main__":
    main()
