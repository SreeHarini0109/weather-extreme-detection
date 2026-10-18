"""Stage 2: parse raw files (synthetic parquet/csv or Open-Meteo JSON) into the unified schema and write
Parquet partitioned by station_id, plus manifest.json and data_quality_report.csv.

Raw values are preserved (invalid values are only *counted* here); the engines' P2 phase does the
actual cleaning, so that work is parallelised and timed like every other phase.

    python data/to_parquet.py --input data/raw/synthetic --output data/parquet/synthetic
    python data/to_parquet.py --input data/raw/openmeteo --output data/parquet/real
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.cleaning import clean_hourly  # noqa: E402
from core.config import load_config, resolve  # noqa: E402

SCHEMA = pa.schema([
    ("station_name", pa.string()), ("lat", pa.float32()), ("lon", pa.float32()), ("elevation", pa.float32()),
    ("timestamp", pa.timestamp("us", tz="UTC")),
    ("temp_c", pa.float32()), ("humidity_pct", pa.float32()), ("precip_mm", pa.float32()),
    ("wind_kmh", pa.float32()), ("gust_kmh", pa.float32()),
])
ROW_GROUP = 8784   # ~one year of hourly rows per row group -> year filters skip row groups


def read_openmeteo(files: list[Path], stations: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for f in sorted(files):
        with open(f) as fh:
            js = json.load(fh)
        h = js["hourly"]
        frames.append(pd.DataFrame({
            "timestamp": pd.to_datetime(h["time"], utc=True),
            "temp_c": h["temperature_2m"], "humidity_pct": h["relative_humidity_2m"],
            "precip_mm": h["precipitation"], "wind_kmh": h["wind_speed_10m"], "gust_kmh": h["wind_gusts_10m"],
        }))
    df = pd.concat(frames, ignore_index=True)
    sid = files[0].name.split("_")[0]
    meta = stations.set_index("station_id").loc[sid]
    df["station_id"] = sid
    df["station_name"] = meta["station_name"]
    df["lat"], df["lon"], df["elevation"] = meta["lat"], meta["lon"], meta["elevation"]
    return df


def read_raw(path_or_files, kind: str, stations: pd.DataFrame | None) -> pd.DataFrame:
    if kind == "openmeteo":
        return read_openmeteo(path_or_files, stations)
    p = Path(path_or_files)
    if p.suffix == ".csv":
        df = pd.read_csv(p)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df
    return pd.read_parquet(p)


def convert_one(args):
    sid, src, kind, out_dir, cfg, stations = args
    t0 = time.perf_counter()
    df = read_raw(src, kind, stations)
    ts = pd.to_datetime(df["timestamp"], utc=True)
    df = df.assign(timestamp=ts).sort_values("timestamp", kind="stable").reset_index(drop=True)
    for c in ["temp_c", "humidity_pct", "precip_mm", "wind_kmh", "gust_kmh", "lat", "lon", "elevation"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(np.float32)
    table = pa.Table.from_pandas(df[[f.name for f in SCHEMA]], schema=SCHEMA, preserve_index=False)
    part = Path(out_dir) / f"station_id={sid}"
    part.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, part / "part-0.parquet", row_group_size=ROW_GROUP, compression="snappy")

    # quality report only (no modification)
    naive = df.assign(timestamp=df["timestamp"].dt.tz_localize(None))
    _, q = clean_hourly(naive, cfg)
    n = max(q["rows_in"], 1)
    flagged = sum(v for k, v in q.items() if k.endswith("_flagged"))
    missing = sum(v for k, v in q.items() if k.endswith("_missing"))
    qrow = {"station_id": sid, **q, "pct_missing": round(100 * missing / (5 * n), 4),
            "pct_flagged": round(100 * flagged / (5 * n), 4)}
    meta = {"rows": int(len(df)), "path": f"station_id={sid}", "bytes": (part / "part-0.parquet").stat().st_size,
            "first": str(ts.min()), "last": str(ts.max()), "name": str(df["station_name"].iloc[0]),
            "lat": float(df["lat"].iloc[0]), "lon": float(df["lon"].iloc[0]),
            "elevation": float(df["elevation"].iloc[0])}
    return sid, meta, qrow, time.perf_counter() - t0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config")
    ap.add_argument("--input", help="raw directory (default: synthetic raw dir from config)")
    ap.add_argument("--output", help="parquet output directory")
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args(argv)
    cfg = load_config(a.config)
    src = resolve(a.input or cfg["paths"]["raw_synthetic"])
    out = resolve(a.output or cfg["paths"]["parquet"])

    json_files = sorted(src.glob("*.json"))
    stations = None
    if json_files:
        kind = "openmeteo"
        stations = pd.read_csv(resolve(cfg["paths"]["stations"]))
        groups: dict[str, list[Path]] = {}
        for f in json_files:
            groups.setdefault(f.name.split("_")[0], []).append(f)
        jobs = list(groups.items())
    else:
        kind = "synthetic"
        files = sorted(src.glob("*.parquet")) or sorted(src.glob("*.csv"))
        jobs = [(f.stem, f) for f in files]
    if not jobs:
        sys.exit(f"No raw files found in {src}")

    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    print(f"Converting {len(jobs)} stations ({kind}) from {src} -> {out}")
    t0 = time.perf_counter()
    manifest = {"dataset": out.name, "source": kind, "created": datetime.now().isoformat(timespec="seconds"),
                "stations": {}}
    qrows = []
    with ProcessPoolExecutor(max_workers=a.workers) as ex:
        for sid, meta, qrow, _ in ex.map(convert_one, [(sid, f, kind, str(out), cfg, stations) for sid, f in jobs]):
            manifest["stations"][sid] = meta
            qrows.append(qrow)
    st = manifest["stations"]
    manifest["n_stations"] = len(st)
    manifest["total_rows"] = int(sum(m["rows"] for m in st.values()))
    manifest["total_bytes"] = int(sum(m["bytes"] for m in st.values()))
    manifest["years"] = [min(int(m["first"][:4]) for m in st.values()), max(int(m["last"][:4]) for m in st.values())]
    # station country (for region-specific rules such as IMD): from the generator's stations_used.csv
    # when present (covers virtual stations), else from the configured stations.csv
    sources = [src / "stations_used.csv", resolve(cfg["paths"]["stations"])]
    countries = {}
    for f in sources[::-1]:
        if f.exists():
            countries.update(pd.read_csv(f).set_index("station_id")["country"].to_dict())
    for sid, m in manifest["stations"].items():
        m["country"] = countries.get(sid)
    gt = src / "ground_truth_events.csv"
    if gt.exists():
        shutil.copy(gt, out / "ground_truth_events.csv")
        shutil.copy(src / "ground_truth_trends.csv", out / "ground_truth_trends.csv")
        manifest["ground_truth"] = "ground_truth_events.csv"
    with open(out / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)
    pd.DataFrame(qrows).sort_values("station_id").to_csv(out / "data_quality_report.csv", index=False)
    dt = time.perf_counter() - t0
    print(f"Done: {manifest['total_rows']:,} rows, {manifest['total_bytes'] / 2**20:.1f} MiB parquet in {dt:.1f} s")


if __name__ == "__main__":
    main()
