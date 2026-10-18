"""Replay stored hourly observations as a live feed (step 1 of the streaming extension).

Reads the stations' partitioned Parquet for a date window and writes it into a landing directory,
`step_hours` simulated hours per file, at `speed` simulated days per wall-clock second, as if weather
stations were reporting live. Spark Structured Streaming (streaming/detector.py) watches that
directory. A replay is needed because no free live feed exists for these stations (ERA5 is
published ~5 days late), and it makes demos repeatable (replay the May 2024 heatwave on demand).

Details that keep the stream identical to the batch input:
  * files are written under a hidden name and renamed into place, so Spark never reads a partial file;
  * rows keep their stored order within a timestamp (the batch dedup keeps the first duplicate);
  * after the last step an end-of-stream marker (one empty reading per station, a day after the
    window) moves the event-time watermark past the last day so that day is closed and analysed;
  * _replay_log.csv records when each simulated hour became available (for detection latency) and
    _DONE marks the end of the replay. Spark ignores files starting with "_" or ".".

    python -m streaming.replayer --data data/parquet/real --start 2024-05-01 --end 2024-06-30 --speed 2
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.cleaning import HOURLY_VARS  # noqa: E402
from core.config import load_config, resolve  # noqa: E402
from core.pipeline import read_manifest, station_path  # noqa: E402

SCHEMA = pa.schema([("station_id", pa.string()), ("timestamp", pa.timestamp("us", tz="UTC"))]
                   + [(v, pa.float64()) for v in HOURLY_VARS])


def load_window(data: Path, stations: list[str], start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    lo, hi = start.tz_localize("UTC"), (end + pd.Timedelta(days=1)).tz_localize("UTC")
    parts = []
    for sid in stations:
        dset = ds.dataset(station_path(data, sid), format="parquet")
        t = dset.to_table(columns=["timestamp", *HOURLY_VARS],
                          filter=(ds.field("timestamp") >= lo) & (ds.field("timestamp") < hi))
        df = t.to_pandas()
        df.insert(0, "station_id", sid)
        # stable: duplicates keep their stored order, exactly like core.cleaning.clean_hourly
        parts.append(df.sort_values("timestamp", kind="stable"))
    out = pd.concat(parts, ignore_index=True)
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
    for v in HOURLY_VARS:
        out[v] = out[v].astype(np.float64)
    return out


def write_atomic(df: pd.DataFrame, landing: Path, name: str) -> None:
    tmp = landing / f".{name}.tmp"
    pq.write_table(pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False), tmp)
    os.replace(tmp, landing / name)


def replay(data: Path, landing: Path, stations: list[str], start: str, end: str, step_hours: int,
           speed: float, clean: bool = True, flush: bool = True, quiet: bool = False) -> dict:
    s, e = pd.Timestamp(start), pd.Timestamp(end)
    if clean and landing.exists():
        shutil.rmtree(landing)
    landing.mkdir(parents=True, exist_ok=True)
    df = load_window(data, stations, s, e)
    t0_sim = s.tz_localize("UTC")
    steps = int(np.ceil(((e + pd.Timedelta(days=1)) - s) / pd.Timedelta(hours=step_hours)))
    interval = 0.0 if speed <= 0 else step_hours / 24.0 / speed
    hours = ((df["timestamp"] - t0_sim) // pd.Timedelta(hours=step_hours)).to_numpy()
    order = np.argsort(hours, kind="stable")
    df, hours = df.iloc[order].reset_index(drop=True), hours[order]
    bounds = np.searchsorted(hours, np.arange(steps + 1))
    (landing / "_replay.json").write_text(json.dumps({
        "data": str(data), "stations": stations, "start": start, "end": end, "step_hours": step_hours,
        "speed_days_per_second": speed, "rows": int(len(df)), "steps": steps, "started": time.time()}))
    log = open(landing / "_replay_log.csv", "w")
    log.write("step,file,rows,sim_until,wall\n")
    if not quiet:
        print(f"replaying {len(df):,} hourly rows from {len(stations)} stations, {start} .. {end}: "
              f"{steps} files of {step_hours} h, {speed:g} sim-days/s -> {landing}", flush=True)
    t_start = time.perf_counter()
    for k in range(steps):
        chunk = df.iloc[bounds[k]:bounds[k + 1]]
        name = f"part-{k:06d}.parquet"
        write_atomic(chunk, landing, name)
        sim_until = t0_sim + pd.Timedelta(hours=step_hours * (k + 1))
        log.write(f"{k},{name},{len(chunk)},{sim_until.isoformat()},{time.time():.3f}\n")
        log.flush()
        if not quiet and (k % max(1, int(24 / step_hours) * 5) == 0 or k == steps - 1):
            print(f"  sim time {sim_until:%Y-%m-%d %H:%M}  ({k + 1}/{steps} files)", flush=True)
        if interval:
            delay = t_start + interval * (k + 1) - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
    if flush:
        # one empty reading per station, a day after the window: advances the watermark so the last
        # replayed day is finalized. Its own day never closes, so it never produces output.
        ts = (e + pd.Timedelta(days=2, hours=3)).tz_localize("UTC")
        marker = pd.DataFrame({"station_id": stations, "timestamp": ts, **{v: np.nan for v in HOURLY_VARS}})
        write_atomic(marker, landing, f"part-{steps:06d}-eos.parquet")
    log.close()
    (landing / "_DONE").write_text(json.dumps({"end": end, "finished": time.time(), "rows": int(len(df))}))
    took = time.perf_counter() - t_start
    if not quiet:
        print(f"replay finished in {took:.1f} s ({len(df) / max(took, 1e-9):,.0f} rows/s)", flush=True)
    return {"rows": int(len(df)), "steps": steps, "seconds": took}


def main(argv=None):
    cfg = load_config()
    sc = cfg["streaming"]
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=cfg["paths"]["parquet"])
    ap.add_argument("--stations", nargs="*", help="station ids (default: all in the manifest)")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--landing", default=sc["landing"])
    ap.add_argument("--step-hours", type=int, default=sc["step_hours"])
    ap.add_argument("--speed", type=float, default=sc["speed_days_per_second"],
                    help="simulated days per second (0 = as fast as possible)")
    ap.add_argument("--keep", action="store_true", help="do not clear the landing directory first")
    ap.add_argument("--no-flush", action="store_true", help="no end-of-stream marker")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args(argv)
    data = resolve(a.data)
    stations = a.stations or sorted(read_manifest(data)["stations"])
    replay(data, resolve(a.landing), stations, a.start, a.end, a.step_hours, a.speed,
           clean=not a.keep, flush=not a.no_flush, quiet=a.quiet)


if __name__ == "__main__":
    main()
