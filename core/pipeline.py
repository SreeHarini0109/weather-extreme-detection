"""The six-phase pipeline (P1 load ... P6 trends) for one station, plus partitioning helpers.

`process_station` is the unit of work for the sequential, multiprocessing, MPI-station and
MPI-dynamic engines. The MPI time-partitioned mode and Spark reuse the individual phase functions.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .aggregates import STAT_VARS, frame_to_vec, station_stats
from .cleaning import clean_hourly, to_daily
from .detection import build_rules, detect_events
from .thresholds import attach_thresholds, climatology_arrays, thresholds_from_arrays
from .timing import PhaseTimer
from .trends import station_trends, yearly_indices

LOAD_COLS = ["timestamp", "temp_c", "humidity_pct", "precip_mm", "wind_kmh", "gust_kmh"]


# ----------------------------------------------------------------------------- manifest / io

def read_manifest(data_dir: str | Path) -> dict:
    with open(Path(data_dir) / "manifest.json") as f:
        return json.load(f)


def station_meta(manifest: dict) -> dict:
    """{station_id: {"country", "elevation"}} for region-specific rules (IMD plains / hills)."""
    return {s: {"country": m.get("country"), "elevation": m.get("elevation")} for s, m in manifest["stations"].items()}


_META_CACHE: dict[str, dict] = {}


def cached_station_meta(data_dir) -> dict:
    key = str(data_dir)
    if key not in _META_CACHE:
        _META_CACHE[key] = station_meta(read_manifest(data_dir))
    return _META_CACHE[key]


def station_path(data_dir: str | Path, station_id: str) -> Path:
    return Path(data_dir) / f"station_id={station_id}"


def load_station(data_dir, station_id: str, year_range: tuple[int, int] | None = None) -> pd.DataFrame:
    """P1: read one station partition (column-pruned; optional year filter uses row-group stats)."""
    path = station_path(data_dir, station_id)
    if year_range is None:
        table = pq.read_table(path, columns=LOAD_COLS)
    else:
        y0, y1 = year_range
        lo = pd.Timestamp(f"{y0}-01-01", tz="UTC")
        hi = pd.Timestamp(f"{y1 + 1}-01-01", tz="UTC")
        dset = ds.dataset(path, format="parquet")
        f = (ds.field("timestamp") >= lo) & (ds.field("timestamp") < hi)
        table = dset.to_table(columns=LOAD_COLS, filter=f)
    df = table.to_pandas()
    ts = df["timestamp"]
    if getattr(ts.dt, "tz", None) is not None:
        df["timestamp"] = ts.dt.tz_convert("UTC").dt.tz_localize(None)
    return df


# ----------------------------------------------------------------------------- partitioning

def lpt_assign(sizes: dict[str, int], p: int) -> list[list[str]]:
    """Longest-Processing-Time greedy bin packing: biggest station to the least-loaded worker."""
    bins = [[] for _ in range(p)]
    load = np.zeros(p)
    for sid, n in sorted(sizes.items(), key=lambda kv: (-kv[1], kv[0])):
        i = int(np.argmin(load))
        bins[i].append(sid)
        load[i] += n
    return bins


def round_robin_assign(stations: list[str], p: int) -> list[list[str]]:
    return [sorted(stations)[i::p] for i in range(p)]


def block_assign(stations: list[str], p: int) -> list[list[str]]:
    return [list(a) for a in np.array_split(sorted(stations), p)]


def assign(manifest: dict, p: int, strategy: str = "lpt", stations: list[str] | None = None) -> list[list[str]]:
    st = manifest["stations"]
    ids = stations or sorted(st)
    if strategy == "lpt":
        return lpt_assign({s: st[s]["rows"] for s in ids}, p)
    if strategy == "round_robin":
        return round_robin_assign(ids, p)
    if strategy == "block":
        return block_assign(ids, p)
    raise ValueError(f"unknown strategy {strategy}")


def year_chunks(y0: int, y1: int, p: int) -> list[tuple[int, int] | None]:
    """Split [y0, y1] into p contiguous year ranges (None for idle ranks when p > n_years)."""
    years = np.arange(y0, y1 + 1)
    parts = np.array_split(years, min(p, years.size))
    out = [(int(a[0]), int(a[-1])) for a in parts]
    return out + [None] * (p - len(out))


# ----------------------------------------------------------------------------- per-station pipeline

def process_station(data_dir, station_id: str, cfg: dict, timer: PhaseTimer | None = None) -> dict:
    """Run P1-P6 for one station and return its (small) partial results."""
    t = timer or PhaseTimer()
    rules = build_rules(cfg)
    with t.phase("load"):
        raw = load_station(data_dir, station_id)
    with t.phase("clean"):
        hourly, quality = clean_hourly(raw, cfg)
        daily = to_daily(hourly, cfg)
    with t.phase("stats"):
        stats = station_stats(hourly, daily, station_id)
        gvec = frame_to_vec(stats[stats["level"] == "overall"], STAT_VARS)
    del raw, hourly
    with t.phase("thresholds"):
        mom, hist = climatology_arrays(daily, cfg)
        thr = thresholds_from_arrays(mom, hist, [station_id], cfg, cached_station_meta(data_dir))
        dthr = attach_thresholds(daily, thr)
    with t.phase("detect"):
        events = detect_events(dthr, station_id, rules)
    with t.phase("trends"):
        indices = yearly_indices(dthr, station_id, {r.event_type: r for r in rules}, cfg)
        trends = station_trends(indices, cfg)
    return {
        "station_id": station_id, "rows": quality["rows_in"], "quality": quality, "stats": stats,
        "global_vec": gvec, "thresholds": thr, "precip_hist": hist["precip_wet"].sum(axis=(0, 1)),
        "events": events, "indices": indices, "trends": trends, "timings": t.to_dict(), "pid": os.getpid(),
    }
