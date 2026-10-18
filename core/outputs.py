"""Merge partial results and write the standard output files (identical layout for every engine)."""
from __future__ import annotations

import csv
import json
import os
import platform
import socket
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from .aggregates import STAT_VARS, empty_vec, finalize_stats, merge_vec, vec_to_frame
from .cleaning import empty_quality, merge_quality
from .detection import EVENT_COLS
from .thresholds import hist_spec
from .timing import PHASES
from .trends import INDEX_COLS, TREND_COLS

BENCH_COLS = ["run_id", "timestamp", "engine", "p", "nodes", "partition_mode", "schedule", "rows", "stations",
              *[f"t_{ph}" for ph in PHASES], "t_comm", "t_merge", "t_compute_total", "total_s", "write_s",
              "startup_s", "rows_per_s", "host", "tag"]


def combine_station_results(results: list[dict]) -> dict:
    """Combine per-station results (sequential / multiprocessing / MPI station & dynamic modes)."""
    results = sorted(results, key=lambda r: r["station_id"])
    gvec = empty_vec(len(STAT_VARS))
    q = empty_quality()
    qrows = []
    hist = None
    for r in results:
        gvec = merge_vec(gvec, r["global_vec"])
        q = merge_quality(q, r["quality"])
        qrows.append({"station_id": r["station_id"], **r["quality"]})
        hist = r["precip_hist"] if hist is None else hist + r["precip_hist"]
    cat = lambda key, cols: (pd.concat([r[key] for r in results], ignore_index=True)
                             if results else pd.DataFrame(columns=cols))
    return {
        "stats": cat("stats", None), "thresholds": cat("thresholds", None),
        "events": cat("events", EVENT_COLS), "indices": cat("indices", INDEX_COLS),
        "trends": cat("trends", TREND_COLS), "global_vec": gvec, "quality": q,
        "quality_rows": pd.DataFrame(qrows), "precip_hist": hist,
        "rows": int(sum(r["rows"] for r in results)), "stations": len(results),
    }


def merge_combined(parts: list[dict], global_vec=None) -> dict:
    """Merge several `combine_station_results` outputs (e.g. one per MPI rank) whose stations are disjoint."""
    parts = [p for p in parts if p["stations"] > 0] or parts[:1]
    out = {}
    for key in ("stats", "thresholds", "events", "indices", "trends", "quality_rows"):
        frames = [p[key] for p in parts if len(p[key])]
        out[key] = pd.concat(frames, ignore_index=True) if frames else parts[0][key]
    q = empty_quality()
    gvec = empty_vec(len(STAT_VARS))
    hist = None
    for p in parts:
        q = merge_quality(q, p["quality"])
        gvec = merge_vec(gvec, p["global_vec"])
        if p["precip_hist"] is not None:
            hist = p["precip_hist"] if hist is None else hist + p["precip_hist"]
    out.update(quality=q, precip_hist=hist, global_vec=gvec if global_vec is None else global_vec,
               rows=sum(p["rows"] for p in parts), stations=sum(p["stations"] for p in parts))
    return out


def global_records(stats: pd.DataFrame) -> dict:
    ov = stats[stats["level"] == "overall"]

    def pick(var, which, label, unit):
        sub = ov[(ov["variable"] == var) & (ov["count"] > 0)]
        if sub.empty:
            return None
        if which == "max":
            sub = sub.assign(_k=-sub["max"]).sort_values(["_k", "max_time", "station_id"])
            r = sub.iloc[0]
            val, when = r["max"], r["max_time"]
        else:
            sub = sub.sort_values(["min", "min_time", "station_id"])
            r = sub.iloc[0]
            val, when = r["min"], r["min_time"]
        return {"record": label, "value": round(float(val), 2), "unit": unit, "station_id": r["station_id"],
                "time": str(pd.Timestamp(when))}

    recs = [
        pick("temp_c", "max", "highest_temperature", "°C"),
        pick("temp_c", "min", "lowest_temperature", "°C"),
        pick("precip_sum", "max", "max_daily_rainfall", "mm/day"),
        pick("precip_mm", "max", "max_hourly_rainfall", "mm/h"),
        pick("wind_kmh", "max", "max_wind_speed", "km/h"),
        pick("gust_kmh", "max", "max_wind_gust", "km/h"),
        pick("humidity_pct", "max", "max_humidity", "%"),
        pick("humidity_pct", "min", "min_humidity", "%"),
    ]
    return {r["record"]: r for r in recs if r}


def extreme_summary(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=["station_id", "year", "event_type", "method", "severity", "events", "event_days"])
    e = events.assign(year=pd.to_datetime(events["start_date"]).dt.year)
    out = e.groupby(["station_id", "year", "event_type", "method", "severity"]).agg(
        events=("event_id", "size"), event_days=("duration_days", "sum")).reset_index()
    return out


def machine_info() -> dict:
    info = {"host": socket.gethostname(), "platform": platform.platform(), "python": platform.python_version(),
            "cpu_count": os.cpu_count(), "processor": platform.processor()}
    try:
        info["ram_gb"] = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1)
    except (ValueError, OSError, AttributeError):
        pass
    return info


def write_outputs(out_root, combined: dict, cfg: dict, run: dict, timings: dict, write_files: bool = True,
                  verbose: bool = True) -> Path:
    """Write all result files to results/<engine>_p<p>[_mode]_<timestamp>/ and append a benchmark row.

    run: engine, p, nodes, partition_mode, schedule, total_s (wall time excluding writing), tag.
    """
    import time as _time
    t0 = _time.perf_counter()
    out_root = Path(out_root)
    stamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    mode = f"_{run['partition_mode']}" if run.get("partition_mode") not in (None, "", "none") else ""
    name = f"{run['engine']}_p{run['p']}{mode}_{stamp}"
    run_dir = out_root / name
    run_id = name

    gframe = vec_to_frame(combined["global_vec"], STAT_VARS)
    gframe.insert(0, "station_id", "ALL")
    gframe.insert(1, "level", "global")
    gframe.insert(2, "period", 0)
    stats_all = pd.concat([gframe, combined["stats"]], ignore_index=True)
    summary = finalize_stats(stats_all)
    records = global_records(combined["stats"])
    events = combined["events"].sort_values(["start_date", "station_id", "event_type"]).reset_index(drop=True)
    ex_sum = extreme_summary(events)
    trends = combined["trends"].sort_values(["station_id", "variable"]).reset_index(drop=True)
    indices = combined["indices"].sort_values(["station_id", "year"]).reset_index(drop=True)

    if write_files:
        run_dir.mkdir(parents=True, exist_ok=True)
        summary.to_csv(run_dir / "summary_stats.csv", index=False)
        with open(run_dir / "global_records.json", "w") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        events.to_csv(run_dir / "extreme_events.csv", index=False)
        ex_sum.to_csv(run_dir / "extreme_summary.csv", index=False)
        trends.to_csv(run_dir / "trends.csv", index=False)
        indices.to_csv(run_dir / "climate_indices.csv", index=False)
        combined["thresholds"].sort_values(["station_id", "month"]).to_csv(run_dir / "thresholds.csv", index=False)
        if len(combined.get("quality_rows", [])):
            combined["quality_rows"].sort_values("station_id").to_csv(run_dir / "data_quality.csv", index=False)
        if combined.get("precip_hist") is not None:
            lo, w, nb = hist_spec(cfg, "precip_wet")
            h = combined["precip_hist"]
            nz = np.flatnonzero(h)
            pd.DataFrame({"bin_lo_mm": lo + nz * w, "bin_hi_mm": lo + (nz + 1) * w, "wet_days": h[nz]}).to_csv(
                run_dir / "precip_histogram.csv", index=False)
        with open(run_dir / "run_config.yaml", "w") as f:
            yaml.safe_dump({"run": {k: v for k, v in run.items()}, "machine": machine_info(), "config": cfg}, f,
                           sort_keys=False)
        latest = out_root / f"{run['engine']}_latest"
        try:
            if latest.is_symlink() or latest.exists():
                latest.unlink()
            latest.symlink_to(run_dir.name)
        except OSError:
            pass
    write_s = _time.perf_counter() - t0

    phase_t = {ph: float(timings.get(ph, 0.0)) for ph in PHASES}
    tim = {"phases": phase_t, "comm": float(timings.get("comm", 0.0)), "merge": float(timings.get("merge", 0.0)),
           "total_s": run["total_s"], "write_s": write_s, "per_worker": run.get("per_worker")}
    if write_files:
        with open(run_dir / "timings.json", "w") as f:
            json.dump(tim, f, indent=2, default=float)

    bench = {
        "run_id": run_id, "timestamp": stamp, "engine": run["engine"], "p": run["p"], "nodes": run.get("nodes", 1),
        "partition_mode": run.get("partition_mode", ""), "schedule": run.get("schedule", ""),
        "rows": combined["rows"], "stations": combined["stations"],
        **{f"t_{ph}": round(v, 4) for ph, v in phase_t.items()},
        "t_comm": round(tim["comm"], 4), "t_merge": round(tim["merge"], 4),
        "t_compute_total": round(sum(phase_t.values()), 4),
        "total_s": round(run["total_s"], 4), "write_s": round(write_s, 4),
        "startup_s": round(run["startup_s"], 4) if run.get("startup_s") is not None else "",
        "rows_per_s": round(combined["rows"] / run["total_s"], 1) if run["total_s"] else 0,
        "host": socket.gethostname(), "tag": run.get("tag", ""),
    }
    append_benchmark(out_root / "benchmark.csv", bench)

    if verbose:
        print_summary(run, combined, records, events, trends, tim, run_dir if write_files else None)
    return run_dir


def append_benchmark(path: Path, row: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=BENCH_COLS)
        if new:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in BENCH_COLS})


def print_summary(run, combined, records, events, trends, tim, run_dir):
    mode = f", mode={run['partition_mode']}" if run.get("partition_mode") else ""
    print(f"\n=== Weather Analysis Complete (engine={run['engine']}, p={run['p']}{mode}) ===")
    idx = combined["indices"]
    yrs = f"{int(idx['year'].min())}–{int(idx['year'].max())}" if len(idx) else "n/a"
    print(f"Rows processed        : {combined['rows']:,}   ({combined['stations']} stations, {yrs})")

    def rec(key, label):
        r = records.get(key)
        if r:
            print(f"{label:<22}: {r['value']} {r['unit']} @ {r['station_id']}  {r['time'][:10]}")

    rec("highest_temperature", "Hottest temperature")
    rec("lowest_temperature", "Coldest temperature")
    rec("max_daily_rainfall", "Max 24h rainfall")
    rec("max_wind_gust", "Max wind gust")
    if len(events):
        th = events[events["method"] == "threshold"]["event_type"].value_counts()
        parts = " | ".join(f"{k} {v:,}" for k, v in th.items())
        print(f"Extreme events found  : {len(events):,}  (threshold: {parts})")
    tm = trends[trends["variable"] == "tmax_mean"]
    if len(tm):
        warm = tm[(tm["significant"]) & (tm["mk_z"] > 0)]
        print(f"Stations warming (MK) : {len(warm)} / {len(tm)} significant, "
              f"median {tm['sen_slope_per_decade'].median():+.2f} °C/decade (Sen)")
    ph = tim["phases"]
    br = " | ".join(f"{k} {v:.2f}" for k, v in ph.items())
    extra = f" | comm {tim['comm']:.2f}" if tim["comm"] else ""
    print(f"Total time            : {tim['total_s']:.2f} s   ({br}{extra})")
    if run_dir:
        print(f"Results               : {run_dir}")
