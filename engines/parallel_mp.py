"""Shared-memory parallel engine: multiprocessing.Pool over station partitions.

Each task runs the full P2-P6 pipeline for one station and returns small partial results; the
main process merges them (reduce).

Scheduling:
  static  - pool.map with chunksize = ceil(n / p): each worker gets one contiguous block up front
  dynamic - imap_unordered with chunksize = 1: idle workers pull the next station (self-balancing);
            with --order lpt the biggest stations are queued first (LPT heuristic)

    python -m engines.parallel_mp --data data/parquet/synthetic --workers 8 --out results/
"""
from __future__ import annotations

import math
import multiprocessing as mp
import time
from functools import partial

from core.outputs import combine_station_results, write_outputs
from core.pipeline import process_station, read_manifest
from core.timing import PhaseTimer, max_per_phase
from engines.common import base_parser, setup


def run(data, cfg, workers: int, schedule: str = "dynamic", order: str = "lpt", stations=None, chunksize=None):
    t0 = time.perf_counter()
    manifest = read_manifest(data)
    ids = stations or sorted(manifest["stations"])
    if order == "lpt":
        ids = sorted(ids, key=lambda s: -manifest["stations"][s]["rows"])
    fn = partial(process_station, str(data), cfg=cfg)
    ctx = mp.get_context("spawn")
    with ctx.Pool(workers) as pool:
        if schedule == "static":
            cs = chunksize or math.ceil(len(ids) / workers)
            results = pool.map(fn, ids, chunksize=cs)
        else:
            results = list(pool.imap_unordered(fn, ids, chunksize=chunksize or 1))
    t_pool = time.perf_counter()
    timer = PhaseTimer()
    with timer.phase("merge"):
        combined = combine_station_results(results)
    # per-worker phase sums -> the slowest worker defines each phase
    per_pid: dict[int, dict] = {}
    for r in results:
        acc = per_pid.setdefault(r["pid"], {})
        for k, v in r["timings"].items():
            acc[k] = acc.get(k, 0.0) + v
    timings = max_per_phase(list(per_pid.values()))
    timings["merge"] = timer.times["merge"]
    per_worker = {str(pid): {k: round(v, 4) for k, v in t.items()} for pid, t in per_pid.items()}
    return combined, timings, time.perf_counter() - t0, per_worker, t_pool - t0


def main(argv=None):
    ap = base_parser(__doc__)
    ap.add_argument("--workers", "-p", type=int, default=mp.cpu_count())
    ap.add_argument("--schedule", choices=["static", "dynamic"], default="dynamic")
    ap.add_argument("--order", choices=["lpt", "name"], default="lpt")
    ap.add_argument("--chunksize", type=int)
    args = ap.parse_args(argv)
    cfg, data, out = setup(args)
    combined, timings, total, per_worker, _ = run(data, cfg, args.workers, args.schedule, args.order,
                                                  args.stations, args.chunksize)
    run_info = {"engine": "multiprocessing", "p": args.workers, "nodes": 1, "partition_mode": "station",
                "schedule": f"{args.schedule}/{args.order}", "total_s": total, "tag": args.tag,
                "per_worker": per_worker}
    write_outputs(out, combined, cfg, run_info, timings, write_files=not args.no_write, verbose=not args.quiet)


if __name__ == "__main__":
    main()
