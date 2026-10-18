"""Distributed engine using MPI (mpi4py). Three partitioning modes:

  station  (default)  each rank owns whole stations (LPT bin-packing by row count). No boundary
                      problems, almost no communication: scatter -> local work -> Reduce/gather.
  time                each rank owns a contiguous block of years for ALL stations. Needs
                        * Gather + Chan merge of partial stats (month / overall levels),
                        * Allreduce of station-month moments (custom op) and histograms (SUM),
                        * halo (ghost-row) exchange with Sendrecv so multi-day events that cross a
                          chunk boundary are detected exactly once.
  dynamic             master-worker: rank 0 hands out one station at a time on request
                      (send/recv with tags) - self-balancing on heterogeneous machines.

MPI features: bcast, scatter, gather, Reduce/Allreduce with a custom MPI.Op (Chan's parallel
variance merge) on a derived contiguous datatype, Allreduce(SUM) on NumPy buffers, sendrecv,
point-to-point send/recv with tags, Wtime + Barrier for timing.

    mpirun -np 8 python -m engines.mpi_engine --data data/parquet/synthetic --mode station
    mpirun --hostfile docker/hostfile -np 8 python -m engines.mpi_engine --mode time
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from mpi4py import MPI

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.aggregates import STAT_VARS, VEC_WIDTH, empty_vec, frame_to_vec, merge_stats_frames, merge_vec, station_stats  # noqa: E402
from core.cleaning import clean_hourly, empty_quality, merge_quality, to_daily  # noqa: E402
from core.detection import EVENT_COLS, build_rules, detect_events, filter_owned_events  # noqa: E402
from core.outputs import combine_station_results, merge_combined, write_outputs  # noqa: E402
from core.pipeline import assign, load_station, process_station, read_manifest, year_chunks  # noqa: E402
from core.thresholds import attach_thresholds, climatology_arrays, empty_arrays, thresholds_from_arrays  # noqa: E402
from core.timing import PhaseTimer, max_per_phase  # noqa: E402
from core.trends import INDEX_COLS, TREND_COLS, station_trends, yearly_indices  # noqa: E402
from engines.common import base_parser, setup  # noqa: E402

TAG_READY, TAG_WORK, TAG_RESULT, TAG_STOP = 1, 2, 3, 4


# ----------------------------------------------------------------------------- custom reduction

def _merge_stats_op(inbuf, inoutbuf, datatype):
    a = np.frombuffer(inbuf, dtype=np.float64).reshape(-1, VEC_WIDTH)
    b = np.frombuffer(inoutbuf, dtype=np.float64).reshape(-1, VEC_WIDTH)
    b[:] = merge_vec(a, b)


MERGE_STATS = MPI.Op.Create(_merge_stats_op, commute=True)
# One MPI element = one 7-double partial-stat record, so MPI never splits a record when it
# segments the buffer during the reduction.
STAT_RECORD = MPI.DOUBLE.Create_contiguous(VEC_WIDTH).Commit()


def reduce_stats(comm, vec: np.ndarray, root: int | None = 0) -> np.ndarray:
    """Reduce (root given) or Allreduce (root=None) a (k, 7) partial-stats array with Chan's merge."""
    send = np.ascontiguousarray(vec, dtype=np.float64).reshape(-1, VEC_WIDTH)
    recv = np.empty_like(send)
    n = send.shape[0]
    if root is None:
        comm.Allreduce([send, n, STAT_RECORD], [recv, n, STAT_RECORD], op=MERGE_STATS)
    else:
        comm.Reduce([send, n, STAT_RECORD], [recv, n, STAT_RECORD], op=MERGE_STATS, root=root)
    return recv


# ----------------------------------------------------------------------------- station mode

def run_station(comm, data, cfg, balance="lpt", stations=None):
    rank, size = comm.Get_rank(), comm.Get_size()
    timer = PhaseTimer(clock=MPI.Wtime)
    with timer.phase("comm"):
        if rank == 0:
            manifest = read_manifest(data)
            assignment = assign(manifest, size, balance, stations)
        else:
            assignment = None
        cfg = comm.bcast(cfg, root=0)
        local = comm.scatter(assignment, root=0)
    results = [process_station(data, sid, cfg, timer) for sid in local]
    with timer.phase("merge"):
        part = combine_station_results(results)
    with timer.phase("comm"):
        gvec = reduce_stats(comm, part["global_vec"], root=0)
        parts = comm.gather({k: v for k, v in part.items() if k != "global_vec"} | {"global_vec": empty_vec(len(STAT_VARS))},
                            root=0)
        loads = comm.gather({"stations": local, "rows": part["rows"]}, root=0)
    combined = merge_combined(parts, global_vec=gvec) if rank == 0 else None
    return combined, timer, loads


# ----------------------------------------------------------------------------- dynamic mode

def run_dynamic(comm, data, cfg, stations=None):
    rank, size = comm.Get_rank(), comm.Get_size()
    timer = PhaseTimer(clock=MPI.Wtime)
    if size == 1:
        return run_station(comm, data, cfg, "lpt", stations)
    status = MPI.Status()
    loads = None
    if rank == 0:
        manifest = read_manifest(data)
        ids = stations or sorted(manifest["stations"])
        queue = sorted(ids, key=lambda s: -manifest["stations"][s]["rows"])   # biggest first
        results, active, served = [], size - 1, {r: [] for r in range(1, size)}
        while active:
            msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            src = status.Get_source()
            if status.Get_tag() == TAG_RESULT:
                results.append(msg)
            if queue:
                sid = queue.pop(0)
                served[src].append(sid)
                comm.send(sid, dest=src, tag=TAG_WORK)
            else:
                comm.send(None, dest=src, tag=TAG_STOP)
                active -= 1
        with timer.phase("merge"):
            combined = combine_station_results(results)
        loads = [{"stations": [], "rows": 0}] + [
            {"stations": served[r], "rows": int(sum(manifest["stations"][s]["rows"] for s in served[r]))}
            for r in range(1, size)]
    else:
        comm.send(None, dest=0, tag=TAG_READY)
        while True:
            sid = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            if status.Get_tag() == TAG_STOP:
                break
            comm.send(process_station(data, sid, cfg, timer), dest=0, tag=TAG_RESULT)
        combined = None
    return combined, timer, loads


# ----------------------------------------------------------------------------- time mode

def run_time(comm, data, cfg, stations=None):
    rank, size = comm.Get_rank(), comm.Get_size()
    timer = PhaseTimer(clock=MPI.Wtime)
    with timer.phase("comm"):
        manifest = comm.bcast(read_manifest(data) if rank == 0 else None, root=0)
        cfg = comm.bcast(cfg, root=0)
    ids = stations or sorted(manifest["stations"])
    S = len(ids)
    y0, y1 = manifest["years"]
    chunks = year_chunks(y0, y1, size)
    mine = chunks[rank]
    active = [i for i, c in enumerate(chunks) if c]
    is_first, is_last = rank == active[0], rank == active[-1]
    rules = build_rules(cfg)
    rule_map = {r.event_type: r for r in rules}

    # P1 load (row-group pruning by year)
    with timer.phase("load"):
        raw = {s: load_station(data, s, mine) for s in ids} if mine else {}
    # P2 clean + daily
    with timer.phase("clean"):
        hourly, daily, quality = {}, {}, empty_quality()
        for s, df in raw.items():
            if df.empty:          # station has no records in this rank's years (shorter history)
                continue
            hourly[s], q = clean_hourly(df, cfg)
            quality = merge_quality(quality, q)
            daily[s] = to_daily(hourly[s], cfg)
        rows = sum(len(df) for df in raw.values())
        del raw
    # P3 partial stats -> Reduce (global, custom op) + Gather (per station, merged with Chan at root)
    with timer.phase("stats"):
        frames = [station_stats(hourly[s], daily[s], s) for s in ids if s in hourly]
        part_stats = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        lvec = empty_vec(len(STAT_VARS))
        for f in frames:
            lvec = merge_vec(lvec, frame_to_vec(f[f["level"] == "overall"], STAT_VARS))
    del hourly
    with timer.phase("comm"):
        gvec = reduce_stats(comm, lvec, root=0)
        all_stats = comm.gather(part_stats, root=0)
    # P4 climatology: local moments + histograms -> Allreduce -> identical thresholds on every rank
    with timer.phase("thresholds"):
        mom, hist = empty_arrays(S, cfg)
        for i, s in enumerate(ids):
            if s in daily:
                climatology_arrays(daily[s], cfg, S, i, mom, hist)
    with timer.phase("comm"):
        mom_g = reduce_stats(comm, mom.reshape(-1, VEC_WIDTH), root=None).reshape(mom.shape)
        hist_g = {}
        for v, h in hist.items():
            hist_g[v] = np.empty_like(h)
            comm.Allreduce(h, hist_g[v], op=MPI.SUM)
    with timer.phase("thresholds"):
        thr = thresholds_from_arrays(mom_g, hist_g, ids, cfg)
        thr_by = {s: g for s, g in thr.groupby("station_id")}
    # P5 detection with halo exchange: receive the previous rank's last H days, send ours onward
    H = cfg["mpi"]["halo_days"]
    with timer.phase("comm"):
        nxt = rank + 1 if (mine and rank + 1 <= active[-1]) else MPI.PROC_NULL
        prv = rank - 1 if (mine and rank > active[0]) else MPI.PROC_NULL
        tails = {s: d.tail(H) for s, d in daily.items()}
        halo = comm.sendrecv(tails, dest=nxt, source=prv) or {}
    with timer.phase("detect"):
        ev = []
        if mine:
            own_start, own_end = pd.Timestamp(f"{mine[0]}-01-01"), pd.Timestamp(f"{mine[1]}-12-31")
            for s, d in daily.items():
                frame = pd.concat([halo[s], d], ignore_index=True) if s in halo else d
                e = detect_events(attach_thresholds(frame, thr_by[s]), s, rules)
                ev.append(filter_owned_events(e, own_start, own_end, is_first, is_last))
        events = pd.concat(ev, ignore_index=True) if ev else pd.DataFrame(columns=EVENT_COLS)
    # P6 yearly indices locally (year-aligned chunks), trends at the root after gathering
    with timer.phase("trends"):
        ind = [yearly_indices(attach_thresholds(d, thr_by[s]), s, rule_map, cfg) for s, d in daily.items()]
        indices = pd.concat(ind, ignore_index=True) if ind else pd.DataFrame(columns=INDEX_COLS)
    with timer.phase("comm"):
        g_events = comm.gather(events, root=0)
        g_indices = comm.gather(indices, root=0)
        g_quality = comm.gather(quality, root=0)
        g_rows = comm.gather(rows, root=0)
        loads = comm.gather({"stations": ids if mine else [], "rows": rows, "years": mine}, root=0)

    if rank != 0:
        return None, timer, None
    with timer.phase("merge"):
        st = pd.concat([f for f in all_stats if len(f)], ignore_index=True)
        stats = merge_stats_frames(st, ["station_id", "level", "period", "variable"])
        events = pd.concat([e for e in g_events if len(e)], ignore_index=True)
        indices = pd.concat([i for i in g_indices if len(i)], ignore_index=True).sort_values(["station_id", "year"])
        q = empty_quality()
        for x in g_quality:
            q = merge_quality(q, x)
    with timer.phase("trends"):
        tr = [station_trends(g, cfg) for _, g in indices.groupby("station_id")]
        trends = pd.concat(tr, ignore_index=True) if tr else pd.DataFrame(columns=TREND_COLS)
    combined = {"stats": stats, "thresholds": thr, "events": events[EVENT_COLS], "indices": indices,
                "trends": trends, "global_vec": gvec, "quality": q, "quality_rows": pd.DataFrame(),
                "precip_hist": hist_g["precip_wet"].sum(axis=(0, 1)), "rows": int(sum(g_rows)), "stations": S}
    return combined, timer, loads


# ----------------------------------------------------------------------------- main

def main(argv=None):
    ap = base_parser(__doc__)
    ap.add_argument("--mode", choices=["station", "time", "dynamic"], default="station")
    ap.add_argument("--balance", choices=["lpt", "round_robin", "block"], default="lpt",
                    help="static assignment strategy for station mode")
    args = ap.parse_args(argv)
    comm = MPI.COMM_WORLD
    rank, size = comm.Get_rank(), comm.Get_size()
    cfg, data, out = setup(args)

    comm.Barrier()
    t0 = MPI.Wtime()
    if args.mode == "station":
        combined, timer, loads = run_station(comm, data, cfg, args.balance, args.stations)
    elif args.mode == "dynamic":
        combined, timer, loads = run_dynamic(comm, data, cfg, args.stations)
    else:
        combined, timer, loads = run_time(comm, data, cfg, args.stations)
    timings = comm.gather(timer.to_dict(), root=0)
    hosts = comm.gather(MPI.Get_processor_name(), root=0)
    total = MPI.Wtime() - t0

    if rank == 0:
        per_rank = {str(r): {**{k: round(v, 4) for k, v in t.items()}, "host": hosts[r],
                             **({"rows": loads[r]["rows"], "n_stations": len(loads[r]["stations"])} if loads else {})}
                    for r, t in enumerate(timings)}
        tmax = max_per_phase(timings)
        run_info = {"engine": "mpi", "p": size, "nodes": len(set(hosts)), "partition_mode": args.mode,
                    "schedule": args.balance if args.mode == "station" else args.mode, "total_s": total,
                    "tag": args.tag, "per_worker": per_rank}
        write_outputs(out, combined, cfg, run_info, tmax, write_files=not args.no_write, verbose=not args.quiet)
        if loads and not args.quiet:
            rows = [l["rows"] for l in loads]
            busy = rows[1:] if args.mode == "dynamic" else rows
            if busy and max(busy):
                print(f"Load balance          : rows per rank min {min(busy):,} / max {max(busy):,} "
                      f"(imbalance {max(busy) / (sum(busy) / len(busy)):.3f})")


if __name__ == "__main__":
    main()
