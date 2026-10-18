"""Sequential baseline: a plain loop over stations (reference time T1 for speedup).

    python -m engines.sequential --data data/parquet/synthetic --out results/
"""
from __future__ import annotations

import time

from core.outputs import combine_station_results, write_outputs
from core.pipeline import process_station, read_manifest
from core.timing import PhaseTimer
from engines.common import base_parser, setup


def run(data, cfg, stations=None):
    t0 = time.perf_counter()
    manifest = read_manifest(data)
    ids = stations or sorted(manifest["stations"])
    timer = PhaseTimer()
    results = [process_station(data, sid, cfg, timer) for sid in ids]
    with timer.phase("merge"):
        combined = combine_station_results(results)
    return combined, timer.to_dict(), time.perf_counter() - t0


def main(argv=None):
    args = base_parser(__doc__).parse_args(argv)
    cfg, data, out = setup(args)
    combined, timings, total = run(data, cfg, args.stations)
    run_info = {"engine": "sequential", "p": 1, "nodes": 1, "partition_mode": "", "schedule": "",
                "total_s": total, "tag": args.tag}
    write_outputs(out, combined, cfg, run_info, timings, write_files=not args.no_write, verbose=not args.quiet)


if __name__ == "__main__":
    main()
