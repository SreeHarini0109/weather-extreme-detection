"""Compare two result directories (engine parity). Usable from pytest and from the command line:

    python -m tests.parity results/sequential_latest results/mpi_latest
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

RTOL, ATOL = 1e-6, 1e-6


def _close(a: pd.Series, b: pd.Series, rtol=RTOL, atol=ATOL) -> np.ndarray:
    a = pd.to_numeric(a, errors="coerce").to_numpy(dtype=float)
    b = pd.to_numeric(b, errors="coerce").to_numpy(dtype=float)
    return np.isclose(a, b, rtol=rtol, atol=atol, equal_nan=True)


def _cmp_table(name, a, b, keys, num_cols, exact_cols=(), rtol=RTOL, atol=ATOL) -> list[str]:
    errs = []
    ka = set(map(tuple, a[keys].astype(str).to_numpy()))
    kb = set(map(tuple, b[keys].astype(str).to_numpy()))
    if ka != kb:
        only_a, only_b = sorted(ka - kb)[:5], sorted(kb - ka)[:5]
        errs.append(f"{name}: key sets differ ({len(ka - kb)} only in A e.g. {only_a}; "
                    f"{len(kb - ka)} only in B e.g. {only_b})")
    a2 = a.assign(**{k: a[k].astype(str) for k in keys})
    b2 = b.assign(**{k: b[k].astype(str) for k in keys})
    m = a2.merge(b2, on=keys, suffixes=("_a", "_b"))
    for c in num_cols:
        bad = ~_close(m[f"{c}_a"], m[f"{c}_b"], rtol, atol)
        if bad.any():
            ex = m.loc[bad, [*keys, f"{c}_a", f"{c}_b"]].head(3).to_dict("records")
            errs.append(f"{name}.{c}: {int(bad.sum())} mismatches, e.g. {ex}")
    for c in exact_cols:
        bad = m[f"{c}_a"].astype(str) != m[f"{c}_b"].astype(str)
        if bad.any():
            ex = m.loc[bad, [*keys, f"{c}_a", f"{c}_b"]].head(3).to_dict("records")
            errs.append(f"{name}.{c}: {int(bad.sum())} mismatches, e.g. {ex}")
    return errs


def compare_runs(dir_a, dir_b, check_stats_times: bool = True) -> list[str]:
    A, B = Path(dir_a), Path(dir_b)
    rd = lambda d, f: pd.read_csv(d / f)
    errs = []
    sa, sb = rd(A, "summary_stats.csv"), rd(B, "summary_stats.csv")
    errs += _cmp_table("summary_stats", sa, sb, ["station_id", "level", "period", "variable"],
                       ["count", "mean", "std", "min", "max"],
                       ["min_time", "max_time"] if check_stats_times else [])
    ea, eb = rd(A, "extreme_events.csv"), rd(B, "extreme_events.csv")
    errs += _cmp_table("extreme_events", ea, eb, ["event_id"], ["duration_days", "peak_value"],
                       ["method", "severity", "end_date", "peak_date"])
    ta, tb = rd(A, "thresholds.csv"), rd(B, "thresholds.csv")
    num = [c for c in ta.columns if c not in ("station_id", "month")]
    errs += _cmp_table("thresholds", ta, tb, ["station_id", "month"], num)
    ia, ib = rd(A, "climate_indices.csv"), rd(B, "climate_indices.csv")
    errs += _cmp_table("climate_indices", ia, ib, ["station_id", "year"],
                       [c for c in ia.columns if c not in ("station_id", "year")])
    tra, trb = rd(A, "trends.csv"), rd(B, "trends.csv")
    errs += _cmp_table("trends", tra, trb, ["station_id", "variable"],
                       ["ols_slope_per_decade", "sen_slope_per_decade", "mk_p", "mk_z"], ["significant"])
    ga = json.loads((A / "global_records.json").read_text())
    gb = json.loads((B / "global_records.json").read_text())
    for k in ga:
        if k not in gb or ga[k]["value"] != gb[k]["value"] or ga[k]["station_id"] != gb[k]["station_id"]:
            errs.append(f"global_records.{k}: {ga[k]} != {gb.get(k)}")
    return errs


def main():
    a, b = sys.argv[1], sys.argv[2]
    errs = compare_runs(a, b)
    if errs:
        print(f"PARITY FAILED ({len(errs)} issues):")
        for e in errs:
            print(" -", e)
        sys.exit(1)
    print(f"PARITY OK: {a} == {b}")


if __name__ == "__main__":
    main()
