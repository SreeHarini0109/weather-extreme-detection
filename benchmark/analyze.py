"""Benchmark analysis: medians of repeated runs -> speedup, efficiency, Karp-Flatt, plots.

    python benchmark/analyze.py --input results/benchmark.csv --plots results/plots/

Metrics (T1 = median sequential time on the same data):
  S(p) = T1 / Tp      E(p) = S(p) / p      Karp-Flatt e = (1/S - 1/p) / (1 - 1/p)
For MPI dynamic mode, p counts all ranks (rank 0 only dispatches, so p-1 ranks compute).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.config import resolve  # noqa: E402
from core.timing import PHASES  # noqa: E402
from reporting import style  # noqa: E402
from reporting.style import ENGINE_COLORS, INK2, MUTED, SERIES, plt  # noqa: E402

PHASE_COLS = [f"t_{p}" for p in PHASES] + ["t_comm", "t_merge"]
PHASE_COLORS = dict(zip(PHASE_COLS, SERIES))
LINE_ORDER = ["multiprocessing", "mpi/station", "mpi/time", "mpi/dynamic", "spark"]


def performance_cores() -> int | None:
    """Apple silicon / hybrid CPUs: number of performance cores (macOS sysctl), else None."""
    import subprocess
    try:
        out = subprocess.run(["sysctl", "-n", "hw.perflevel0.physicalcpu", "hw.perflevel1.physicalcpu"],
                             capture_output=True, text=True, timeout=5).stdout.split()
        return int(out[0]) if len(out) == 2 else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def _pcore_line(ax, pcores):
    if pcores:
        ax.axvline(pcores, color=MUTED, lw=0.8, ls=":")
        ax.annotate(f"{pcores} performance cores | efficiency cores →", (pcores, 1), xycoords=("data", "axes fraction"),
                    xytext=(4, -4), textcoords="offset points", fontsize=7.5, color=MUTED, va="top")


PCORES = performance_cores()


def label(r) -> str:
    if r["engine"] == "mpi":
        return f"mpi/{r['partition_mode']}"
    return r["engine"]


def load(path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["label"] = df.apply(label, axis=1)
    df["schedule"] = df["schedule"].fillna("")
    keys = ["tag", "label", "schedule", "p", "rows", "stations"]
    num = ["total_s", "startup_s", *PHASE_COLS, "rows_per_s"]
    agg = df.groupby(keys)[num].median()
    agg["runs"] = df.groupby(keys).size()
    return agg.reset_index()


def scaling_table(m: pd.DataFrame, tag: str) -> pd.DataFrame:
    s = m[m["tag"] == tag].copy()
    if s.empty:
        return s
    if tag == "strong":
        seq = s.loc[s["label"] == "sequential", "total_s"]
        t1 = float(seq.iloc[0]) if len(seq) else np.nan
        s["speedup"] = t1 / s["total_s"]
        # self-relative: each engine's own p=1 run (MPI time / dynamic have no p=1 run)
        base = s[s["p"] == 1].set_index("label")["total_s"]
        s["self_speedup"] = s["label"].map(base) / s["total_s"]
    else:   # weak scaling: per-worker load constant -> efficiency = T(1)/T(p)
        base = s[s["p"] == 1].groupby("label")["total_s"].first()
        s["weak_efficiency"] = s["label"].map(base) / s["total_s"]
        return s.sort_values(["label", "p"])
    s["efficiency"] = s["speedup"] / s["p"]
    p = s["p"].astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        s["karp_flatt"] = np.where(p > 1, (1 / s["speedup"] - 1 / p) / (1 - 1 / p), np.nan)
    return s.sort_values(["label", "p"])


def amdahl_fit(st: pd.DataFrame, lab: str) -> float:
    """Least-squares serial fraction f for S(p) = 1 / (f + (1-f)/p)."""
    d = st[(st["label"] == lab) & (st["p"] > 1)]
    if d.empty:
        return np.nan
    fs = np.linspace(0, 0.5, 5001)
    err = [((1 / (f + (1 - f) / d["p"]) - d["speedup"]) ** 2).sum() for f in fs]
    return float(fs[int(np.argmin(err))])


def _lines(ax, st, ycol, labels, annotate_last=True):
    for lab in labels:
        d = st[st["label"] == lab].sort_values("p")
        if d.empty:
            continue
        ax.plot(d["p"], d[ycol], marker="o", color=ENGINE_COLORS[lab], label=lab, markersize=6,
                markeredgecolor=style.SURFACE, markeredgewidth=1.5)
        if annotate_last:
            r = d.iloc[-1]
            ax.annotate(f"{r[ycol]:.2f}", (r["p"], r[ycol]), xytext=(6, 0), textcoords="offset points",
                        va="center", fontsize=8, color=INK2)


def plot_all(m: pd.DataFrame, strong: pd.DataFrame, weak: pd.DataFrame, out: Path):
    style.apply()
    out.mkdir(parents=True, exist_ok=True)
    labels = [l for l in LINE_ORDER if l in set(strong["label"])]
    pmax = int(strong["p"].max()) if len(strong) else 8
    ps = np.arange(1, pmax + 1)

    # 1. speedup vs p
    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.plot(ps, ps, ls="--", lw=1.2, color=MUTED, label="ideal (linear)")
    _lines(ax, strong, "speedup", labels)
    ax.set(xlabel="processes / cores (p)", ylabel="speedup vs sequential  T₁ / Tₚ", xticks=sorted(strong["p"].unique()))
    _pcore_line(ax, PCORES)
    ax.set_title("Strong scaling: speedup (13.1M rows)")
    ax.legend(loc="upper left")
    style.save(fig, out / "speedup.png")

    # 2. efficiency vs p
    fig, ax = plt.subplots(figsize=(7.2, 4.0))
    ax.axhline(1.0, ls="--", lw=1.2, color=MUTED)
    _lines(ax, strong, "efficiency", labels)
    ax.set(xlabel="processes / cores (p)", ylabel="efficiency  S / p", ylim=(0, 1.15),
           xticks=sorted(strong["p"].unique()))
    _pcore_line(ax, PCORES)
    ax.set_title("Strong scaling: parallel efficiency")
    ax.legend(loc="lower left")
    style.save(fig, out / "efficiency.png")

    # 3. phase breakdown at the largest common p (+ sequential)
    rows = [strong[strong["label"] == "sequential"]]
    for lab in labels:
        d = strong[strong["label"] == lab]
        if len(d):
            rows.append(d[d["p"] == d["p"].max()])
    pb = pd.concat(rows)
    pb["name"] = pb.apply(lambda r: f"{r['label']} (p={int(r['p'])})", axis=1)
    fig, ax = plt.subplots(figsize=(8.2, 0.55 * len(pb) + 1.6))
    left = np.zeros(len(pb))
    y = np.arange(len(pb))[::-1]
    for c in PHASE_COLS:
        v = pb[c].fillna(0).to_numpy()
        if v.sum() == 0:
            continue
        ax.barh(y, v, left=left, color=PHASE_COLORS[c], height=0.6, label=c[2:], edgecolor=style.SURFACE, linewidth=2)
        left += v
    for yi, tot in zip(y, pb["total_s"]):
        ax.annotate(f"{tot:.1f} s wall", (left[list(y).index(yi)], yi), xytext=(6, 0), textcoords="offset points",
                    va="center", fontsize=8, color=INK2)
    ax.set_yticks(y, pb["name"])
    ax.grid(axis="x")
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("seconds (slowest worker per phase)")
    ax.set_title("Where the time goes: phase breakdown")
    ax.legend(ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.18 if len(pb) > 4 else -0.3))
    style.save(fig, out / "phase_breakdown.png")

    # 4. Amdahl: measured vs fitted curve for MPI station mode
    lab = "mpi/station" if "mpi/station" in labels else (labels[0] if labels else None)
    if lab:
        f = amdahl_fit(strong, lab)
        fig, ax = plt.subplots(figsize=(7.2, 4.2))
        pp = np.linspace(1, max(16, pmax), 200)
        ax.plot(pp, pp, ls="--", lw=1.2, color=MUTED, label="ideal")
        ax.plot(pp, 1 / (f + (1 - f) / pp), color=SERIES[6], lw=2, label=f"Amdahl fit, serial fraction f = {f:.3f}")
        d = strong[strong["label"] == lab].sort_values("p")
        ax.plot(d["p"], d["speedup"], "o", color=ENGINE_COLORS[lab], label=f"measured ({lab})", markersize=8,
                markeredgecolor=style.SURFACE, markeredgewidth=1.5)
        ax.annotate(f"ceiling 1/f = {1 / f:.1f}×" if f > 0 else "", (pp[-1], 1 / (f + (1 - f) / pp[-1])),
                    xytext=(-4, 8), textcoords="offset points", ha="right", fontsize=8, color=INK2)
        ax.set(xlabel="processes (p)", ylabel="speedup")
        ax.set_title("Amdahl's law: measured speedup vs fitted model")
        ax.legend(loc="upper left")
        style.save(fig, out / "amdahl.png")

    # 5. weak scaling
    if len(weak):
        fig, ax = plt.subplots(figsize=(7.2, 3.8))
        ax.axhline(1.0, ls="--", lw=1.2, color=MUTED)
        _lines(ax, weak, "weak_efficiency", [l for l in LINE_ORDER if l in set(weak["label"])])
        ax.set(xlabel="processes (p), ~1.58M rows per process", ylabel="weak efficiency  T(1) / T(p)",
               ylim=(0, 1.15), xticks=sorted(weak["p"].unique()))
        ax.set_title("Weak scaling (Gustafson): constant work per process")
        ax.legend(loc="lower left")
        style.save(fig, out / "weak_scaling.png")

    # 6. time vs data size
    sz = m[m["tag"] == "size"]
    if len(sz):
        fig, ax = plt.subplots(figsize=(7.2, 4.2))
        for lab in ["sequential", *LINE_ORDER]:
            d = sz[sz["label"] == lab].sort_values("rows")
            if d.empty:
                continue
            ax.plot(d["rows"] / 1e6, d["total_s"], marker="o", color=ENGINE_COLORS[lab],
                    label=lab + ("" if lab == "sequential" else " (p=8)"), markeredgecolor=style.SURFACE,
                    markeredgewidth=1.5)
            r = d.iloc[-1]
            ax.annotate(f"{r['total_s']:.1f} s", (r["rows"] / 1e6, r["total_s"]), xytext=(6, 0),
                        textcoords="offset points", va="center", fontsize=8, color=INK2)
        ax.set(xlabel="rows processed (millions)", ylabel="wall time (s)")
        ax.set_title("Data-size scaling")
        ax.legend(loc="upper left")
        style.save(fig, out / "size_scaling.png")

    # 7. load balancing
    bl = m[m["tag"] == "balance"].copy()
    if len(bl):
        names = {"block": "MPI static: block", "round_robin": "MPI static: round-robin", "lpt": "MPI static: LPT",
                 "dynamic": "MPI dynamic (master–worker)", "static/name": "MP static chunks",
                 "dynamic/lpt": "MP dynamic, LPT order"}
        bl["strategy"] = bl["schedule"].map(names).fillna(bl["schedule"])
        bl["workers"] = np.where(bl["label"] == "mpi/dynamic", bl["p"] - 1, bl["p"])
        order = [v for v in names.values() if v in set(bl["strategy"])]
        groups = sorted(bl["workers"].unique())
        fig, ax = plt.subplots(figsize=(8.4, 4.2))
        w = 0.8 / len(order)
        for i, s_ in enumerate(order):
            vals = [bl[(bl["strategy"] == s_) & (bl["workers"] == g)]["total_s"].median() for g in groups]
            xs = np.arange(len(groups)) + (i - (len(order) - 1) / 2) * w
            ax.bar(xs, vals, width=w - 0.02, color=SERIES[i], label=s_, edgecolor=style.SURFACE, linewidth=2)
        ax.set_xticks(np.arange(len(groups)), [f"{int(g)} computing workers" for g in groups])
        ax.set_ylabel("wall time (s)")
        ax.set_title("Load balancing with unequal station sizes (35.5M rows)")
        ax.legend(ncol=2, loc="upper center", bbox_to_anchor=(0.5, -0.12))
        style.save(fig, out / "load_balancing.png")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", default="results/benchmark.csv")
    ap.add_argument("--plots", default="results/plots")
    a = ap.parse_args(argv)
    src = resolve(a.input)
    m = load(src)
    strong = scaling_table(m, "strong")
    weak = scaling_table(m, "weak")
    out_dir = src.parent
    cols = ["label", "schedule", "p", "rows", "runs", "total_s", "startup_s", "speedup", "self_speedup", "efficiency",
            "karp_flatt", "rows_per_s"]
    if len(strong):
        strong[cols].round(4).to_csv(out_dir / "speedup_table.csv", index=False)
    m.round(4).to_csv(out_dir / "benchmark_medians.csv", index=False)
    plot_all(m, strong, weak, resolve(a.plots))
    if len(strong):
        print(strong[["label", "p", "runs", "total_s", "speedup", "efficiency", "karp_flatt"]].round(3).to_string(index=False))
        for lab in LINE_ORDER:
            f = amdahl_fit(strong, lab)
            if not np.isnan(f):
                print(f"Amdahl serial fraction ({lab}): {f:.3f}  -> max speedup {1 / f if f else np.inf:.1f}x")
    if len(weak):
        print(weak[["label", "p", "rows", "total_s", "weak_efficiency"]].round(3).to_string(index=False))
    print(f"plots -> {resolve(a.plots)}")


if __name__ == "__main__":
    main()
