"""Generate weather plots + a self-contained report (report.md and report.html with embedded images).

    python reporting/make_report.py --run results/mpi_latest --out results/report.html
"""
from __future__ import annotations

import argparse
import base64
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.config import load_config, resolve  # noqa: E402
from reporting import style  # noqa: E402
from reporting.evaluate import evaluate, trend_accuracy  # noqa: E402
from reporting.style import DIVERGING, EVENT_COLORS, INK2, MUTED, SEQ_ORANGE, SERIES, plt  # noqa: E402

SHOW_STATIONS = ["DEL", "BOM", "LDN", "MOW", "DXB", "SYD"]
THRESH_TYPES = ["heatwave", "cold_wave", "heavy_rain", "cloudburst", "high_wind", "humidity_low", "humidity_high"]


def weather_plots(run: Path, out: Path, cfg: dict, stations: pd.DataFrame):
    style.apply()
    out.mkdir(parents=True, exist_ok=True)
    ind = pd.read_csv(run / "climate_indices.csv")
    ev = pd.read_csv(run / "extreme_events.csv", parse_dates=["start_date"])
    tr = pd.read_csv(run / "trends.csv")
    names = stations.set_index("station_id")["station_name"].to_dict()

    # 1. annual mean Tmax for selected stations (+ OLS line)
    fig, ax = plt.subplots(figsize=(8, 4.6))
    for i, sid in enumerate([s for s in SHOW_STATIONS if s in set(ind["station_id"])]):
        d = ind[ind["station_id"] == sid]
        ax.plot(d["year"], d["tmax_mean"], color=SERIES[i], lw=1.6, label=names.get(sid, sid))
        b = np.polyfit(d["year"], d["tmax_mean"], 1)
        ax.plot(d["year"], np.polyval(b, d["year"]), color=SERIES[i], lw=1, ls="--", alpha=0.9)
        ax.annotate(f"{names.get(sid, sid)} {b[0] * 10:+.2f}°C/dec", (d["year"].iloc[-1], np.polyval(b, d["year"].iloc[-1])),
                    xytext=(6, 0), textcoords="offset points", va="center", fontsize=8, color=INK2)
    ax.set(xlabel="year", ylabel="annual mean daily Tmax (°C)")
    ax.set_xlim(right=ind["year"].max() + 9)
    ax.set_title("Warming trends: annual mean Tmax (dashed = OLS fit)")
    ax.legend(ncol=6, loc="upper center", bbox_to_anchor=(0.5, -0.14), fontsize=8)
    style.save(fig, out / "tmax_trends.png")

    # 2. extreme events per year by type (threshold method), stacked
    th = ev[ev["method"] == "threshold"].assign(year=lambda d: d["start_date"].dt.year)
    piv = th.pivot_table(index="year", columns="event_type", values="event_id", aggfunc="count").fillna(0)
    types = [t for t in THRESH_TYPES if t in piv.columns]
    fig, ax = plt.subplots(figsize=(8.4, 4.4))
    bottom = np.zeros(len(piv))
    for t in types:
        ax.bar(piv.index, piv[t], bottom=bottom, color=EVENT_COLORS[t], label=t.replace("_", " "), width=0.8,
               edgecolor=style.SURFACE, linewidth=1)
        bottom += piv[t].to_numpy()
    b = np.polyfit(piv.index, bottom, 1)
    ax.plot(piv.index, np.polyval(b, piv.index), color=style.INK, lw=1.2, ls="--", label=f"trend {b[0] * 10:+.0f} events/decade")
    ax.set(xlabel="year", ylabel="events per year (all stations)")
    ax.set_title("Are extremes becoming more frequent? (fixed-threshold events, heat stress excluded)")
    ax.legend(ncol=4, fontsize=8, loc="upper center", bbox_to_anchor=(0.5, -0.14))
    style.save(fig, out / "extremes_per_year.png")

    # 3. heatmap: heatwave days per station x year (stations that have any)
    hw = ind.pivot_table(index="station_id", columns="year", values="heatwave_days", aggfunc="sum").fillna(0)
    hw = hw[hw.sum(axis=1) > 0]
    hw = hw.loc[hw.sum(axis=1).sort_values(ascending=False).index]
    if len(hw):
        fig, ax = plt.subplots(figsize=(9, 0.32 * len(hw) + 1.6))
        im = ax.imshow(hw.to_numpy(), aspect="auto", cmap=SEQ_ORANGE, interpolation="nearest")
        ax.set_yticks(range(len(hw)), [f"{names.get(s, s)}" for s in hw.index], fontsize=8)
        yrs = hw.columns.to_numpy()
        ax.set_xticks(range(0, len(yrs), 5), yrs[::5])
        ax.grid(False)
        cb = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.01)
        cb.set_label("heatwave days (IMD rule)", color=INK2)
        cb.outline.set_visible(False)
        ax.set_title("Heatwave days per station and year")
        style.save(fig, out / "heatwave_heatmap.png")

    # 4. wet-day rainfall distribution with IMD categories
    ph = pd.read_csv(run / "precip_histogram.csv")
    hr = cfg["thresholds"]["heavy_rain"]
    fig, ax = plt.subplots(figsize=(8, 4.2))
    bands = [(64.5, hr["very_heavy"], "heavy"), (hr["very_heavy"], hr["extremely_heavy"], "very heavy"),
             (hr["extremely_heavy"], max(ph["bin_hi_mm"].max(), 300), "extremely heavy")]
    for (a, b_, n), c in zip(bands, ["#eef4fc", "#dce9f9", "#c8dcf5"]):
        ax.axvspan(a, b_, color=c, lw=0)
        ax.text(a + 3, 0.97, n, transform=ax.get_xaxis_transform(), fontsize=8, color=INK2, va="top")
    cw = ph.assign(b=(ph["bin_lo_mm"] // 5) * 5).groupby("b")["wet_days"].sum()
    ax.bar(cw.index + 2.5, cw.values, width=4.6, color=SERIES[0], edgecolor=style.SURFACE, linewidth=0.5)
    ax.set_yscale("log")
    ax.set(xlabel="daily rainfall on wet days (mm, 5 mm bins)", ylabel="number of station-days (log)")
    ax.set_xlim(0, min(ph["bin_hi_mm"].max(), 450))
    ax.set_title("Daily rainfall distribution with IMD heavy-rain categories")
    style.save(fig, out / "rainfall_distribution.png")

    # 5. station map coloured by warming trend (Sen slope, tmax_mean)
    t = tr[tr["variable"] == "tmax_mean"].merge(stations, on="station_id")
    if len(t):
        fig, ax = plt.subplots(figsize=(9, 4.6))
        lim = max(0.5, float(np.nanmax(np.abs(t["sen_slope_per_decade"]))))
        sig = t["significant"]
        sc = ax.scatter(t["lon"], t["lat"], c=t["sen_slope_per_decade"], cmap=DIVERGING, vmin=-lim, vmax=lim, s=70,
                        edgecolor=np.where(sig, style.INK, style.SURFACE), linewidth=np.where(sig, 1.2, 1.5), zorder=3)
        india = (t["lon"].between(68, 98)) & (t["lat"].between(6, 36))
        ax.annotate(f"India cluster: {int(india.sum())} stations", (82, 4), ha="center", va="top", fontsize=8, color=INK2)
        for _, r in t[~india].iterrows():
            ax.annotate(r["station_id"], (r["lon"], r["lat"]), xytext=(4, 3), textcoords="offset points", fontsize=6.5,
                        color=INK2)
        cb = fig.colorbar(sc, ax=ax, fraction=0.025, pad=0.01)
        cb.set_label("Tmax trend, Sen slope (°C / decade)", color=INK2)
        cb.outline.set_visible(False)
        ax.set(xlim=(-180, 180), ylim=(-60, 75), xlabel="longitude", ylabel="latitude")
        ax.grid(axis="both")
        ax.set_title("Station warming trends (dark ring = significant, Mann–Kendall p < 0.05)")
        style.save(fig, out / "trend_map.png")


def validation_plots(run: Path, data: Path, out: Path):
    style.apply()
    res = {}
    gt_ev = data / "ground_truth_events.csv"
    if gt_ev.exists():
        rec, prec = evaluate(pd.read_csv(run / "extreme_events.csv"), pd.read_csv(gt_ev))
        res["recall"], res["precision"] = rec, prec
    gt_tr = data / "ground_truth_trends.csv"
    if gt_tr.exists():
        ta = trend_accuracy(pd.read_csv(run / "trends.csv"), pd.read_csv(gt_tr))
        res["trend_acc"] = ta
        fig, ax = plt.subplots(figsize=(5.4, 5))
        lo = min(ta["true_trend_c_per_decade"].min(), ta["sen_slope_per_decade"].min()) - 0.05
        hi = max(ta["true_trend_c_per_decade"].max(), ta["sen_slope_per_decade"].max()) + 0.05
        ax.plot([lo, hi], [lo, hi], ls="--", color=MUTED, lw=1.2, label="perfect recovery")
        s = ta["significant"]
        ax.scatter(ta.loc[s, "true_trend_c_per_decade"], ta.loc[s, "sen_slope_per_decade"], s=40, color=SERIES[0],
                   edgecolor=style.SURFACE, linewidth=1.2, label="significant (MK p<0.05)", zorder=3)
        ax.scatter(ta.loc[~s, "true_trend_c_per_decade"], ta.loc[~s, "sen_slope_per_decade"], s=40,
                   facecolor=style.SURFACE, edgecolor=SERIES[0], linewidth=1.5, label="not significant", zorder=3)
        ax.set(xlabel="true injected trend (°C / decade)", ylabel="estimated Sen slope, Tmean (°C / decade)",
               xlim=(lo, hi), ylim=(lo, hi))
        ax.grid(axis="both")
        ax.set_title("Trend recovery on synthetic data")
        ax.legend(loc="upper left", fontsize=8)
        style.save(fig, out / "trend_recovery.png")
    return res


def md_table(df: pd.DataFrame, floatfmt="{:.3f}") -> str:
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for _, r in df.iterrows():
        cells = []
        for c in cols:
            v = r[c]
            cells.append(floatfmt.format(v) if isinstance(v, (float, np.floating)) and not pd.isna(v) else
                         ("" if pd.isna(v) else str(v)))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def build_markdown(run: Path, data: Path, plots: Path, results_root: Path, val: dict) -> str:
    cfgrun = json.loads(json.dumps(__import__("yaml").safe_load(open(run / "run_config.yaml"))))
    tim = json.loads((run / "timings.json").read_text())
    rec = json.loads((run / "global_records.json").read_text())
    ss = pd.read_csv(run / "summary_stats.csv")
    ev = pd.read_csv(run / "extreme_events.csv")
    tr = pd.read_csv(run / "trends.csv")
    dq = pd.read_csv(run / "data_quality.csv") if (run / "data_quality.csv").exists() else None
    manifest = json.loads((data / "manifest.json").read_text())
    rel = lambda p: f"plots/{p}"
    L = []
    r = cfgrun["run"]
    L += ["# Weather Data Processing & Extreme Weather Detection: Results Report", "",
          f"Run `{run.name}` · engine **{r['engine']}** · p = {r['p']} · {manifest['total_rows']:,} hourly rows · "
          f"{manifest['n_stations']} stations · {manifest['years'][0]}–{manifest['years'][1]} · source: {manifest['source']}",
          ""]
    L += ["## 1. Global records", "", "| record | value | station | when |", "|---|---|---|---|"]
    for k, v in rec.items():
        L.append(f"| {k.replace('_', ' ')} | {v['value']} {v['unit']} | {v['station_id']} | {v['time'][:16]} |")
    g = ss[ss["level"] == "global"][["variable", "count", "mean", "std", "min", "max"]]
    L += ["", "### Global min / max / average (all stations, all hours)", "", md_table(g, "{:.2f}"), ""]
    if dq is not None and len(dq):
        tot = dq.drop(columns="station_id").sum()
        L += ["### Data quality (handled in P2 clean)", "",
              f"{int(tot['rows_in']):,} rows read · {int(tot['duplicates']):,} duplicate timestamps removed · "
              f"{int(tot[[c for c in tot.index if c.endswith('_missing')]].sum()):,} missing values · "
              f"{int(tot[[c for c in tot.index if c.endswith('_flagged')]].sum()):,} out-of-range values set to NaN · "
              f"{int(tot['humidity_pct_clipped']):,} humidity values clipped to 100 %.", ""]

    L += ["## 2. Extreme weather events", ""]
    cnt = ev.groupby(["method", "event_type"]).agg(events=("event_id", "size"), event_days=("duration_days", "sum"),
                                                   stations=("station_id", "nunique")).reset_index()
    L += [md_table(cnt), "", f"![extremes per year]({rel('extremes_per_year.png')})", "",
          f"![heatwave heatmap]({rel('heatwave_heatmap.png')})", "",
          f"![rainfall distribution]({rel('rainfall_distribution.png')})", ""]
    top = ev[ev["method"] == "threshold"].sort_values("peak_value", ascending=False).groupby("event_type").head(1)
    L += ["### Most intense event per category", "",
          md_table(top[["event_type", "station_id", "severity", "start_date", "duration_days", "peak_value"]], "{:.1f}"), ""]
    if "recall" in val:
        L += ["### Detection quality vs injected ground truth", "",
              "Recall = injected events found by ≥1 detector of the matching family. Precision = share of a detector's "
              "events that coincide with an injected event; the synthetic climate also produces natural extremes, so "
              "this is a lower bound.", "",
              md_table(val["recall"][["injected_type", "injected", "detected_any_method", "recall_any"]]), "",
              md_table(val["precision"]), ""]

    L += ["## 3. Trends", ""]
    tm = tr[tr["variable"] == "tmax_mean"]
    warm = tm[tm["significant"] & (tm["mk_z"] > 0)]
    hwt = tr[tr["variable"] == "heatwave_days"]
    L += [f"* **{len(warm)} / {len(tm)}** stations show a significant warming of annual mean Tmax "
          f"(Mann–Kendall p < 0.05); median Sen slope **{tm['sen_slope_per_decade'].median():+.2f} °C/decade**.",
          f"* Heatwave days: {int((hwt['significant'] & (hwt['mk_z'] > 0)).sum())} stations with a significant increase, "
          f"{int((hwt['significant'] & (hwt['mk_z'] < 0)).sum())} with a significant decrease.", "",
          f"![tmax trends]({rel('tmax_trends.png')})", "", f"![trend map]({rel('trend_map.png')})", ""]
    if "trend_acc" in val:
        ta = val["trend_acc"]
        err = (ta["sen_slope_per_decade"] - ta["true_trend_c_per_decade"]).abs()
        L += [f"Recovery of the generator's true trend: mean absolute error **{err.mean():.3f} °C/decade**, "
              f"correlation {np.corrcoef(ta['true_trend_c_per_decade'], ta['sen_slope_per_decade'])[0, 1]:.2f}.", "",
              f"![trend recovery]({rel('trend_recovery.png')})", ""]

    L += ["## 4. Processing time & scalability", "",
          f"This run: **{tim['total_s']:.2f} s** wall (excluding {tim['write_s']:.2f} s writing outputs). Phases "
          "(slowest worker): " + ", ".join(f"{k} {v:.2f} s" for k, v in tim["phases"].items()) +
          (f", comm {tim['comm']:.2f} s" if tim.get("comm") else "") + ".", ""]
    st = results_root / "speedup_table.csv"
    if st.exists():
        s = pd.read_csv(st)
        s = s[["label", "p", "runs", "total_s", "speedup", "efficiency", "karp_flatt"]]
        L += ["Median of repeated runs on the same 13.1M-row dataset (T₁ = sequential):", "", md_table(s), ""]
        for img, cap in [("speedup.png", "speedup"), ("efficiency.png", "efficiency"),
                         ("phase_breakdown.png", "phase breakdown"), ("amdahl.png", "Amdahl"),
                         ("weak_scaling.png", "weak scaling"), ("size_scaling.png", "size scaling"),
                         ("load_balancing.png", "load balancing")]:
            if (plots / img).exists():
                L += [f"![{cap}]({rel(img)})", ""]
    fb = results_root / "format_benchmark.csv"
    if fb.exists():
        L += ["### File format: CSV vs Parquet", "", md_table(pd.read_csv(fb)), ""]
    return "\n".join(L)


def md_to_html(md: str, plots: Path) -> str:
    """Tiny markdown -> HTML converter (headings, tables, images, bullets, bold, code), images embedded."""
    html, in_table, in_list = [], False, False

    def inline(t):
        t = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", t)
        return re.sub(r"`(.+?)`", r"<code>\1</code>", t)

    for line in md.split("\n"):
        if line.startswith("|"):
            cells = [c.strip() for c in line.strip("|").split("|")]
            if all(set(c) <= set("-") for c in cells):
                continue
            if not in_table:
                html.append("<div class='tw'><table><thead><tr>" + "".join(f"<th>{inline(c)}</th>" for c in cells) +
                            "</tr></thead><tbody>")
                in_table = True
            else:
                html.append("<tr>" + "".join(f"<td>{inline(c)}</td>" for c in cells) + "</tr>")
            continue
        if in_table:
            html.append("</tbody></table></div>")
            in_table = False
        if line.startswith("* "):
            if not in_list:
                html.append("<ul>")
                in_list = True
            html.append(f"<li>{inline(line[2:])}</li>")
            continue
        if in_list:
            html.append("</ul>")
            in_list = False
        m = re.match(r"!\[(.*?)\]\((.*?)\)", line)
        if m:
            p = plots / Path(m.group(2)).name
            if p.exists():
                b64 = base64.b64encode(p.read_bytes()).decode()
                html.append(f"<figure><img alt='{m.group(1)}' src='data:image/png;base64,{b64}'></figure>")
            continue
        h = re.match(r"(#+) (.*)", line)
        if h:
            n = len(h.group(1))
            html.append(f"<h{n}>{inline(h.group(2))}</h{n}>")
        elif line.strip():
            html.append(f"<p>{inline(line)}</p>")
    if in_table:
        html.append("</tbody></table></div>")
    css = """
:root{--surface:#fcfcfb;--page:#f9f9f7;--ink:#0b0b0b;--ink2:#52514e;--muted:#898781;--grid:#e1e0d9}
body{margin:0;background:var(--page);color:var(--ink);font:15px/1.55 system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:980px;margin:0 auto;padding:24px 16px 64px}
h1{font-size:26px;line-height:1.2} h2{margin-top:40px;border-bottom:1px solid var(--grid);padding-bottom:6px}
p,li{color:var(--ink2)} code{background:#f0efec;padding:1px 4px;border-radius:4px}
.tw{overflow-x:auto;margin:12px 0} table{border-collapse:collapse;font-size:13px;font-variant-numeric:tabular-nums;background:var(--surface)}
th,td{padding:5px 10px;border-bottom:1px solid var(--grid);text-align:left;white-space:nowrap} th{color:var(--ink2);font-weight:600}
figure{margin:18px 0;background:var(--surface);border:1px solid var(--grid);border-radius:8px;padding:8px}
img{max-width:100%;height:auto;display:block}
"""
    return (f"<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
            f"<title>Weather Extremes Report</title><style>{css}</style></head><body><main>{''.join(html)}</main></body></html>")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run", default="results/sequential_latest")
    ap.add_argument("--data", help="parquet dataset (for manifest + ground truth)")
    ap.add_argument("--out", default="results/report.html")
    a = ap.parse_args()
    cfg = load_config()
    run = resolve(a.run).resolve()
    out_html = resolve(a.out)
    results_root = out_html.parent
    plots = results_root / "plots"
    data = resolve(a.data or cfg["paths"]["parquet"])
    stations = pd.DataFrame([{"station_id": k, "station_name": v["name"], "lat": v["lat"], "lon": v["lon"]}
                             for k, v in json.loads((data / "manifest.json").read_text())["stations"].items()])
    weather_plots(run, plots, cfg, stations)
    val = validation_plots(run, data, plots)
    md = build_markdown(run, data, plots, results_root, val)
    out_html.with_suffix(".md").write_text(md)
    out_html.write_text(md_to_html(md, plots))
    print(f"report -> {out_html} and {out_html.with_suffix('.md')}")


if __name__ == "__main__":
    main()
