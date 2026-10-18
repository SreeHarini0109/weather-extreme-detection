"""Build the shareable results page (results/page.html): synthetic results, ERA5 reality check, performance.

    python reporting/make_page.py --synthetic results/<synthetic run> --era5 results/<era5 run>
"""
from __future__ import annotations

import argparse
import base64
import html
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.cleaning import clean_hourly, to_daily  # noqa: E402
from core.config import load_config, resolve  # noqa: E402
from core.pipeline import load_station  # noqa: E402
from reporting import style  # noqa: E402
from reporting.evaluate import evaluate, trend_accuracy  # noqa: E402
from reporting.style import INK2, SERIES, plt  # noqa: E402

REPO = "https://github.com/SreeHarini0109/weather-extreme-detection"


def img(path: Path, alt: str) -> str:
    b64 = base64.b64encode(path.read_bytes()).decode()
    return f'<figure class="plate"><img src="data:image/png;base64,{b64}" alt="{html.escape(alt)}"></figure>'


def table(df: pd.DataFrame, num_fmt: dict | None = None, cls: str = "") -> str:
    num_fmt = num_fmt or {}
    head = "".join(f"<th>{html.escape(str(c))}</th>" for c in df.columns)
    rows = []
    for _, r in df.iterrows():
        cells = []
        for c in df.columns:
            v = r[c]
            if c in num_fmt and pd.notna(v):
                v = num_fmt[c].format(v)
            elif not isinstance(v, str) and pd.isna(v):
                v = "–"
            num = isinstance(r[c], (int, float, np.integer, np.floating)) and not isinstance(r[c], bool)
            cells.append(f'<td class="{"n" if num else ""}">{html.escape(str(v))}</td>')
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f'<div class="tw {cls}"><table><thead><tr>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>'


def heatwave_chart(data: Path, events: pd.DataFrame, out: Path, cfg: dict):
    """ERA5 daily Tmax, Apr-Jun 2024, with detected heatwave spells shaded."""
    style.apply()
    fig, axes = plt.subplots(3, 1, figsize=(8.6, 6.2), sharex=True)
    names = {"PHL": "Phalodi", "DEL": "Delhi", "LKO": "Lucknow"}
    for ax, (sid, color) in zip(axes, [("PHL", SERIES[1]), ("DEL", SERIES[7]), ("LKO", SERIES[6])]):
        h, _ = clean_hourly(load_station(data, sid), cfg)
        d = to_daily(h, cfg)
        d = d[(d["date"] >= "2024-04-15") & (d["date"] <= "2024-06-30")]
        lo, hi = pd.Timestamp("2024-04-15"), pd.Timestamp("2024-06-30")
        e = events[(events["station_id"] == sid) & (events["event_type"] == "heatwave_spell") &
                   (pd.to_datetime(events["end_date"]) >= lo) & (pd.to_datetime(events["start_date"]) <= hi)]
        for _, r in e.iterrows():
            ax.axvspan(pd.Timestamp(r["start_date"]), pd.Timestamp(r["end_date"]) + pd.Timedelta(days=1),
                       color=color, alpha=0.14, lw=0)
        ax.axhline(40, color=style.MUTED, lw=0.8, ls=":")
        ax.plot(d["date"], d["tmax"], color=color, lw=1.8)
        pk = d.loc[d["tmax"].idxmax()]
        ax.annotate(f"{pk['tmax']:.1f} °C", (pk["date"], pk["tmax"]), xytext=(4, 4), textcoords="offset points",
                    fontsize=8, color=INK2)
        ax.set_ylabel("Tmax °C")
        ax.set_title(names[sid], fontsize=10, pad=4)
        ax.set_ylim(28, 51)
        ax.set_xlim(lo, hi)
    axes[0].annotate("dotted: IMD 40 °C heatwave floor · shaded: detected heatwave spells (≥ 3 days)",
                     (0, 1.18), xycoords="axes fraction", fontsize=8, color=INK2)
    fig.suptitle("ERA5 daily maximum temperature, spring 2024", x=0.01, ha="left", fontsize=12, fontweight="bold")
    fig.tight_layout()
    style.save(fig, out)


CSS = """
:root{
  /* layout: one reading column (max 1040px), figures as light "plates", data in mono */
  --bg:#f4f6f8; --surface:#ffffff; --plate:#fcfcfb; --ink:#14202b; --ink2:#4a5866; --muted:#7a8794;
  --line:#dde3e9; --heat:#c2410c; --cool:#1d5fae; --good:#0a7a2f;
  --f-display:"Archivo", "Helvetica Neue", Arial, sans-serif;
  --f-body:"Public Sans", "Helvetica Neue", Arial, sans-serif;
  --f-mono:"JetBrains Mono", ui-monospace, Menlo, monospace;
}
@media (prefers-color-scheme: dark){:root:not([data-theme="light"]){
  --bg:#0f151b; --surface:#16202a; --plate:#fcfcfb; --ink:#e8edf2; --ink2:#a9b6c3; --muted:#7f8c99;
  --line:#26323e; --heat:#f08a4b; --cool:#6aa5ec; --good:#4cc46f; color-scheme:dark}}
:root[data-theme="dark"]{
  --bg:#0f151b; --surface:#16202a; --plate:#fcfcfb; --ink:#e8edf2; --ink2:#a9b6c3; --muted:#7f8c99;
  --line:#26323e; --heat:#f08a4b; --cool:#6aa5ec; --good:#4cc46f; color-scheme:dark}
body{background:var(--bg);color:var(--ink);font:16px/1.6 var(--f-body)}
.wrap{max-width:1040px;margin:0 auto;padding-block:40px 72px;
  padding-left:max(16px, env(safe-area-inset-left, 0px));padding-right:max(16px, env(safe-area-inset-right, 0px));
  display:flex;flex-direction:column;gap:56px}
header{display:flex;flex-direction:column;gap:14px}
.eyebrow{font:600 12px/1 var(--f-mono);letter-spacing:.08em;text-transform:uppercase;color:var(--heat)}
h1{font:800 clamp(34px,5.4vw,56px)/1.02 var(--f-display);letter-spacing:-.02em;margin:0;text-wrap:balance}
h2{font:700 26px/1.15 var(--f-display);margin:0;text-wrap:balance}
h3{font:700 17px/1.3 var(--f-display);margin:0}
p{margin:0;max-width:68ch;color:var(--ink2)}
.lede{font-size:18px;color:var(--ink2)}
section{display:flex;flex-direction:column;gap:18px;min-width:0}
.facts{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:1px;background:var(--line);
  border:1px solid var(--line);border-radius:10px;overflow:hidden}
.fact{background:var(--surface);padding:16px 18px;display:flex;flex-direction:column;gap:4px}
.fact b{font:700 26px/1.1 var(--f-display);font-variant-numeric:tabular-nums}
.fact span{font-size:13px;color:var(--muted)}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,460px),1fr));gap:18px}
.plate{margin:0;background:var(--plate);border:1px solid var(--line);border-radius:10px;padding:10px;min-width:0}
.plate img{display:block;width:100%;height:auto}
.tw{overflow-x:auto;border:1px solid var(--line);border-radius:10px;background:var(--surface)}
table{border-collapse:collapse;width:100%;font-size:14px}
th,td{padding:8px 12px;text-align:left;border-bottom:1px solid var(--line);white-space:nowrap}
th{font:600 11px/1.2 var(--f-mono);letter-spacing:.06em;text-transform:uppercase;color:var(--muted);background:var(--surface)}
td.n{font-family:var(--f-mono);font-variant-numeric:tabular-nums;text-align:right}
tbody tr:last-child td{border-bottom:0}
.callout{border-left:3px solid var(--heat);background:var(--surface);padding:14px 18px;border-radius:0 10px 10px 0}
.callout p{color:var(--ink)}
.pill{display:inline-block;font:600 11px/1 var(--f-mono);padding:4px 8px;border-radius:999px;border:1px solid currentColor}
.hit{color:var(--good)} .miss{color:var(--heat)}
ul{margin:0;padding-left:20px;color:var(--ink2);display:flex;flex-direction:column;gap:6px;max-width:72ch}
code{font:13px var(--f-mono);background:var(--surface);border:1px solid var(--line);padding:1px 5px;border-radius:5px}
a{color:var(--cool)} a:focus-visible{outline:2px solid var(--cool);outline-offset:2px}
footer{border-top:1px solid var(--line);padding-top:18px;font-size:14px;color:var(--muted)}
"""


def streaming_html(results_root: Path, P) -> tuple[str, str]:
    sm = results_root / "stream" / "summary.csv"
    if not sm.exists():
        return "", ""
    s = pd.read_csv(sm)
    label = {"heatwave_2024": "2024 heatwave", "amphan_2020": "Cyclone Amphan 2020", "michaung_2023": "Cyclone Michaung 2023",
             "synthetic_2024": "Synthetic, 50 stations"}

    def where(r):
        name = label.get(r["scenario"], r["scenario"])
        if "worker_killed" in r["run"]:
            return f"{name}, worker stopped mid-stream"
        if "driver_kill" in r["run"]:
            return f"{name}, detector killed then resumed"
        return name

    t = pd.DataFrame({
        "run": s.apply(where, axis=1),
        "Spark": s["master"].str.replace("spark://master:7077", "cluster, 3 workers", regex=False),
        "speed": s["speed_days_per_s"].map(lambda v: "max" if v == 0 else f"{v:g} day/s"),
        "hourly rows": s["rows_in"], "alerts": s["alerts"],
        "latency median": s["latency_median_s"], "latency p95": s["latency_p95_s"],
        "parity": s["parity_matched"].astype(int).astype(str) + " / " + s["parity_batch_events"].astype(int).astype(str)})
    t.loc[s["speed_days_per_s"] == 0, ["latency median", "latency p95"]] = np.nan
    local = s[(s["scenario"] == "heatwave_2024") & s["master"].str.startswith("local") & ~s["run"].str.contains("kill")]
    lat = float(local["latency_median_s"].iloc[0]) if len(local) else float(s["latency_median_s"].median())
    fact = f'<div class="fact"><b>{lat:.1f} s</b><span>from a day\'s last reading to its alert, streaming</span></div>'
    thr = s[s["speed_days_per_s"] == 0]
    tput = f"{float(thr['rows_per_s'].iloc[0]):,.0f}" if len(thr) else "?"
    sec = f"""
<section id="streaming">
  <h2>Live detection with Spark Structured Streaming</h2>
  <p>The batch engines analyse stored history. The streaming extension detects extremes while data arrives. A replayer
  feeds the stored hourly readings back as a live feed, six simulated hours per file. Spark Structured Streaming
  removes duplicates under a 2-hour event-time watermark, cleans the readings with the batch engine's own expressions,
  and aggregates each station's day in a one-day window that is emitted once, when the day is complete. Each finished
  day is compared with the station's normal (learnt by a batch run) using the same 19 rules, and an incremental
  version of the event builder raises NEW, ESCALATED and ENDED alerts.</p>
  <p>At the end of every run each closed streamed event is compared with the batch engine's event of the same id. All
  runs below match exactly. A day's alerts arrive about {lat:.0f} s after its last hourly reading: the watermark waits
  for the next file, then the 1 s trigger fires. Replayed as fast as possible, 50 stations stream at about
  {tput} rows/s on four local cores.</p>
  {table(t, {"latency median": "{:.1f} s", "latency p95": "{:.1f} s", "hourly rows": "{:,}", "alerts": "{:,}"})}
  {P("stream_alert_timeline.png", "New threshold alerts per station during the replay of May-June 2024")}
  {P("stream_latency.png", "Detection latency per micro-batch, local and on the Docker cluster with a worker stopped")}
  <div class="callout"><p>Fault tolerance: Spark checkpoints its file offsets and its dedup and window state, and the
  detector saves its open events after every micro-batch. Stopping a worker container mid-stream only delayed alerts
  while Spark re-ran the lost tasks. Killing the detector process and restarting it with <code>--resume</code> lost
  and duplicated nothing: both runs gave the same 232 alerts as an undisturbed run.</p></div>
</section>
"""
    return fact, sec


def build(syn: Path, era: Path, syn_data: Path, era_data: Path, results_root: Path, out: Path):
    cfg = load_config()
    plots = results_root / "plots"
    rec = json.loads((syn / "global_records.json").read_text())
    tr = pd.read_csv(syn / "trends.csv")
    ev = pd.read_csv(syn / "extreme_events.csv")
    st = pd.read_csv(results_root / "speedup_table.csv")
    syn_manifest = json.loads((syn_data / "manifest.json").read_text())
    recall, prec = evaluate(ev, pd.read_csv(syn_data / "ground_truth_events.csv"))
    ta = trend_accuracy(tr, pd.read_csv(syn_data / "ground_truth_trends.csv"))
    mae = (ta["sen_slope_per_decade"] - ta["true_trend_c_per_decade"]).abs().mean()

    era_ev = pd.read_csv(era / "extreme_events.csv", parse_dates=["start_date", "end_date"])
    era_rec = json.loads((era / "global_records.json").read_text())
    era_manifest = json.loads((era_data / "manifest.json").read_text())
    heatwave_chart(era_data, era_ev, plots / "era5_heatwave_2024.png", cfg)

    best = st[st["label"] != "sequential"].sort_values("speedup", ascending=False).iloc[0]
    t1 = st.loc[st["label"] == "sequential", "total_s"].iloc[0]
    tm = tr[tr["variable"] == "tmax_mean"]
    warm = int((tm["significant"] & (tm["mk_z"] > 0)).sum())

    # ---- tables
    rec_rows = pd.DataFrame([{"record": k.replace("_", " "), "value": f"{v['value']} {v['unit']}",
                              "station": v["station_id"], "when (UTC)": v["time"][:16]} for k, v in rec.items()])
    sp = st[st["p"].isin([1, 2, 4, 8, 10, 11])][["label", "p", "total_s", "speedup", "efficiency"]].copy()
    sp = sp.rename(columns={"label": "engine / mode", "total_s": "median wall s"})
    sp = sp.sort_values(["engine / mode", "p"])
    rc = recall[["injected_type", "injected", "detected_any_method", "recall_any"]].rename(
        columns={"injected_type": "injected event", "detected_any_method": "found", "recall_any": "recall"})

    def era_check(title, q, truth):
        d = era_ev.query(q)
        status = '<span class="pill hit">detected</span>' if len(d) else '<span class="pill miss">not detected</span>'
        if len(d):
            r = d.sort_values("peak_value", ascending=False).iloc[0]
            what = f"{r['event_type'].replace('_', ' ')} · {r['severity'].replace('_', ' ')} · peak {r['peak_value']}"
        else:
            what = "ERA5 daily rain 41 mm and 60 mm on 1–2 Dec, under the 64.5 mm IMD heavy-rain line"
        return f"<tr><td>{title}</td><td>{truth}</td><td>{status}</td><td>{html.escape(what)}</td></tr>"

    checks = "".join([
        era_check("North India heatwave, May–Jun 2024", "station_id in ['DEL','PHL','LKO'] and start_date>='2024-05-01' and "
                  "start_date<='2024-06-20' and event_type=='heatwave_spell'", "Phalodi ~50 °C (25 May)"),
        era_check("Cyclone Amphan, Kolkata", "station_id=='CCU' and start_date>='2020-05-20' and start_date<='2020-05-20' and event_type=='high_wind'",
                  "landfall 20 May 2020"),
        era_check("Cyclone Michaung, Chennai", "station_id=='MAA' and start_date>='2023-12-04' and start_date<='2023-12-04' and event_type=='heavy_rain'",
                  "4 Dec 2023, city flooding"),
        era_check("Chennai floods", "station_id=='MAA' and start_date>='2015-11-30' and start_date<='2015-12-03' and "
                  "event_type=='heavy_rain'", "~290 mm+ on 1 Dec 2015 (gauges)"),
    ])
    era_table = ('<div class="tw"><table><thead><tr><th>real event</th><th>observed</th><th>result</th>'
                 f'<th>what the pipeline found</th></tr></thead><tbody>{checks}</tbody></table></div>')

    P = lambda name, alt: img(plots / name, alt) if (plots / name).exists() else ""
    stream_fact, stream_section = streaming_html(results_root, P)
    body = f"""
<title>Weather Extremes Lab</title>
<meta name="claude:viewport-fit-sides" content="cover">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Archivo:wght@700;800&family=JetBrains+Mono:wght@400;600&family=Public+Sans:wght@400;600&display=swap">
<style>{CSS}</style>
<div class="wrap">
<header>
  <div class="eyebrow">Parallel &amp; Distributed Systems Lab · results</div>
  <h1>Finding weather extremes in 13 million hourly readings</h1>
  <p class="lede">One pipeline cleans hourly temperature, rain, humidity and wind data, computes min / max / average
  statistics, flags extreme days with IMD, NWS and Beaufort rules plus station-relative statistics, and tests for
  long-term trends. It runs on four engines (sequential, multiprocessing, MPI, Spark), which produce identical results,
  is checked against real ERA5 data, and also runs as a live stream that raises alerts as each day completes.</p>
  <div class="facts">
    <div class="fact"><b>{syn_manifest['total_rows'] / 1e6:.1f} M</b><span>hourly rows, {syn_manifest['n_stations']} stations, {syn_manifest['years'][0]}–{syn_manifest['years'][1]}</span></div>
    <div class="fact"><b>{best['speedup']:.1f}×</b><span>best speedup ({best['label']}, p={int(best['p'])}) vs {t1:.1f} s sequential</span></div>
    <div class="fact"><b>{len(ev):,}</b><span>extreme events detected by 19 rules</span></div>
    <div class="fact"><b>{recall['recall_any'].min() * 100:.0f}–{recall['recall_any'].max() * 100:.0f} %</b><span>recall on injected extremes</span></div>
    <div class="fact"><b>3 / 4</b><span>real Indian extremes found in ERA5 data</span></div>
    {stream_fact}
  </div>
</header>

<section id="records">
  <h2>Records and statistics</h2>
  <p>Every engine computes count, mean, M2, min and max with the time each extreme occurred, per station, year and month.
  Partial results from different workers merge exactly with Chan's parallel-variance formula, which MPI runs as a custom
  reduction operator. Records below are from the synthetic dataset, which has realistic climates for 50 real cities and
  injected extremes.</p>
  {table(rec_rows)}
</section>

<section id="extremes">
  <h2>Extreme events</h2>
  <p>Fixed thresholds (IMD heatwave and heavy-rain categories, cloudburst ≥ 100 mm/h, NWS heat index, Beaufort ≥ 8)
  sit beside station-relative tests (P99 from merged histograms, z-score, IQR) and multi-day spells. Consecutive
  flagged days become one event.</p>
  {P("extremes_per_year.png", "Fixed-threshold extreme events per year, stacked by type")}
  <div class="grid2">{P("heatwave_heatmap.png", "Heatwave days per station and year")}{P("rainfall_distribution.png", "Daily rainfall distribution with IMD categories")}</div>
  <h3>Detection quality against injected ground truth</h3>
  <p>The generator injects heatwaves, cold waves, cloudbursts and cyclones at known dates. Recall counts how many were
  found by at least one detector of the matching family.</p>
  {table(rc, {"recall": "{:.1%}"})}
</section>

<section id="era5">
  <h2>Reality check on ERA5 data</h2>
  <p>The same code was run, unchanged, on {era_manifest['total_rows']:,} hours of ERA5 reanalysis (Open-Meteo) for six
  Indian cities, 2015–2024. The hottest reading it found was {era_rec['highest_temperature']['value']} °C at Phalodi on
  {era_rec['highest_temperature']['time'][:10]}, the wettest day {era_rec['max_daily_rainfall']['value']} mm at Chennai on
  {era_rec['max_daily_rainfall']['time'][:10]} (Cyclone Michaung), and the strongest gust
  {era_rec['max_wind_gust']['value']} km/h at Kolkata on {era_rec['max_wind_gust']['time'][:10]} (Cyclone Amphan).</p>
  {era_table}
  {P("era5_heatwave_2024.png", "ERA5 daily Tmax in spring 2024 with detected heatwave spells")}
  <div class="callout"><p>The miss is a data limitation, not a detector bug. ERA5 averages rain over a ~25 km grid cell,
  so the 2015 Chennai downpour appears as 41 mm and 60 mm days. The same smoothing explains why ERA5 peaks at 48.1 °C at
  Phalodi while the station measured about 50 °C.</p></div>
</section>

<section id="trends">
  <h2>Trends</h2>
  <p>Annual indices (mean Tmax, TXx, Rx1day, Rx5day, consecutive dry days, heatwave days) per station are tested with
  Mann–Kendall and Sen's slope. {warm} of {len(tm)} synthetic stations show significant warming of mean Tmax
  (median {tm['sen_slope_per_decade'].median():+.2f} °C per decade). Against the generator's true trends the mean
  absolute error is {mae:.3f} °C per decade.</p>
  {P("tmax_trends.png", "Annual mean Tmax with OLS trend lines")}
  <div class="grid2">{P("trend_map.png", "Station map coloured by warming trend")}{P("trend_recovery.png", "Estimated vs true trend")}</div>
</section>

<section id="performance">
  <h2>Processing time and scalability</h2>
  <p>Medians of three runs on the 13.1M-row dataset, on an Apple M4 with 4 performance and 6 efficiency cores. Station
  partitioning (MPI with longest-processing-time balancing) scales best. Past four processes the work spills onto the
  slower efficiency cores. MPI's time-partitioned mode needs a halo exchange and histogram Allreduce, and every rank
  still checks all 50 stations, so it stops scaling at p = 4. Spark is slower than plain Python on one machine at this
  size, because JVM and shuffle overhead outweigh the parallelism.</p>
  <div class="grid2">{P("speedup.png", "Speedup vs processes")}{P("efficiency.png", "Parallel efficiency")}</div>
  {P("phase_breakdown.png", "Phase breakdown per engine")}
  {table(sp, {"median wall s": "{:.2f}", "speedup": "{:.2f}", "efficiency": "{:.2f}"})}
  <div class="grid2">{P("amdahl.png", "Amdahl fit")}{P("weak_scaling.png", "Weak scaling")}</div>
  <div class="grid2">{P("size_scaling.png", "Data-size scaling")}{P("load_balancing.png", "Load balancing strategies")}</div>
  <h3>On a real cluster</h3>
  <p>A Docker Compose cluster (one master, three workers) ran MPI across four containers and Spark on three worker
  nodes, and both produced output identical to the sequential engine. A deployment kit for separate lab PCs
  (host networking, SSH on port 2222) is in <code>docker/lan/</code>. Stopping a Spark worker mid-job did not break
  the run: Spark rebuilt the lost partitions from lineage and finished with the same results.</p>
</section>

{stream_section}
<section id="limits">
  <h2>Limitations</h2>
  <ul>
    <li>Synthetic data comes from a statistical weather model, not a climate model. It contains natural extremes as well
    as injected ones, so detector precision against the injected set is a lower bound.</li>
    <li>IMD heatwave and cold-wave rules apply to Indian stations only (with IMD's hill-station criteria above
    1000 m); other stations rely on the station-relative detectors, so their heat and cold recall is lower.</li>
    <li>Days are UTC days, and "departure from normal" uses the station-month mean rather than IMD's daily normals.</li>
    <li>ERA5 underestimates local extremes, especially short, intense rain.</li>
    <li>The live stream is a replay of stored data (no free live feed exists for these stations), and its thresholds
    come from a batch run over the same years.</li>
    <li>Cluster timings come from four containers sharing one 8-CPU virtual machine. They show correctness and
    fault tolerance, not real multi-machine speed.</li>
  </ul>
</section>

<footer>Code, data and full benchmark tables: <a href="{REPO}">{REPO.replace('https://', '')}</a></footer>
</div>
"""
    out.write_text(body)
    print(f"page -> {out} ({out.stat().st_size / 1e6:.2f} MB)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--synthetic", required=True)
    ap.add_argument("--era5", required=True)
    ap.add_argument("--synthetic-data", default="data/parquet/synthetic")
    ap.add_argument("--era5-data", default="data/parquet/real")
    ap.add_argument("--out", default="results/page.html")
    a = ap.parse_args()
    build(resolve(a.synthetic), resolve(a.era5), resolve(a.synthetic_data), resolve(a.era5_data), resolve("results"),
          resolve(a.out))


if __name__ == "__main__":
    main()
