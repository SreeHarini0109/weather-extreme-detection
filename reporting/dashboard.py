"""Streamlit dashboard: station explorer with highlighted extremes, trend map, records, benchmarks.

    streamlit run reporting/dashboard.py
    streamlit run reporting/dashboard.py -- --run results/mpi_latest --data data/parquet/synthetic
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from core.cleaning import clean_hourly, to_daily  # noqa: E402
from core.config import load_config, resolve  # noqa: E402
from core.pipeline import load_station  # noqa: E402
from reporting.style import ENGINE_COLORS, EVENT_COLORS, SERIES  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--run", default="results/sequential_latest")
ap.add_argument("--data", default=None)
args, _ = ap.parse_known_args()
cfg = load_config()
RUN = resolve(args.run)
DATA = resolve(args.data or cfg["paths"]["parquet"])
RESULTS = RUN.parent

st.set_page_config(page_title="Weather Extremes", layout="wide")
LAYOUT = dict(template="plotly_white", margin=dict(l=10, r=10, t=40, b=10), hovermode="x unified",
              font=dict(family="system-ui, -apple-system, Segoe UI, sans-serif"))


@st.cache_data
def load_run(run: str):
    r = Path(run)
    return {
        "events": pd.read_csv(r / "extreme_events.csv", parse_dates=["start_date", "end_date", "peak_date"]),
        "trends": pd.read_csv(r / "trends.csv"), "indices": pd.read_csv(r / "climate_indices.csv"),
        "stats": pd.read_csv(r / "summary_stats.csv"),
        "records": json.loads((r / "global_records.json").read_text()),
        "timings": json.loads((r / "timings.json").read_text()),
    }


@st.cache_data
def load_manifest(data: str):
    m = json.loads((Path(data) / "manifest.json").read_text())
    return pd.DataFrame([{"station_id": k, "name": v["name"], "lat": v["lat"], "lon": v["lon"], "rows": v["rows"]}
                         for k, v in m["stations"].items()]).sort_values("station_id")


@st.cache_data
def station_daily(data: str, sid: str):
    h, _ = clean_hourly(load_station(data, sid), cfg)
    return to_daily(h, cfg)


R = load_run(str(RUN))
stations = load_manifest(str(DATA))
st.title("Weather Data Processing & Extreme Weather Detection")
st.caption(f"Run `{RUN.resolve().name}` · {len(stations)} stations · data `{DATA.name}`")

tab1, tab2, tab3, tab4 = st.tabs(["Station explorer", "Trends map", "Records & extremes", "Performance"])

with tab1:
    c1, c2, c3 = st.columns([2, 2, 3])
    sid = c1.selectbox("Station", stations["station_id"],
                       format_func=lambda s: f"{s} · {stations.set_index('station_id').loc[s, 'name']}")
    d = station_daily(str(DATA), sid)
    years = sorted(d["year"].unique())
    yr = c2.select_slider("Years", options=years, value=(years[max(0, len(years) - 3)], years[-1]))
    ev_all = R["events"][R["events"]["station_id"] == sid]
    types = sorted(ev_all["event_type"].unique())
    default = [t for t in ["heatwave", "cold_wave", "heavy_rain", "high_wind", "cloudburst"] if t in types]
    sel = c3.multiselect("Highlight event types", types, default=default)
    dd = d[(d["year"] >= yr[0]) & (d["year"] <= yr[1])]
    ev = ev_all[ev_all["event_type"].isin(sel) & (ev_all["start_date"].dt.year >= yr[0]) &
                (ev_all["start_date"].dt.year <= yr[1])]

    fig = go.Figure()
    fig.add_scatter(x=dd["date"], y=dd["tmax"], name="Tmax", line=dict(color=SERIES[1], width=1.5))
    fig.add_scatter(x=dd["date"], y=dd["tmin"], name="Tmin", line=dict(color=SERIES[0], width=1.5))
    for _, e in ev[ev["event_type"].isin(["heatwave", "cold_wave", "heatwave_spell", "warm_spell", "hot_day_p99",
                                           "cold_night_p01", "hot_zscore", "cold_zscore"])].iterrows():
        fig.add_vrect(x0=e["start_date"], x1=e["end_date"] + pd.Timedelta(days=1), line_width=0, opacity=0.18,
                      fillcolor=EVENT_COLORS.get(e["event_type"], SERIES[6]))
    fig.update_layout(title="Daily temperature (shaded = detected temperature events)", yaxis_title="°C",
                      height=340, **LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

    r1, r2 = st.columns(2)
    fig = go.Figure()
    fig.add_bar(x=dd["date"], y=dd["precip_sum"], name="daily rain", marker_color=SERIES[0])
    for name, v in [("heavy 64.5 mm", 64.5), ("very heavy 115.6 mm", 115.6)]:
        fig.add_hline(y=v, line=dict(color="#898781", dash="dot", width=1), annotation_text=name,
                      annotation_position="top left")
    fig.update_layout(title="Daily rainfall (IMD heavy-rain thresholds dotted)", yaxis_title="mm/day", height=300,
                      showlegend=False, **LAYOUT)
    r1.plotly_chart(fig, use_container_width=True)
    fig = go.Figure()
    fig.add_scatter(x=dd["date"], y=dd["wind_max"], name="max wind", line=dict(color=SERIES[2], width=1.2))
    fig.add_hline(y=62, line=dict(color="#898781", dash="dot", width=1), annotation_text="gale (B8) 62 km/h",
                  annotation_position="top left")
    fig.update_layout(title="Daily maximum wind", yaxis_title="km/h", height=300, showlegend=False, **LAYOUT)
    r2.plotly_chart(fig, use_container_width=True)
    st.subheader(f"Events at {sid} ({len(ev)})")
    st.dataframe(ev.sort_values("start_date", ascending=False), use_container_width=True, hide_index=True)

with tab2:
    var = st.selectbox("Trend variable", sorted(R["trends"]["variable"].unique()),
                       index=sorted(R["trends"]["variable"].unique()).index("tmax_mean"))
    t = R["trends"][R["trends"]["variable"] == var].merge(stations, on="station_id")
    lim = float(t["sen_slope_per_decade"].abs().max() or 1)
    fig = px.scatter_geo(t, lat="lat", lon="lon", color="sen_slope_per_decade", hover_name="name",
                         hover_data={"station_id": True, "sen_slope_per_decade": ":.3f", "mk_p": ":.3f",
                                     "trend": True, "lat": False, "lon": False},
                         color_continuous_scale=["#184f95", "#6da7ec", "#f0efec", "#ef8a8a", "#b02a2a"],
                         range_color=(-lim, lim), projection="natural earth")
    fig.update_traces(marker=dict(size=11, line=dict(width=1, color="#fcfcfb")))
    fig.update_layout(height=520, coloraxis_colorbar_title="per decade", margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig, use_container_width=True)
    sig = t[t["significant"]]
    st.markdown(f"**{len(sig)} / {len(t)}** stations have a significant trend (Mann–Kendall p < "
                f"{cfg['trends']['alpha']}): {int((sig['mk_z'] > 0).sum())} increasing, "
                f"{int((sig['mk_z'] < 0).sum())} decreasing.")
    st.dataframe(t[["station_id", "name", "n_years", "ols_slope_per_decade", "sen_slope_per_decade", "mk_p", "trend"]]
                 .sort_values("sen_slope_per_decade", ascending=False), use_container_width=True, hide_index=True)

with tab3:
    cols = st.columns(4)
    for i, (k, v) in enumerate(R["records"].items()):
        cols[i % 4].metric(k.replace("_", " ").capitalize(), f"{v['value']} {v['unit']}", f"{v['station_id']} · {v['time'][:10]}",
                           delta_color="off")
    g = R["stats"][R["stats"]["level"] == "global"][["variable", "count", "mean", "std", "min", "max"]]
    st.subheader("Global min / max / average")
    st.dataframe(g.round(2), use_container_width=True, hide_index=True)
    ev = R["events"].assign(year=lambda x: x["start_date"].dt.year)
    th = ev[ev["method"] == "threshold"].groupby(["year", "event_type"]).size().reset_index(name="events")
    fig = px.bar(th, x="year", y="events", color="event_type", color_discrete_map=EVENT_COLORS,
                 title="Fixed-threshold extreme events per year")
    fig.update_layout(height=380, **(LAYOUT | {"hovermode": "closest"}))
    st.plotly_chart(fig, use_container_width=True)
    st.subheader("Event counts by method")
    st.dataframe(ev.groupby(["method", "event_type"]).agg(events=("event_id", "size"),
                                                         days=("duration_days", "sum")).reset_index(),
                 use_container_width=True, hide_index=True)

with tab4:
    tm = R["timings"]
    st.markdown(f"This run: **{tm['total_s']:.2f} s** (writing {tm['write_s']:.2f} s excluded)")
    ph = pd.DataFrame({"phase": list(tm["phases"]) + ["comm"], "seconds": list(tm["phases"].values()) + [tm.get("comm", 0)]})
    st.plotly_chart(px.bar(ph, x="seconds", y="phase", orientation="h", title="Phase times (slowest worker)",
                           color_discrete_sequence=[SERIES[0]]).update_layout(height=300, **(LAYOUT | {"hovermode": "closest"})),
                    use_container_width=True)
    sp = RESULTS / "speedup_table.csv"
    if sp.exists():
        s = pd.read_csv(sp)
        s = s[s["label"] != "sequential"]
        fig = go.Figure()
        pmax = int(s["p"].max())
        fig.add_scatter(x=[1, pmax], y=[1, pmax], name="ideal", line=dict(color="#898781", dash="dash", width=1.5))
        for lab, g_ in s.groupby("label"):
            fig.add_scatter(x=g_["p"], y=g_["speedup"], name=lab, mode="lines+markers",
                            line=dict(color=ENGINE_COLORS.get(lab, SERIES[6]), width=2), marker=dict(size=8))
        fig.update_layout(title="Strong scaling: speedup vs sequential (median of repeats)", xaxis_title="p",
                          yaxis_title="speedup", height=420, **LAYOUT)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(s.round(3), use_container_width=True, hide_index=True)
    else:
        st.info("Run `bash benchmark/run_benchmarks.sh` then `python benchmark/analyze.py` to populate this tab.")
    for img in ["phase_breakdown.png", "amdahl.png", "weak_scaling.png", "size_scaling.png", "load_balancing.png"]:
        p = RESULTS / "plots" / img
        if p.exists():
            st.image(str(p))
