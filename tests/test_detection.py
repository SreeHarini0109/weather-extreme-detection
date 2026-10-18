"""Threshold boundaries, spell detection, halo ownership rule, and ground-truth recall."""
import numpy as np
import pandas as pd
import pytest

from core.cleaning import clean_hourly, to_daily
from core.detection import build_rules, detect_events, filter_owned_events
from core.thresholds import attach_thresholds, climatology_arrays, hist_percentile, thresholds_from_arrays


def make_daily(n=40, start="2000-01-01", **cols):
    d = pd.DataFrame({"date": pd.date_range(start, periods=n, freq="D")})
    d["year"], d["month"] = d["date"].dt.year, d["date"].dt.month
    base = dict(tmax=30.0, tmin=20.0, tmean=25.0, rh_mean=60.0, rh_max=80.0, rh_min=40.0, precip_sum=0.0,
                precip_max_1h=0.0, wind_max=10.0, gust_max=15.0, hi_max=30.0,
                tmax_mean=30.0, tmax_std=2.0, tmin_mean=20.0, tmin_std=2.0, wind_mean=10.0, wind_std=3.0,
                tmax_p90=33.0, tmax_p99=36.0, tmin_p01=14.0, precip_wet_p99=80.0, precip_wet_q1=2.0,
                precip_wet_q3=20.0, wind_p99=40.0, wind_q1=8.0, wind_q3=14.0)
    for k, v in base.items():
        d[k] = cols.get(k, v)
    return d


def events_of(cfg, d, etype):
    ev = detect_events(d, "T", build_rules(cfg))
    return ev[ev["event_type"] == etype]


def test_heavy_rain_boundaries(cfg):
    p = np.zeros(40)
    p[[3, 10, 20, 30]] = [64.4, 64.5, 115.6, 204.5]
    ev = events_of(cfg, make_daily(precip_sum=p), "heavy_rain")
    assert list(ev["severity"]) == ["heavy", "very_heavy", "extremely_heavy"]
    assert pd.Timestamp("2000-01-04") not in set(pd.to_datetime(ev["start_date"]))


def test_beaufort_and_cloudburst(cfg):
    w = np.full(40, 10.0)
    w[[5, 15, 25]] = [61.9, 62.0, 118.0]
    ev = events_of(cfg, make_daily(wind_max=w, wind_p99=500, wind_q3=500, wind_std=100), "high_wind")
    assert list(ev["severity"]) == ["B8_gale", "B12_hurricane"]
    h = np.zeros(40)
    h[7] = 100.0
    assert len(events_of(cfg, make_daily(precip_max_1h=h), "cloudburst")) == 1


def test_heatwave_spell_and_imd_rule(cfg):
    t = np.full(40, 38.0)
    t[10:14] = 41.0          # departure 41-36 = 5 >= 4.5 -> heatwave, 4 days -> spell
    t[20:22] = 46.0          # absolute >= 45 (departure 10 -> severe), only 2 days -> no spell
    t[30] = 47.5             # severe (absolute >= 47)
    d = make_daily(tmax=t, tmax_mean=36.0, tmax_p99=99, tmax_p90=99)
    hw = events_of(cfg, d, "heatwave")
    assert list(hw["duration_days"]) == [4, 2, 1]
    assert list(hw["severity"]) == ["heatwave", "severe_heatwave", "severe_heatwave"]
    sp = events_of(cfg, d, "heatwave_spell")
    assert len(sp) == 1 and sp.iloc[0]["duration_days"] == 4
    assert str(sp.iloc[0]["start_date"])[:10] == "2000-01-11"
    assert sp.iloc[0]["peak_value"] == 41.0 and str(sp.iloc[0]["peak_date"])[:10] == "2000-01-11"


def test_zscore_ignores_zero_std(cfg):
    d = make_daily(tmax=np.full(40, 50.0), tmax_std=0.0, tmax_p99=99, tmax_p90=99)
    assert events_of(cfg, d, "hot_zscore").empty


def test_hist_percentile_matches_nearest_rank():
    rng = np.random.default_rng(0)
    x = np.round(rng.normal(30, 3, 997), 1)
    lo, w = -60.05, 0.1
    counts = np.bincount(np.floor((x - lo) / w).astype(int), minlength=1201)
    for q in (0.01, 0.25, 0.9, 0.99):
        exact = np.sort(x)[int(np.ceil(q * x.size)) - 1]
        assert abs(hist_percentile(counts, q, lo, w) - exact) < 1e-9


@pytest.mark.parametrize("cut_days", [[365], [100, 400], [3, 4, 5, 200]])
def test_halo_ownership_matches_single_chunk(cfg, cut_days):
    """Split a series into chunks + halo; union of owned events == events on the whole series."""
    rng = np.random.default_rng(5)
    n = 730
    t = np.round(rng.normal(38, 3, n), 1)
    t[360:370] = 44.0                               # 10-day heatwave crossing the 365-day boundary
    d = make_daily(n, tmax=t, tmax_mean=36.0, tmax_p99=43.0, tmax_p90=40.0)
    rules = build_rules(cfg)
    whole = detect_events(d, "T", rules)
    bounds = [0, *cut_days, n]
    owned, H = [], cfg["mpi"]["halo_days"]
    for i, (a, b) in enumerate(zip(bounds[:-1], bounds[1:])):
        frame = d.iloc[max(0, a - H):b]
        ev = detect_events(frame, "T", rules)
        owned.append(filter_owned_events(ev, d["date"].iloc[a], d["date"].iloc[b - 1], i == 0, i == len(bounds) - 2))
    got = pd.concat(owned).sort_values("event_id").reset_index(drop=True)
    exp = whole.sort_values("event_id").reset_index(drop=True)
    assert list(got["event_id"]) == list(exp["event_id"])            # each event exactly once
    assert list(got["duration_days"]) == list(exp["duration_days"])


def test_ground_truth_recall(cfg):
    """Injected heatwaves / cloudbursts / cyclones in synthetic data are found by the detectors."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "data"))
    from generator import build_station_table, generate_station
    base = pd.read_csv(Path(__file__).resolve().parent.parent / "data" / "stations.csv")
    st = build_station_table(base, 3, 7).iloc[[0, 2]]          # Delhi (hot, inland), Chennai (coastal)
    g = cfg["generator"] | {"inject": cfg["generator"]["inject"] | {"heatwave_rate": 2, "cloudburst_rate": 2,
                                                                     "cyclone_rate": 2}}
    found = {"heatwave": [0, 0], "cloudburst": [0, 0], "cyclone": [0, 0]}
    match = {"heatwave": {"heatwave", "heatwave_spell", "hot_day_p99", "hot_zscore", "warm_spell"},
             "cloudburst": {"cloudburst"}, "cyclone": {"high_wind", "wind_p99", "wind_zscore", "wind_iqr"}}
    for row in st.to_dict("records"):
        raw, gt, _ = generate_station(row, 2000, 2009, g, 7)
        raw["timestamp"] = raw["timestamp"].dt.tz_localize(None)
        hourly, _ = clean_hourly(raw, cfg)
        daily = to_daily(hourly, cfg)
        mom, hist = climatology_arrays(daily, cfg)
        d = attach_thresholds(daily, thresholds_from_arrays(mom, hist, [row["station_id"]], cfg))
        ev = detect_events(d, row["station_id"], build_rules(cfg))
        ev["s"], ev["e"] = pd.to_datetime(ev["start_date"]), pd.to_datetime(ev["end_date"])
        for g_ in gt:
            if g_["event_type"] not in found:
                continue
            s, e = pd.Timestamp(g_["start_date"]), pd.Timestamp(g_["end_date"])
            hit = ev[ev["event_type"].isin(match[g_["event_type"]]) & (ev["s"] <= e) & (ev["e"] >= s)]
            found[g_["event_type"]][0] += 1
            found[g_["event_type"]][1] += int(len(hit) > 0)
    for k, (n, hit) in found.items():
        assert n > 0, k
        assert hit / n >= 0.9, (k, hit, n)
