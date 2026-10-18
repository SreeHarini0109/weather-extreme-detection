"""Synthetic hourly weather generator with injected extremes (known ground truth).

Model (per station, hourly, UTC):
  Temperature  T = t_mean + A_season*cos(2π(doy-peak)/365.25) + A_daily*cos(2π(local_hour-15)/24)
                   + trend*years + AR(1) noise (3-day memory) + white noise
  Humidity     inversely related to temperature anomaly / diurnal cycle, wetter in the rainy season,
               +12 % during rain hours, clipped to [5, 100]
  Rainfall     two-stage: Bernoulli wet day (season-weighted, von Mises shaped monsoon) x Gamma(0.8)
               amount, split over hours with Gamma(0.15) shares (bursty), slowly intensifying trend
  Wind         Weibull(k=2) marginal via Gaussian AR(1) copula (12 h memory), diurnal modulation;
               gust = wind x U(1.3, 1.8)
  Extremes     heatwaves (+6..10 °C, 3-7 days), cold waves (-6..10 °C, 3-6 days), cloudbursts
               (100-180 mm in one hour), cyclones on coastal stations (75-150 km/h, 80-250 mm/day)
               -> written to ground_truth_events.csv so detection precision/recall can be measured.
  Data quality NaNs, sentinel spikes (999.9 °C, 135 % RH, -1 mm, 999 km/h) and duplicate rows are
               injected so the cleaning phase has real work to do.

Each station is generated independently with a deterministic seed derived from its id, so the
generator itself runs in parallel (one process per station) and is reproducible.

    python data/generator.py --stations 50 --years 30 --workers 8
    python data/generator.py --rows-target 100M --workers 8
"""
from __future__ import annotations

import argparse
import math
import sys
import time
import zlib
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.signal import lfilter
from scipy.special import ndtr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.config import load_config, resolve  # noqa: E402

OUT_COLS = ["station_id", "station_name", "lat", "lon", "elevation", "timestamp",
            "temp_c", "humidity_pct", "precip_mm", "wind_kmh", "gust_kmh"]


def parse_count(s: str | None) -> int | None:
    if s is None:
        return None
    s = str(s).strip().upper()
    mult = {"K": 1e3, "M": 1e6, "B": 1e9}
    if s[-1] in mult:
        return int(float(s[:-1]) * mult[s[-1]])
    return int(float(s))


def station_seed(station_id: str, seed: int) -> int:
    return (seed * 1_000_003 + zlib.crc32(station_id.encode())) % (2**32)


def build_station_table(base: pd.DataFrame, n: int, seed: int, heterogeneous: bool = False,
                        y0: int = 1995, y1: int = 2024) -> pd.DataFrame:
    """Use the base stations; beyond len(base), create perturbed virtual stations.

    heterogeneous: every station gets its own record start year (10-30 years of data, like a real
    station network) so stations differ in size and load balancing matters.
    """
    rows = []
    for i in range(n):
        b = base.iloc[i % len(base)].to_dict()
        k = i // len(base)
        if k > 0:
            rng = np.random.default_rng(station_seed(f"{b['station_id']}{k}", seed))
            b["station_id"] = f"{b['station_id']}{k:03d}"
            b["station_name"] = f"{b['station_name']} #{k}"
            b["lat"] = round(b["lat"] + rng.uniform(-1, 1), 2)
            b["lon"] = round(b["lon"] + rng.uniform(-1, 1), 2)
            b["t_mean"] = b["t_mean"] + rng.uniform(-1.5, 1.5)
            b["rain_annual_mm"] = b["rain_annual_mm"] * rng.uniform(0.8, 1.2)
            b["wind_scale_kmh"] = b["wind_scale_kmh"] * rng.uniform(0.85, 1.15)
        b["first_year"] = y0
        if heterogeneous:
            rng = np.random.default_rng(station_seed(f"{b['station_id']}-start", seed))
            b["first_year"] = int(rng.integers(y0, max(y0, y1 - 9) + 1))
        rows.append(b)
    return pd.DataFrame(rows)


def _ar1(rng, n, phi, sd):
    """Stationary Gaussian AR(1) series with marginal std `sd`."""
    eps = rng.normal(0.0, sd * math.sqrt(1 - phi * phi), n)
    eps[0] = rng.normal(0.0, sd)
    return lfilter([1.0], [1.0, -phi], eps)


def _von_mises_weight(doy, peak, s):
    kappa = 4.0 * s + 0.01
    w = np.exp(kappa * (np.cos(2 * np.pi * (doy - peak) / 365.25) - 1.0))
    grid = np.arange(1, 366)
    mean_w = np.exp(kappa * (np.cos(2 * np.pi * (grid - peak) / 365.25) - 1.0)).mean()
    return w / mean_w


def generate_station(st: dict, y0: int, y1: int, gcfg: dict, seed: int):
    sid = st["station_id"]
    rng = np.random.default_rng(station_seed(sid, seed))
    y0 = max(y0, int(st.get("first_year", y0)))
    ts = pd.date_range(f"{y0}-01-01", f"{y1}-12-31 23:00", freq="h")
    n = len(ts)
    n_days = n // 24
    doy = ts.dayofyear.to_numpy()
    hour = ts.hour.to_numpy()
    years = (ts - ts[0]).total_seconds().to_numpy() / (365.25 * 86400)
    local_hour = (hour + st["lon"] / 15.0) % 24
    days = pd.date_range(f"{y0}-01-01", f"{y1}-12-31", freq="D")
    ddoy = days.dayofyear.to_numpy()
    dyears = np.arange(n_days) / 365.25

    # --- true long-term trend (ground truth)
    lo, hi = gcfg["warming_per_decade"]
    trend = rng.uniform(-0.03, 0.03) if rng.random() < gcfg["no_trend_fraction"] else rng.uniform(lo, hi)
    rain_trend = rng.uniform(0.0, 0.06)   # fractional intensity increase per decade

    # --- temperature
    A_s, A_d = st["t_amp_season"], st["t_amp_daily"]
    seasonal = A_s * np.cos(2 * np.pi * (doy - st["t_peak_doy"]) / 365.25)
    diurnal = A_d * np.cos(2 * np.pi * (local_hour - 15.0) / 24.0)
    sigma = 1.2 + 0.18 * A_s
    ar_t = _ar1(rng, n, math.exp(-1 / 72), sigma)
    temp = st["t_mean"] + seasonal + diurnal + trend * years / 10.0 + ar_t + rng.normal(0, 0.3, n)

    # --- rainfall (daily two-stage, then split into hours)
    s = st["rain_seasonality"]
    w_norm = _von_mises_weight(ddoy, st["rain_peak_doy"], s)
    w_total = (1 - s) + s * w_norm
    expected = st["rain_annual_mm"] / 365.25 * w_total * (1 + rain_trend * dyears / 10.0)
    p0 = float(np.clip(0.12 + st["rain_annual_mm"] / 8000.0, 0.12, 0.45))
    p_wet = np.clip(p0 * w_total, 0.01, 0.9)
    wet = rng.random(n_days) < p_wet
    amount = np.where(wet, rng.gamma(0.8, (expected / p_wet) / 0.8), 0.0)
    shares = rng.gamma(0.15, 1.0, (n_days, 24))
    shares /= shares.sum(axis=1, keepdims=True)
    precip = (amount[:, None] * shares).reshape(-1)

    # --- humidity
    w_hourly = np.repeat(np.clip(w_norm, 0, 3), 24)
    rh = (st["rh_base"] + 10.0 * s * (w_hourly - 1.0) - 2.0 * diurnal - 1.5 * ar_t
          + _ar1(rng, n, math.exp(-1 / 24), 5.0) + 12.0 * (precip > 0.2))

    # --- wind (Weibull k=2 via Gaussian copula)
    z = _ar1(rng, n, math.exp(-1 / 12), 1.0)
    u = np.clip(ndtr(z), 1e-12, 1 - 1e-12)
    c = st["wind_scale_kmh"] / 0.886
    wind = c * np.sqrt(-np.log(1 - u)) * (1 + 0.25 * np.cos(2 * np.pi * (local_hour - 14.0) / 24.0))
    gust = wind * rng.uniform(1.3, 1.8, n)

    # --- injected extremes (ground truth)
    inj = gcfg["inject"]
    gt = []
    day_index = {d: i for i, d in enumerate(days.normalize())}

    def day_range(year, center_doy, spread, dur):
        start = int(round(center_doy + rng.uniform(-spread, spread)))
        start = (start - 1) % 365 + 1
        d0 = pd.Timestamp(year=year, month=1, day=1) + pd.Timedelta(days=start - 1)
        i0 = day_index.get(d0)
        if i0 is None or i0 + dur > n_days:
            return None
        return i0

    for yi, year in enumerate(range(y0, y1 + 1)):
        growth = 1 + inj["rate_growth_per_year"] * yi
        for _ in range(rng.poisson(inj["heatwave_rate"] * growth)):
            dur = int(rng.integers(3, 8))
            i0 = day_range(year, st["t_peak_doy"], 40, dur)
            if i0 is None:
                continue
            mag = rng.uniform(6, 10)
            sl = slice(i0 * 24, (i0 + dur) * 24)
            temp[sl] += mag
            rh[sl] -= 15
            gt.append(("heatwave", i0, i0 + dur - 1, round(mag, 2), "°C"))
        for _ in range(rng.poisson(inj["coldwave_rate"] * growth)):
            dur = int(rng.integers(3, 7))
            i0 = day_range(year, st["t_peak_doy"] + 182, 40, dur)
            if i0 is None:
                continue
            mag = rng.uniform(6, 10)
            temp[i0 * 24:(i0 + dur) * 24] -= mag
            gt.append(("cold_wave", i0, i0 + dur - 1, round(mag, 2), "°C"))
        for _ in range(rng.poisson(inj["cloudburst_rate"] * growth)):
            i0 = day_range(year, st["rain_peak_doy"], 45, 1)
            if i0 is None:
                continue
            h = i0 * 24 + int(rng.integers(1, 23))
            amt = rng.uniform(100, 180)
            precip[h] += amt
            precip[h - 1] += rng.uniform(10, 40)
            precip[h + 1] += rng.uniform(10, 40)
            rh[max(h - 2, 0):h + 4] = 98
            gt.append(("cloudburst", i0, i0, round(amt, 1), "mm/h"))
        if st["coastal"]:
            for _ in range(rng.poisson(inj["cyclone_rate"] * growth)):
                dur = int(rng.integers(1, 4))
                i0 = day_range(year, st["rain_peak_doy"], 60, dur)
                if i0 is None:
                    continue
                N = dur * 24
                sl = slice(i0 * 24, i0 * 24 + N)
                prof = np.sin(np.pi * (np.arange(N) + 0.5) / N) ** 1.5
                peak = rng.uniform(75, 150)
                wind[sl] = np.maximum(wind[sl], peak * prof)
                gust[sl] = np.maximum(gust[sl], wind[sl] * rng.uniform(1.2, 1.5, N))
                daily_rain = rng.uniform(80, 250)
                precip[sl] += daily_rain / 24.0 * 2.0 * prof
                rh[sl] = rng.uniform(95, 100, N)
                temp[sl] -= 2.0
                gt.append(("cyclone", i0, i0 + dur - 1, round(peak, 1), "km/h"))

    rh = np.clip(rh, 5, 100)
    # soft saturation of hot extremes (land-atmosphere feedback): above 48 °C only 30 % of any
    # further anomaly is realised, so injected heatwaves on already-hot stations stay plausible
    hot = temp > 48.0
    temp[hot] = 48.0 + (temp[hot] - 48.0) * 0.3

    df = pd.DataFrame({
        "timestamp": ts.tz_localize("UTC"),
        "temp_c": np.round(temp, 1).astype(np.float32),
        "humidity_pct": np.round(rh, 0).astype(np.float32),
        "precip_mm": np.round(precip, 1).astype(np.float32),
        "wind_kmh": np.round(wind, 1).astype(np.float32),
        "gust_kmh": np.round(gust, 1).astype(np.float32),
    })

    # --- data-quality problems
    q = gcfg["quality"]
    for col in ["temp_c", "humidity_pct", "precip_mm", "wind_kmh", "gust_kmh"]:
        df.loc[rng.random(n) < q["nan_frac"], col] = np.nan
    k = max(1, int(n * q["spike_frac"]))
    df.loc[rng.choice(n, k, replace=False), "temp_c"] = rng.choice(np.float32([999.9, -999.9]), k)
    df.loc[rng.choice(n, k, replace=False), "humidity_pct"] = rng.choice(np.float32([101.5, 135.0]), k)
    df.loc[rng.choice(n, k, replace=False), "precip_mm"] = np.float32(-1.0)
    df.loc[rng.choice(n, k, replace=False), "wind_kmh"] = np.float32(999.0)
    kd = int(n * q["dup_frac"])
    if kd:
        dups = df.iloc[np.sort(rng.choice(n, kd, replace=False))]
        df = pd.concat([df, dups]).sort_values("timestamp", kind="stable").reset_index(drop=True)

    df.insert(0, "station_id", sid)
    df.insert(1, "station_name", st["station_name"])
    df.insert(2, "lat", np.float32(st["lat"]))
    df.insert(3, "lon", np.float32(st["lon"]))
    df.insert(4, "elevation", np.float32(st["elevation"]))

    gt_rows = [{
        "gt_id": f"GT_{sid}_{t}_{days[a].strftime('%Y%m%d')}", "station_id": sid, "event_type": t,
        "start_date": days[a].date().isoformat(), "end_date": days[b].date().isoformat(),
        "magnitude": m, "unit": u} for t, a, b, m, u in gt]
    truth = {"station_id": sid, "true_trend_c_per_decade": round(trend, 4),
             "true_rain_intensity_trend_per_decade": round(rain_trend, 4)}
    return df[OUT_COLS], gt_rows, truth


def _worker(args):
    st, y0, y1, gcfg, seed, out_dir, fmt = args
    t0 = time.perf_counter()
    df, gt, truth = generate_station(st, y0, y1, gcfg, seed)
    out = Path(out_dir) / f"{st['station_id']}.{fmt}"
    if fmt == "csv":
        df.to_csv(out, index=False)
    else:
        df.to_parquet(out, index=False)
    return st["station_id"], len(df), gt, truth, time.perf_counter() - t0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config")
    ap.add_argument("--stations", type=int, help="number of stations (default from config)")
    ap.add_argument("--years", type=int, help="number of years starting at start_year")
    ap.add_argument("--start-year", type=int)
    ap.add_argument("--rows-target", help="e.g. 13M, 100M - overrides --stations")
    ap.add_argument("--workers", type=int)
    ap.add_argument("--format", choices=["parquet", "csv"])
    ap.add_argument("--out", help="raw output directory")
    ap.add_argument("--seed", type=int)
    ap.add_argument("--heterogeneous", action="store_true",
                    help="random record start year per station (10-30 years) -> unequal station sizes")
    a = ap.parse_args(argv)

    cfg = load_config(a.config)
    g = cfg["generator"]
    y0 = a.start_year or g["start_year"]
    y1 = y0 + a.years - 1 if a.years else g["end_year"]
    seed = a.seed if a.seed is not None else g["seed"]
    workers = a.workers or g["workers"]
    fmt = a.format or g["format"]
    hours = len(pd.date_range(f"{y0}-01-01", f"{y1}-12-31 23:00", freq="h"))
    n_st = a.stations or g["n_stations"]
    rows_target = parse_count(a.rows_target)
    if rows_target:
        n_st = max(1, math.ceil(rows_target / hours * (1.5 if a.heterogeneous else 1.0)))
    out_dir = resolve(a.out or cfg["paths"]["raw_synthetic"])
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in list(out_dir.glob("*.parquet")) + list(out_dir.glob("*.csv")):
        old.unlink()

    base = pd.read_csv(resolve(cfg["paths"]["stations"]))
    table = build_station_table(base, n_st, seed, a.heterogeneous, y0, y1)
    print(f"Generating {n_st} stations x {y1 - y0 + 1} years ({y0}-{y1}) = ~{n_st * hours:,} rows "
          f"with {workers} workers -> {out_dir} [{fmt}]")
    t0 = time.perf_counter()
    jobs = [(row, y0, y1, g, seed, str(out_dir), fmt) for row in table.to_dict("records")]
    gts, truths, total = [], [], 0
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for sid, nrows, gt, truth, dt in ex.map(_worker, jobs):
            total += nrows
            gts.extend(gt)
            truths.append(truth)
    gt_df = pd.DataFrame(gts, columns=["gt_id", "station_id", "event_type", "start_date", "end_date", "magnitude", "unit"])
    gt_df.sort_values(["station_id", "start_date", "event_type"]).to_csv(out_dir / "ground_truth_events.csv", index=False)
    pd.DataFrame(truths).sort_values("station_id").to_csv(out_dir / "ground_truth_trends.csv", index=False)
    table.to_csv(out_dir / "stations_used.csv", index=False)
    dt = time.perf_counter() - t0
    print(f"Done: {total:,} rows, {len(gt_df)} injected events in {dt:.1f} s "
          f"({total / dt / 1e6:.2f} M rows/s)")
    print(gt_df["event_type"].value_counts().to_string())


if __name__ == "__main__":
    main()
