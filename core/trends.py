"""P6: yearly climate indices and long-term trends (OLS, Mann-Kendall test, Sen's slope).

Mann-Kendall is implemented here (no pymannkendall dependency): S statistic with tie-corrected
variance, normal approximation with continuity correction, two-sided p-value.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .detection import Rule

INDEX_COLS = ["station_id", "year", "n_days", "tmax_mean", "tmin_mean", "tmean_mean", "txx", "tnn",
              "prcptot", "rx1day", "rx5day", "cdd", "cwd", "heatwave_days", "heavy_rain_days",
              "hot_days_p99", "high_wind_days"]

TREND_VARS = ["tmax_mean", "tmin_mean", "tmean_mean", "txx", "tnn", "prcptot", "rx1day", "rx5day", "cdd",
              "heatwave_days", "heavy_rain_days", "hot_days_p99"]

TREND_COLS = ["station_id", "variable", "n_years", "start_year", "end_year", "ols_slope_per_decade",
              "ols_r2", "sen_slope_per_decade", "mk_tau", "mk_z", "mk_p", "significant", "trend"]


def _longest_run(mask: np.ndarray) -> int:
    if not mask.any():
        return 0
    m = np.concatenate([[False], mask, [False]]).astype(np.int8)
    d = np.diff(m)
    starts, ends = np.flatnonzero(d == 1), np.flatnonzero(d == -1)
    return int((ends - starts).max())


def yearly_indices(daily: pd.DataFrame, station_id: str, rules: dict[str, Rule], cfg: dict) -> pd.DataFrame:
    """ETCCDI-style indices per calendar year. `daily` has thresholds attached (needs heatwave rule).

    All indices are computed within a calendar year, so year-aligned chunks can compute them
    independently (used by the MPI time-partitioned mode).
    """
    if daily.empty:
        return pd.DataFrame(columns=INDEX_COLS)
    wet = cfg["thresholds"]["wet_day_mm"]
    d = daily.sort_values("date")
    flags = {
        "heatwave_days": rules["heatwave"].cond(d),
        "heavy_rain_days": rules["heavy_rain"].cond(d),
        "hot_days_p99": rules["hot_day_p99"].cond(d),
        "high_wind_days": rules["high_wind"].cond(d),
    }
    flags = {k: np.asarray(pd.Series(v).fillna(False), dtype=bool) for k, v in flags.items()}
    rows = []
    years = d["year"].to_numpy()
    for y in np.unique(years):
        sel = years == y
        yd = d.loc[sel]
        p = yd["precip_sum"].to_numpy(dtype=np.float64)
        p_ok = ~np.isnan(p)
        r5 = pd.Series(p).rolling(5, min_periods=5).sum()
        rows.append({
            "station_id": station_id, "year": int(y),
            "n_days": int(yd["tmax"].notna().sum()),
            "tmax_mean": yd["tmax"].mean(), "tmin_mean": yd["tmin"].mean(), "tmean_mean": yd["tmean"].mean(),
            "txx": yd["tmax"].max(), "tnn": yd["tmin"].min(),
            "prcptot": float(np.nansum(np.where(p >= wet, p, 0.0))) if p_ok.any() else np.nan,
            "rx1day": float(np.nanmax(p)) if p_ok.any() else np.nan,
            "rx5day": float(r5.max()) if r5.notna().any() else np.nan,
            "cdd": _longest_run(p_ok & (p < wet)),
            "cwd": _longest_run(p_ok & (p >= wet)),
            **{k: int(v[sel].sum()) for k, v in flags.items()},
        })
    out = pd.DataFrame(rows)[INDEX_COLS]
    for c in ("tmax_mean", "tmin_mean", "tmean_mean", "prcptot", "rx5day"):
        out[c] = out[c].round(6)
    return out


def ols_slope(x: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    xm, ym = x.mean(), y.mean()
    sxx = ((x - xm) ** 2).sum()
    if sxx == 0:
        return np.nan, np.nan
    b = ((x - xm) * (y - ym)).sum() / sxx
    resid = y - (ym + b * (x - xm))
    sst = ((y - ym) ** 2).sum()
    r2 = 1 - (resid ** 2).sum() / sst if sst > 0 else np.nan
    return float(b), float(r2)


def mann_kendall(y: np.ndarray) -> tuple[float, float, float, float]:
    """Return (S, tau, z, p) for the two-sided Mann-Kendall trend test."""
    y = np.asarray(y, dtype=np.float64)
    n = y.size
    diff = np.sign(y[None, :] - y[:, None])
    s = float(np.triu(diff, k=1).sum())
    _, counts = np.unique(y, return_counts=True)
    ties = counts[counts > 1]
    var = (n * (n - 1) * (2 * n + 5) - (ties * (ties - 1) * (2 * ties + 5)).sum()) / 18.0
    if var <= 0:
        return s, 0.0, 0.0, 1.0
    if s > 0:
        z = (s - 1) / math.sqrt(var)
    elif s < 0:
        z = (s + 1) / math.sqrt(var)
    else:
        z = 0.0
    p = math.erfc(abs(z) / math.sqrt(2))
    tau = s / (n * (n - 1) / 2)
    return s, tau, z, p


def sens_slope(x: np.ndarray, y: np.ndarray) -> float:
    i, j = np.triu_indices(x.size, k=1)
    dx = x[j] - x[i]
    ok = dx != 0
    if not ok.any():
        return np.nan
    return float(np.median((y[j] - y[i])[ok] / dx[ok]))


def station_trends(indices: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    tc = cfg["trends"]
    if indices.empty:
        return pd.DataFrame(columns=TREND_COLS)
    station_id = indices["station_id"].iloc[0]
    ind = indices[indices["n_days"] >= tc["min_days_per_year"]].sort_values("year")
    rows = []
    for var in TREND_VARS:
        sub = ind[["year", var]].dropna()
        if len(sub) < tc["min_years"]:
            continue
        x = sub["year"].to_numpy(dtype=np.float64)
        y = sub[var].to_numpy(dtype=np.float64)
        b, r2 = ols_slope(x, y)
        _, tau, z, p = mann_kendall(y)
        sen = sens_slope(x, y)
        sig = bool(p < tc["alpha"])
        rows.append({
            "station_id": station_id, "variable": var, "n_years": len(sub),
            "start_year": int(x.min()), "end_year": int(x.max()),
            "ols_slope_per_decade": b * 10, "ols_r2": r2, "sen_slope_per_decade": sen * 10,
            "mk_tau": tau, "mk_z": z, "mk_p": p, "significant": sig,
            "trend": ("increasing" if z > 0 else "decreasing") if sig else "no trend",
        })
    return pd.DataFrame(rows, columns=TREND_COLS)
