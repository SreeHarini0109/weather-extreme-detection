"""P4: station-month climatology and percentile thresholds from mergeable fixed-bin histograms.

Exact percentiles need all values in one place. Instead every worker builds fixed-bin histograms
per (station, month); histograms from different workers are simply *summed* (MPI Allreduce SUM,
Spark groupBy-count), and percentiles are read from the cumulative histogram. The error is bounded
by half a bin width and - crucially - the result is identical no matter how the data was split.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .aggregates import M2, MEAN, N, VEC_WIDTH, empty_vec, merge_vec

MOMENT_VARS = ["tmax", "tmin", "wind_max"]          # climatological mean/std (z-score, departures)
HIST_VARS = ["tmax", "tmin", "precip_wet", "wind_max"]

THRESHOLD_COLS = [
    "station_id", "month", "n_days",
    "tmax_mean", "tmax_std", "tmin_mean", "tmin_std", "wind_mean", "wind_std",
    "tmax_p90", "tmax_p99", "tmin_p01", "n_wet", "precip_wet_p99", "precip_wet_q1", "precip_wet_q3",
    "wind_p99", "wind_q1", "wind_q3",
]


def hist_spec(cfg: dict, var: str) -> tuple[float, float, int]:
    h = cfg["histograms"][var]
    nb = int(round((h["hi"] - h["lo"]) / h["width"]))
    return float(h["lo"]), float(h["width"]), nb


def bin_index(x: np.ndarray, lo: float, width: float, nb: int) -> np.ndarray:
    """floor((x - lo) / width) clipped to [0, nb-1]. Spark uses the same expression."""
    idx = np.floor((x - lo) / width)
    return np.clip(idx, 0, nb - 1).astype(np.int64)


def hist_values(daily: pd.DataFrame, var: str, cfg: dict) -> tuple[np.ndarray, np.ndarray]:
    """Values (and their months) that enter the histogram for `var`."""
    if var == "precip_wet":
        x = daily["precip_sum"].to_numpy(dtype=np.float64)
        ok = x >= cfg["thresholds"]["wet_day_mm"]
    else:
        x = daily[var].to_numpy(dtype=np.float64)
        ok = ~np.isnan(x)
    return x[ok], daily["month"].to_numpy()[ok]


def empty_arrays(n_stations: int, cfg: dict) -> tuple[np.ndarray, dict[str, np.ndarray]]:
    mom = np.stack([empty_vec(len(MOMENT_VARS)) for _ in range(n_stations * 12)]).reshape(
        n_stations, 12, len(MOMENT_VARS), VEC_WIDTH)
    hist = {v: np.zeros((n_stations, 12, hist_spec(cfg, v)[2]), dtype=np.int64) for v in HIST_VARS}
    return mom, hist


def climatology_arrays(daily: pd.DataFrame, cfg: dict, n_stations: int = 1, s: int = 0,
                       mom=None, hist=None):
    """Accumulate one station's daily data into moment / histogram arrays at station index `s`."""
    if mom is None:
        mom, hist = empty_arrays(n_stations, cfg)
    months = daily["month"].to_numpy()
    for j, var in enumerate(MOMENT_VARS):
        x = daily[var].to_numpy(dtype=np.float64)
        ok = ~np.isnan(x)
        for m in range(1, 13):
            sel = x[ok & (months == m)]
            if sel.size == 0:
                continue
            v = empty_vec(1)
            v[0, N] = sel.size
            v[0, MEAN] = sel.mean()
            v[0, M2] = ((sel - v[0, MEAN]) ** 2).sum()
            mom[s, m - 1, j] = merge_vec(mom[s, m - 1, j], v)[0]
    for var in HIST_VARS:
        lo, w, nb = hist_spec(cfg, var)
        x, mo = hist_values(daily, var, cfg)
        if x.size:
            np.add.at(hist[var][s], (mo - 1, bin_index(x, lo, w, nb)), 1)
    return mom, hist


def hist_percentile(counts: np.ndarray, q: float, lo: float, width: float) -> np.ndarray:
    """Percentile (bin centre) from histograms along the last axis; NaN where the histogram is empty."""
    total = counts.sum(axis=-1)
    rank = np.maximum(np.ceil(q * total - 1e-9), 1)
    cum = np.cumsum(counts, axis=-1)
    k = (cum >= rank[..., None]).argmax(axis=-1)
    val = lo + (k + 0.5) * width
    return np.where(total > 0, val, np.nan)


def thresholds_from_arrays(mom: np.ndarray, hist: dict[str, np.ndarray], station_ids: list[str],
                           cfg: dict) -> pd.DataFrame:
    """Build the station x month thresholds table from (merged) moments and histograms."""
    S = len(station_ids)
    n = mom[..., N]
    with np.errstate(invalid="ignore", divide="ignore"):
        mean = np.where(n > 0, mom[..., MEAN], np.nan)
        std = np.where(n > 0, np.sqrt(mom[..., M2] / n), np.nan)

    def pct(var, q):
        lo, w, _ = hist_spec(cfg, var)
        return hist_percentile(hist[var], q, lo, w)

    df = pd.DataFrame({
        "station_id": np.repeat(station_ids, 12),
        "month": np.tile(np.arange(1, 13), S),
        "n_days": n[..., 0].reshape(-1).astype(np.int64),
        "tmax_mean": mean[..., 0].reshape(-1), "tmax_std": std[..., 0].reshape(-1),
        "tmin_mean": mean[..., 1].reshape(-1), "tmin_std": std[..., 1].reshape(-1),
        "wind_mean": mean[..., 2].reshape(-1), "wind_std": std[..., 2].reshape(-1),
        "tmax_p90": pct("tmax", 0.90).reshape(-1), "tmax_p99": pct("tmax", 0.99).reshape(-1),
        "tmin_p01": pct("tmin", 0.01).reshape(-1),
        "n_wet": hist["precip_wet"].sum(axis=-1).reshape(-1).astype(np.int64),
        "precip_wet_p99": pct("precip_wet", 0.99).reshape(-1),
        "precip_wet_q1": pct("precip_wet", 0.25).reshape(-1),
        "precip_wet_q3": pct("precip_wet", 0.75).reshape(-1),
        "wind_p99": pct("wind_max", 0.99).reshape(-1),
        "wind_q1": pct("wind_max", 0.25).reshape(-1), "wind_q3": pct("wind_max", 0.75).reshape(-1),
    })
    return df[THRESHOLD_COLS]


def attach_thresholds(daily: pd.DataFrame, thr: pd.DataFrame) -> pd.DataFrame:
    """Join the station-month thresholds onto each daily row (broadcast join in Spark)."""
    t = thr.drop(columns=["station_id"]) if "station_id" in thr else thr
    return daily.merge(t, on="month", how="left", sort=False)


# ----------------------------------------------------------------------------- severity scales

def heat_index_category(hi_c: float) -> str:
    """NWS heat-index caution categories (°C)."""
    if math.isnan(hi_c) or hi_c < 27:
        return "none"
    if hi_c < 32:
        return "caution"
    if hi_c < 41:
        return "extreme_caution"
    if hi_c < 54:
        return "danger"
    return "extreme_danger"


def beaufort(wind_kmh: float) -> int:
    """Beaufort number from a wind speed in km/h."""
    edges = [1, 6, 12, 20, 29, 39, 50, 62, 75, 89, 103, 118]
    return int(np.searchsorted(edges, wind_kmh, side="right"))
