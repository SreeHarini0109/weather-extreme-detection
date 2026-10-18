"""P2: cleaning, validation and hourly -> daily aggregation.

The rules here are mirrored exactly by the Spark engine (engines/spark_engine.py), so any change
must be made in both places (the parity test will catch a mismatch).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

HOURLY_VARS = ["temp_c", "humidity_pct", "precip_mm", "wind_kmh", "gust_kmh"]

DAILY_COLS = [
    "date", "year", "month", "n_hours", "complete",
    "tmax", "tmin", "tmean", "rh_mean", "rh_max", "rh_min",
    "precip_sum", "precip_max_1h", "wind_max", "gust_max", "hi_max",
]


def clean_hourly(df: pd.DataFrame, cfg: dict) -> tuple[pd.DataFrame, dict]:
    """Deduplicate, round and range-check hourly observations.

    Returns the cleaned frame (sorted by timestamp, float64 values) and a dict of quality counters.
    Invalid values become NaN (flagged) rather than being dropped, so no timestamps are lost.
    """
    c = cfg["cleaning"]
    q = {"rows_in": int(len(df))}
    df = df.sort_values("timestamp", kind="stable")
    dup = df.duplicated("timestamp", keep="first")
    q["duplicates"] = int(dup.sum())
    df = df.loc[~dup, ["timestamp", *HOURLY_VARS]].reset_index(drop=True)

    out = {"timestamp": df["timestamp"].to_numpy()}
    for v in HOURLY_VARS:
        x = np.round(df[v].to_numpy(dtype=np.float64), c["round_decimals"])
        q[f"{v}_missing"] = int(np.isnan(x).sum())
        out[v] = x

    lo, hi = c["temp_range"]
    t = out["temp_c"]
    bad = (t < lo) | (t > hi)
    q["temp_c_flagged"] = int(bad.sum())
    t[bad] = np.nan

    lo, hi = c["humidity_range"]
    tol = c["humidity_clip_tolerance"]
    h = out["humidity_pct"]
    clip_hi = (h > hi) & (h <= hi + tol)
    clip_lo = (h < lo) & (h >= lo - tol)
    bad = (h > hi + tol) | (h < lo - tol)
    q["humidity_pct_clipped"] = int(clip_hi.sum() + clip_lo.sum())
    q["humidity_pct_flagged"] = int(bad.sum())
    h[clip_hi] = hi
    h[clip_lo] = lo
    h[bad] = np.nan

    lo, hi = c["precip_range"]
    p = out["precip_mm"]
    bad = (p < lo) | (p > hi)
    q["precip_mm_flagged"] = int(bad.sum())
    p[bad] = np.nan

    lo, hi = c["wind_range"]
    for v in ("wind_kmh", "gust_kmh"):
        w = out[v]
        bad = (w < lo) | (w > hi)
        q[f"{v}_flagged"] = int(bad.sum())
        w[bad] = np.nan

    return pd.DataFrame(out), q


def hi_parts(t_c, rh, L=float):
    """Heat-index building blocks written once for numpy arrays AND Spark Columns.

    L wraps literals (float for numpy, F.lit for Spark) so both engines evaluate the exact same
    operations in the same order (floating point is not associative).
    """
    T = t_c * L(9.0) / L(5.0) + L(32.0)
    simple = L(0.5) * (T + L(61.0) + (T - L(68.0)) * L(1.2) + rh * L(0.094))
    roth = (L(-42.379) + L(2.04901523) * T + L(10.14333127) * rh - L(0.22475541) * T * rh
            - L(0.00683783) * T * T - L(0.05481717) * rh * rh + L(0.00122874) * T * T * rh
            + L(0.00085282) * T * rh * rh - L(0.00000199) * T * T * rh * rh)
    return T, simple, roth


def heat_index_c(t_c, rh):
    """NWS heat index (Rothfusz regression, Steadman fallback below 80 °F, low/high RH adjustments), in °C."""
    T, simple, roth = hi_parts(t_c, rh)
    with np.errstate(invalid="ignore"):
        adj1 = np.where((rh < 13.0) & (T >= 80.0) & (T <= 112.0),
                        ((13.0 - rh) / 4.0) * np.sqrt(np.clip((17.0 - np.abs(T - 95.0)) / 17.0, 0, None)), 0.0)
    adj2 = np.where((rh > 85.0) & (T >= 80.0) & (T <= 87.0), ((rh - 85.0) / 10.0) * ((87.0 - T) / 5.0), 0.0)
    hi_f = np.where((simple + T) / 2.0 < 80.0, simple, roth - adj1 + adj2)
    hi_f = np.where(np.isnan(T) | np.isnan(rh), np.nan, hi_f)
    return (hi_f - 32.0) * 5.0 / 9.0


def to_daily(hourly: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Aggregate cleaned hourly data to daily values (UTC days), reindexed to a continuous date range."""
    if hourly.empty:
        return pd.DataFrame(columns=DAILY_COLS)
    h = hourly
    day = h["timestamp"].dt.floor("D")
    hi = heat_index_c(h["temp_c"].to_numpy(), h["humidity_pct"].to_numpy())
    tmp = pd.DataFrame({
        "date": day, "temp_c": h["temp_c"], "humidity_pct": h["humidity_pct"],
        "precip_mm": h["precip_mm"], "wind_kmh": h["wind_kmh"], "gust_kmh": h["gust_kmh"], "hi": hi,
    })
    g = tmp.groupby("date", sort=True)
    d = g.agg(
        n_hours=("temp_c", "count"),
        tmax=("temp_c", "max"), tmin=("temp_c", "min"), tmean=("temp_c", "mean"),
        rh_mean=("humidity_pct", "mean"), rh_max=("humidity_pct", "max"), rh_min=("humidity_pct", "min"),
        precip_max_1h=("precip_mm", "max"),
        wind_max=("wind_kmh", "max"), gust_max=("gust_kmh", "max"), hi_max=("hi", "max"),
    )
    d["precip_sum"] = g["precip_mm"].sum(min_count=1)
    # Sums / formula outputs are rounded to absorb summation-order noise (64.49999999 vs 64.5).
    # Means are NOT rounded: means of 24 values hit exact .xxxx5 ties often, where half-even and
    # half-up rounding disagree between engines.
    for col in ("precip_sum", "hi_max"):
        d[col] = d[col].round(4)

    full = pd.date_range(d.index.min(), d.index.max(), freq="D", name="date")
    d = d.reindex(full)
    d["n_hours"] = d["n_hours"].fillna(0).astype(np.int64)
    d["complete"] = d["n_hours"] >= cfg["cleaning"]["min_hours_complete"]
    d = d.reset_index()
    d["year"] = d["date"].dt.year.astype(np.int64)
    d["month"] = d["date"].dt.month.astype(np.int64)
    return d[DAILY_COLS]


def empty_quality() -> dict:
    keys = ["rows_in", "duplicates"]
    for v in HOURLY_VARS:
        keys.append(f"{v}_missing")
    keys += ["temp_c_flagged", "humidity_pct_clipped", "humidity_pct_flagged", "precip_mm_flagged",
             "wind_kmh_flagged", "gust_kmh_flagged"]
    return {k: 0 for k in keys}


def merge_quality(a: dict, b: dict) -> dict:
    out = dict(a)
    for k, v in b.items():
        out[k] = out.get(k, 0) + v
    return out
