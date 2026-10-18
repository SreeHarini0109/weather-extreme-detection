"""P3: mergeable summary statistics (count, mean, M2, min, max, argmin_time, argmax_time).

Partial aggregates from disjoint chunks of data merge *exactly* with Chan et al.'s parallel
variance formula, which is what lets MPI / multiprocessing / Spark combine per-worker results
without moving raw data:

    n   = na + nb
    d   = mean_b - mean_a
    mean = mean_a + d * nb / n
    M2  = M2_a + M2_b + d^2 * na * nb / n          (std = sqrt(M2 / n), population)

Ties on min/max are broken by the earliest timestamp so every engine reports the same time.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

STAT_FIELDS = ["count", "mean", "m2", "min", "max", "min_time", "max_time"]
# vector layout used for the MPI custom reduction: one row per variable
N, MEAN, M2, MIN, MAX, TMIN, TMAX = range(7)
VEC_WIDTH = 7

HOURLY_STAT_VARS = ["temp_c", "humidity_pct", "precip_mm", "wind_kmh", "gust_kmh"]
DAILY_STAT_VARS = ["precip_sum"]
STAT_VARS = HOURLY_STAT_VARS + DAILY_STAT_VARS
LEVELS = {"overall": None, "year": "year", "month": "month"}


# ----------------------------------------------------------------------------- vectors

def empty_vec(nvars: int) -> np.ndarray:
    v = np.zeros((nvars, VEC_WIDTH))
    v[:, MEAN] = 0.0
    v[:, MIN] = np.inf
    v[:, MAX] = -np.inf
    v[:, TMIN] = np.nan
    v[:, TMAX] = np.nan
    return v


def merge_vec(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Chan's merge of two (nvars, 7) partial-stat arrays. Pure function, vectorised over rows."""
    a = np.asarray(a, dtype=np.float64).reshape(-1, VEC_WIDTH)
    b = np.asarray(b, dtype=np.float64).reshape(-1, VEC_WIDTH)
    out = np.empty_like(a)
    na, nb = a[:, N], b[:, N]
    n = na + nb
    with np.errstate(invalid="ignore", divide="ignore"):
        d = b[:, MEAN] - a[:, MEAN]
        mean = np.where(n > 0, a[:, MEAN] + d * np.where(n > 0, nb / n, 0.0), 0.0)
        m2 = a[:, M2] + b[:, M2] + np.where(n > 0, d * d * na * nb / n, 0.0)
    mean = np.where(na == 0, b[:, MEAN], np.where(nb == 0, a[:, MEAN], mean))
    m2 = np.where(na == 0, b[:, M2], np.where(nb == 0, a[:, M2], m2))
    out[:, N], out[:, MEAN], out[:, M2] = n, mean, m2

    # min with earliest-time tie break
    take_b = (b[:, MIN] < a[:, MIN]) | ((b[:, MIN] == a[:, MIN]) & (b[:, TMIN] < a[:, TMIN])) | (
        (b[:, MIN] == a[:, MIN]) & np.isnan(a[:, TMIN]) & ~np.isnan(b[:, TMIN]))
    out[:, MIN] = np.where(take_b, b[:, MIN], a[:, MIN])
    out[:, TMIN] = np.where(take_b, b[:, TMIN], a[:, TMIN])
    take_b = (b[:, MAX] > a[:, MAX]) | ((b[:, MAX] == a[:, MAX]) & (b[:, TMAX] < a[:, TMAX])) | (
        (b[:, MAX] == a[:, MAX]) & np.isnan(a[:, TMAX]) & ~np.isnan(b[:, TMAX]))
    out[:, MAX] = np.where(take_b, b[:, MAX], a[:, MAX])
    out[:, TMAX] = np.where(take_b, b[:, TMAX], a[:, TMAX])
    return out


def vec_from_values(values: np.ndarray, times: np.ndarray | None = None) -> np.ndarray:
    """Partial stats of a 1-D array (NaNs ignored). times: datetime64 array or epoch seconds."""
    v = empty_vec(1)
    x = np.asarray(values, dtype=np.float64)
    ok = ~np.isnan(x)
    if not ok.any():
        return v
    xs = x[ok]
    v[0, N] = xs.size
    v[0, MEAN] = xs.mean()
    v[0, M2] = ((xs - v[0, MEAN]) ** 2).sum()
    v[0, MIN] = xs.min()
    v[0, MAX] = xs.max()
    if times is not None:
        t = _to_epoch(np.asarray(times)[ok])
        v[0, TMIN] = t[np.argmax(xs == v[0, MIN])]
        v[0, TMAX] = t[np.argmax(xs == v[0, MAX])]
    return v


def _to_epoch(t: np.ndarray) -> np.ndarray:
    if np.issubdtype(t.dtype, np.datetime64):
        return t.astype("datetime64[s]").astype(np.int64).astype(np.float64)
    return t.astype(np.float64)


def frame_to_vec(stats: pd.DataFrame, variables: list[str]) -> np.ndarray:
    """Convert overall rows of a long stats frame (one station) into a (nvars, 7) vector."""
    v = empty_vec(len(variables))
    idx = stats.set_index("variable")
    for i, var in enumerate(variables):
        if var not in idx.index:
            continue
        r = idx.loc[var]
        if r["count"] == 0:
            continue
        v[i] = [r["count"], r["mean"], r["m2"], r["min"], r["max"],
                _ts_to_epoch(r["min_time"]), _ts_to_epoch(r["max_time"])]
    return v


def _ts_to_epoch(ts) -> float:
    if pd.isna(ts):
        return np.nan
    return float(pd.Timestamp(ts).value // 10**9)


def vec_to_frame(v: np.ndarray, variables: list[str]) -> pd.DataFrame:
    rows = []
    for i, var in enumerate(variables):
        n = v[i, N]
        rows.append({
            "variable": var, "count": int(n),
            "mean": v[i, MEAN] if n else np.nan, "m2": v[i, M2] if n else np.nan,
            "min": v[i, MIN] if n else np.nan, "max": v[i, MAX] if n else np.nan,
            "min_time": pd.to_datetime(v[i, TMIN], unit="s") if n and not np.isnan(v[i, TMIN]) else pd.NaT,
            "max_time": pd.to_datetime(v[i, TMAX], unit="s") if n and not np.isnan(v[i, TMAX]) else pd.NaT,
        })
    return pd.DataFrame(rows)


# ----------------------------------------------------------------------------- frames

def grouped_stats(df: pd.DataFrame, time_col: str, var_cols: list[str], key: str | None) -> pd.DataFrame:
    """Partial stats per group (key column, or the whole frame when key is None).

    Returns a long frame: [period, variable, count, mean, m2, min, min_time, max, max_time].
    `df` must be sorted by time so that the first occurrence of a min/max is the earliest.
    Vectorised with np.bincount (count / sum / M2) and one cython groupby for min / max.
    """
    if key:   # small integer keys (year / month): offset codes, no sorting needed
        k = df[key].to_numpy().astype(np.int64)
        lo = int(k.min()) if k.size else 0
        codes = k - lo
        periods = np.arange(lo, lo + (int(codes.max()) + 1 if k.size else 0))
    else:
        periods, codes = np.zeros(1, dtype=np.int64), np.zeros(len(df), dtype=np.int64)
    G = len(periods)
    t_all = df[time_col].to_numpy()
    out = []
    for v in var_cols:
        x = df[v].to_numpy(dtype=np.float64)
        ok = ~np.isnan(x)
        if not ok.any():
            continue
        g, xs, ts = codes[ok], x[ok], t_all[ok]
        count = np.bincount(g, minlength=G)
        with np.errstate(invalid="ignore", divide="ignore"):
            mean = np.bincount(g, xs, minlength=G) / count
        m2 = np.bincount(g, (xs - mean[g]) ** 2, minlength=G)
        mm = pd.DataFrame({"g": g, "x": xs}).groupby("g", sort=True)["x"].agg(["min", "max"])
        gmin = np.full(G, np.nan)
        gmax = np.full(G, np.nan)
        gmin[mm.index.to_numpy()] = mm["min"].to_numpy()
        gmax[mm.index.to_numpy()] = mm["max"].to_numpy()
        tmin = np.full(G, np.datetime64("NaT"), dtype=t_all.dtype)
        tmax = tmin.copy()
        for target, tout in ((gmin, tmin), (gmax, tmax)):
            hit = xs == target[g]
            grp, first = np.unique(g[hit], return_index=True)
            tout[grp] = ts[hit][first]
        f = pd.DataFrame({"period": periods, "variable": v, "count": count, "mean": mean, "m2": m2,
                          "min": gmin, "max": gmax, "min_time": tmin, "max_time": tmax})
        out.append(f[f["count"] > 0])
    if not out:
        return pd.DataFrame(columns=["period", "variable", *STAT_FIELDS])
    res = pd.concat(out, ignore_index=True)
    return res[["period", "variable", *STAT_FIELDS]]


def station_stats(hourly: pd.DataFrame, daily: pd.DataFrame, station_id: str) -> pd.DataFrame:
    """All stats levels (overall / year / month) for one station's (cleaned) hourly + daily data."""
    h = hourly
    h = h.assign(year=h["timestamp"].dt.year.astype(np.int64), month=h["timestamp"].dt.month.astype(np.int64))
    frames = []
    for level, key in LEVELS.items():
        a = grouped_stats(h, "timestamp", HOURLY_STAT_VARS, key)
        b = grouped_stats(daily, "date", DAILY_STAT_VARS, key)
        f = pd.concat([a, b], ignore_index=True)
        f.insert(0, "level", level)
        frames.append(f)
    out = pd.concat(frames, ignore_index=True)
    out.insert(0, "station_id", station_id)
    return out


def merge_stats_frames(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Merge partial-stat rows sharing the same keys (multi-way Chan merge, exact)."""
    if df.empty:
        return df
    df = df[df["count"] > 0].copy()
    df["_s"] = df["count"] * df["mean"]
    tot = df.groupby(keys, sort=True)[["count", "_s"]].transform("sum")
    df["_m2"] = df["m2"] + df["count"] * (df["mean"] - tot["_s"] / tot["count"]) ** 2
    out = df.groupby(keys, sort=True).agg(count=("count", "sum"), _s=("_s", "sum"), m2=("_m2", "sum")).reset_index()
    out["mean"] = out["_s"] / out["count"]
    mins = df.sort_values([*keys, "min", "min_time"]).drop_duplicates(keys)[[*keys, "min", "min_time"]]
    maxs = df.assign(_neg=-df["max"]).sort_values([*keys, "_neg", "max_time"]).drop_duplicates(keys)[
        [*keys, "max", "max_time"]]
    out = out.merge(mins, on=keys).merge(maxs, on=keys)
    return out[[*keys, *STAT_FIELDS]]


def finalize_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Add std and format the summary table for output."""
    out = df.copy()
    out["std"] = np.sqrt(out["m2"] / out["count"])
    out["period"] = [_fmt_period(l, p) for l, p in zip(out["level"], out["period"])]
    cols = ["station_id", "level", "period", "variable", "count", "mean", "std", "min", "min_time", "max", "max_time"]
    order = {"global": 0, "overall": 1, "year": 2, "month": 3}
    out["_o"] = out["level"].map(order)
    out = out.sort_values(["_o", "station_id", "period", "variable"]).drop(columns="_o")
    return out[cols].reset_index(drop=True)


def _fmt_period(level, p) -> str:
    if level in ("overall", "global"):
        return "all"
    if level == "month":
        return f"{int(p):02d}"
    return str(int(p))
