"""Distributed engine using Apache Spark (PySpark DataFrames).

  P1 load        spark.read.parquet (partition discovery on station_id), repartition by station
  P2 clean       column expressions (same rules as core/cleaning.py), hourly -> daily groupBy
  P3 stats       groupBy(station[, year|month]).agg(count, avg, var_pop, min/max of struct(value, time))
  P4 thresholds  distributed fixed-bin histograms (groupBy(station, month, bin).count()) -> the same
                 percentile code as every other engine; optional comparison with percentile_approx
                 (Greenwald-Khanna); thresholds go back to the executors via a BROADCAST join
  P5 detect      the shared rule definitions evaluated as Spark Columns, gaps-and-islands with
                 window functions (row_number) to build multi-day events
  P6 trends      groupBy(station).applyInPandas(...) running the shared indices / Mann-Kendall code

Spark is lazy; to get per-phase timings each phase ends with cache() + an action (count/collect).
Session start-up (JVM launch) is measured separately as startup_s.

    python -m engines.spark_engine --data data/parquet/synthetic --master "local[8]"
    spark-submit --master spark://master:7077 engines/spark_engine.py --data /data/parquet/synthetic
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pyspark.sql import SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from core.aggregates import (DAILY_STAT_VARS, HOURLY_STAT_VARS, MEAN, M2, N, STAT_VARS,  # noqa: E402
                             frame_to_vec, merge_stats_frames)
from core.cleaning import HOURLY_VARS, hi_parts  # noqa: E402
from core.detection import EVENT_COLS, build_rules  # noqa: E402
from core.outputs import write_outputs  # noqa: E402
from core.pipeline import read_manifest, station_path  # noqa: E402
from core.thresholds import (HIST_VARS, MOMENT_VARS, empty_arrays, hist_spec,  # noqa: E402
                             thresholds_from_arrays)
from core.timing import PhaseTimer  # noqa: E402
from core.trends import INDEX_COLS, TREND_COLS  # noqa: E402
from engines.common import base_parser, setup  # noqa: E402

INDEX_SCHEMA = ("station_id string, year long, n_days long, tmax_mean double, tmin_mean double, tmean_mean double, "
                "txx double, tnn double, prcptot double, rx1day double, rx5day double, cdd long, cwd long, "
                "heatwave_days long, heavy_rain_days long, hot_days_p99 long, high_wind_days long")
TREND_SCHEMA = ("station_id string, variable string, n_years long, start_year long, end_year long, "
                "ols_slope_per_decade double, ols_r2 double, sen_slope_per_decade double, mk_tau double, "
                "mk_z double, mk_p double, significant boolean, trend string")


class Cols:
    """Column namespace so the shared rule lambdas (c.tmax >= 40 ...) produce Spark Columns."""

    def __getattr__(self, name):
        return F.col(name)


def build_session(master: str, cfg: dict, partitions: int) -> SparkSession:
    sc = cfg["spark"]
    b = (SparkSession.builder.appName("pds-weather").master(master)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.ansi.enabled", "false")
         .config("spark.sql.shuffle.partitions", str(partitions))
         .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         .config("spark.driver.memory", sc["driver_memory"])
         .config("spark.ui.showConsoleProgress", "false")
         # Let the JIT compile Spark's large generated aggregation methods (the JVM skips methods
         # > 8000 bytecodes by default and runs them interpreted): ~2x faster wide aggregations.
         .config("spark.driver.extraJavaOptions", "-XX:-DontCompileHugeMethods")
         .config("spark.executor.extraJavaOptions", "-XX:-DontCompileHugeMethods"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # ship the shared code to executors (needed on a real cluster; harmless locally)
    tmp = tempfile.mkdtemp()
    zpath = shutil.make_archive(os.path.join(tmp, "pdsweather"), "zip", root_dir=ROOT, base_dir="core")
    spark.sparkContext.addPyFile(zpath)
    return spark


# ----------------------------------------------------------------------------- P2 expressions

def _rounded(cfg):
    r = cfg["cleaning"]["round_decimals"]
    return {v: nan_to_null(np_round(F.col(v).cast("double"), r)) for v in HOURLY_VARS}


def clean_columns(df, cfg):
    """Cleaned values: same rules as core.cleaning.clean_hourly."""
    c = cfg["cleaning"]
    x = _rounded(cfg)
    out = {}
    lo, hi = c["temp_range"]
    t = x["temp_c"]
    out["temp_c"] = F.when((t < lo) | (t > hi), F.lit(None)).otherwise(t)
    lo, hi = c["humidity_range"]
    tol = c["humidity_clip_tolerance"]
    h = x["humidity_pct"]
    out["humidity_pct"] = (F.when((h > hi + tol) | (h < lo - tol), F.lit(None))
                           .when(h > hi, F.lit(float(hi))).when(h < lo, F.lit(float(lo))).otherwise(h))
    lo, hi = c["precip_range"]
    p = x["precip_mm"]
    out["precip_mm"] = F.when((p < lo) | (p > hi), F.lit(None)).otherwise(p)
    lo, hi = c["wind_range"]
    for v in ("wind_kmh", "gust_kmh"):
        w = x[v]
        out[v] = F.when((w < lo) | (w > hi), F.lit(None)).otherwise(w)
    return df.select("station_id", "timestamp", *[out[v].alias(v) for v in HOURLY_VARS])


def quality_columns(cfg):
    """Per-station quality counters on the rounded (pre-cleaning) values; mirrors clean_hourly.

    Kept as its own small aggregation (one row per station): folding these 11 sums into the wide
    daily aggregation made that aggregation ~10x slower (oversized generated code).
    """
    c = cfg["cleaning"]
    x = _rounded(cfg)
    s = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
    tl, th = c["temp_range"]
    hl, hh = c["humidity_range"]
    tol = c["humidity_clip_tolerance"]
    pl, ph = c["precip_range"]
    wl, wh = c["wind_range"]
    h = x["humidity_pct"]
    aggs = [F.count(F.lit(1)).alias("rows_dedup")]
    aggs += [s(x[v].isNull()).alias(f"{v}_missing") for v in HOURLY_VARS]
    aggs += [
        s((x["temp_c"] < tl) | (x["temp_c"] > th)).alias("temp_c_flagged"),
        s(((h > hh) & (h <= hh + tol)) | ((h < hl) & (h >= hl - tol))).alias("humidity_pct_clipped"),
        s((h > hh + tol) | (h < hl - tol)).alias("humidity_pct_flagged"),
        s((x["precip_mm"] < pl) | (x["precip_mm"] > ph)).alias("precip_mm_flagged"),
        s((x["wind_kmh"] < wl) | (x["wind_kmh"] > wh)).alias("wind_kmh_flagged"),
        s((x["gust_kmh"] < wl) | (x["gust_kmh"] > wh)).alias("gust_kmh_flagged"),
    ]
    return aggs


def heat_index_col(t_c, rh):
    """Same expression (shared hi_parts) and operation order as core.cleaning.heat_index_c."""
    T, simple, roth = hi_parts(t_c, rh, F.lit)
    adj1 = F.when((rh < 13.0) & (T >= 80.0) & (T <= 112.0),
                  ((F.lit(13.0) - rh) / 4.0) * F.sqrt(F.greatest((F.lit(17.0) - F.abs(T - 95.0)) / 17.0, F.lit(0.0)))
                  ).otherwise(0.0)
    adj2 = F.when((rh > 85.0) & (T >= 80.0) & (T <= 87.0), ((rh - 85.0) / 10.0) * ((F.lit(87.0) - T) / 5.0)).otherwise(0.0)
    hi_f = F.when((simple + T) / 2.0 < 80.0, simple).otherwise(roth - adj1 + adj2)
    return (hi_f - 32.0) * 5.0 / 9.0


def np_round(c, decimals: int):
    """numpy's np.round(x, d) == rint(x * 10^d) / 10^d, as plain double arithmetic.

    Bit-for-bit identical to the pandas engines and far cheaper than F.bround (Java BigDecimal),
    which matters because the optimiser inlines cleaned columns into every expression using them.
    """
    scale = float(10 ** decimals)
    return F.rint(c * scale) / scale


def nan_to_null(c):
    """Spark orders NaN above every number (NaN > 0 is TRUE); pandas treats NaN as missing. Use null."""
    return F.when(F.isnan(c), F.lit(None)).otherwise(c)


# ----------------------------------------------------------------------------- P3 helpers

# "min/max and WHEN it happened" as a single fixed-width long, so Spark can use its fast hash
# aggregate (min/max over a struct forces a sort-based aggregate: ~4x slower here).
#   key = (round(v * 1e4) + 1e8) << 32 | seconds          min(key) -> min value, earliest time
#   key = (round(v * 1e4) + 1e8) << 32 | (2^32-1-seconds) max(key) -> max value, earliest time
# Exact because cleaned values are on a 1e-4 grid (rounded to <= 4 decimals) and |v| < 10^4.
_VSCALE, _VOFF, _TBITS = 10_000, 100_000_000, 32


def _packed(x, secs, for_max: bool):
    vk = (F.round(x * _VSCALE) + _VOFF).cast("long")
    t = (F.lit((1 << _TBITS) - 1) - secs) if for_max else secs
    return F.shiftleft(vk, _TBITS) + t


def stats_aggs(vars_, tcol):
    secs = F.col(tcol).cast("timestamp").cast("long")
    aggs = []
    for v in vars_:
        x = F.col(v)
        aggs += [F.count(x).alias(f"{v}__count"), F.avg(x).alias(f"{v}__mean"),
                 (F.var_pop(x) * F.count(x)).alias(f"{v}__m2"),
                 # coalesce -> never null, so the column stays int64 in pandas (float64 would
                 # silently drop the low bits of the 60-bit key)
                 F.coalesce(F.min(_packed(x, secs, False)), F.lit(-1)).alias(f"{v}__mn"),
                 F.coalesce(F.max(_packed(x, secs, True)), F.lit(-1)).alias(f"{v}__mx")]
    return aggs


def _unpack(keys: pd.Series, for_max: bool):
    ki = keys.to_numpy()
    assert ki.dtype == np.int64, ki.dtype
    ok = ki >= 0
    vk = ki >> _TBITS
    t = ki & ((1 << _TBITS) - 1)
    if for_max:
        t = ((1 << _TBITS) - 1) - t
    v = np.where(ok, (vk - _VOFF) / _VSCALE, np.nan)
    when = pd.to_datetime(np.where(ok, t, 0), unit="s").where(ok)
    return v, when


def wide_to_partials(pdf: pd.DataFrame, vars_) -> pd.DataFrame:
    """Wide Spark aggregate rows (station, year, month) -> long partial-stat rows."""
    rows = []
    for v in vars_:
        mn, tmn = _unpack(pdf[f"{v}__mn"], False)
        mx, tmx = _unpack(pdf[f"{v}__mx"], True)
        f = pd.DataFrame({
            "station_id": pdf["station_id"], "year": pdf["year"].astype(np.int64),
            "month": pdf["month"].astype(np.int64), "variable": v,
            "count": pdf[f"{v}__count"].astype(np.int64), "mean": pdf[f"{v}__mean"], "m2": pdf[f"{v}__m2"],
            "min": mn, "min_time": tmn, "max": mx, "max_time": tmx})
        rows.append(f[f["count"] > 0])
    return pd.concat(rows, ignore_index=True)


def levels_from_partials(part: pd.DataFrame) -> pd.DataFrame:
    """Exact Chan merge of (station, year, month) partials up to the overall / year / month levels."""
    keys = ["station_id", "level", "period", "variable"]
    out = []
    for level, col in (("overall", None), ("year", "year"), ("month", "month")):
        f = part.assign(level=level, period=part[col] if col else 0)
        out.append(merge_stats_frames(f, keys))
    return pd.concat(out, ignore_index=True)


# ----------------------------------------------------------------------------- P6 UDFs

def make_indices_fn(cfg):
    def fn(pdf: pd.DataFrame) -> pd.DataFrame:
        from core.detection import build_rules as _br
        from core.trends import yearly_indices as _yi
        pdf = pdf.copy()
        pdf["date"] = pd.to_datetime(pdf["date"])
        pdf = pdf.sort_values("date").set_index("date")
        full = pd.date_range(pdf.index.min(), pdf.index.max(), freq="D", name="date")
        pdf = pdf.reindex(full).reset_index()
        pdf["year"], pdf["month"] = pdf["date"].dt.year, pdf["date"].dt.month
        sid = pdf["station_id"].dropna().iloc[0]
        out = _yi(pdf, sid, {r.event_type: r for r in _br(cfg)}, cfg)
        return out.astype({"year": "int64", "n_days": "int64", "cdd": "int64", "cwd": "int64",
                           "heatwave_days": "int64", "heavy_rain_days": "int64", "hot_days_p99": "int64",
                           "high_wind_days": "int64"})
    return fn


def make_trends_fn(cfg):
    def fn(pdf: pd.DataFrame) -> pd.DataFrame:
        from core.trends import station_trends as _st
        out = _st(pdf.sort_values("year"), cfg)
        return out.astype({"n_years": "int64", "start_year": "int64", "end_year": "int64", "significant": bool})
    return fn


# ----------------------------------------------------------------------------- pipeline

def run(spark, data, cfg, stations=None, compare_approx=False, out_extra: dict | None = None):
    timer = PhaseTimer()
    t0 = time.perf_counter()
    manifest = read_manifest(data)
    ids = stations or sorted(manifest["stations"])
    nparts = int(spark.conf.get("spark.sql.shuffle.partitions"))
    rules = build_rules(cfg)
    C = Cols()

    # P1 load: one shuffle (hash-partition by station); every later groupBy has station_id in its keys,
    # so Spark reuses this partitioning instead of shuffling again ---------------------------
    with timer.phase("load"):
        paths = [str(station_path(data, s)) for s in ids]
        raw = (spark.read.option("basePath", str(data)).parquet(*paths)
               .select("station_id", "timestamp", *HOURLY_VARS)
               .repartition(nparts, "station_id").cache())
        raw.count()
        rows = int(sum(manifest["stations"][s]["rows"] for s in ids))

    # P2 dedup (cached: narrow, reused by everything below), quality counters, clean, daily --------
    with timer.phase("clean"):
        dedup = raw.dropDuplicates(["station_id", "timestamp"]).cache()
        dedup.count()
        raw.unpersist()
        qual = dedup.groupBy("station_id").agg(*quality_columns(cfg)).toPandas()
        qual["rows_in"] = qual["station_id"].map(lambda s: manifest["stations"][s]["rows"])
        qual["duplicates"] = qual["rows_in"] - qual["rows_dedup"]
        qual = qual.drop(columns=["rows_dedup"])
        # hourly is NOT cached: caching this wide 13M-row frame costs far more than recomputing it
        # from the cached dedup table; it is used twice (daily here, stats in P3).
        hourly = (clean_columns(dedup, cfg)
                  .withColumn("hi", heat_index_col(F.col("temp_c"), F.col("humidity_pct")))
                  .withColumn("year", F.year("timestamp")).withColumn("month", F.month("timestamp")))
        daily = (hourly.groupBy("station_id", F.to_date("timestamp").alias("date")).agg(
            F.count("temp_c").alias("n_hours"),
            F.max("temp_c").alias("tmax"), F.min("temp_c").alias("tmin"), F.avg("temp_c").alias("tmean"),
            F.avg("humidity_pct").alias("rh_mean"), F.max("humidity_pct").alias("rh_max"),
            F.min("humidity_pct").alias("rh_min"),
            np_round(F.sum("precip_mm"), 4).alias("precip_sum"), F.max("precip_mm").alias("precip_max_1h"),
            F.max("wind_kmh").alias("wind_max"), F.max("gust_kmh").alias("gust_max"),
            np_round(F.max("hi"), 4).alias("hi_max"))
            .withColumn("year", F.year("date").cast("long")).withColumn("month", F.month("date").cast("long"))
            .withColumn("complete", F.col("n_hours") >= cfg["cleaning"]["min_hours_complete"])
            .cache())
        daily.count()

    # P3 stats: ONE fine-grained pass (station, year, month), then exact Chan merges up to the
    # overall / year / month / global levels (mergeable partial aggregates) ----------------
    with timer.phase("stats"):
        a = hourly.groupBy("station_id", "year", "month").agg(*stats_aggs(HOURLY_STAT_VARS, "timestamp")).toPandas()
        b = daily.groupBy("station_id", "year", "month").agg(*stats_aggs(DAILY_STAT_VARS, "date")).toPandas()
        part = pd.concat([wide_to_partials(a, HOURLY_STAT_VARS), wide_to_partials(b, DAILY_STAT_VARS)],
                         ignore_index=True)
        stats = levels_from_partials(part)
        g = merge_stats_frames(stats[stats["level"] == "overall"].assign(station_id="ALL", level="global", period=0),
                               ["station_id", "level", "period", "variable"])
        gvec = frame_to_vec(g, STAT_VARS)
        dedup.unpersist()

    # P4 thresholds: distributed histograms -> shared percentile code -> broadcast join ---
    with timer.phase("thresholds"):
        S = len(ids)
        sidx = {s: i for i, s in enumerate(ids)}
        mom, hist = empty_arrays(S, cfg)
        maggs = []
        for v in MOMENT_VARS:
            maggs += [F.count(v).alias(f"{v}__n"), F.avg(v).alias(f"{v}__mean"),
                      (F.var_pop(v) * F.count(v)).alias(f"{v}__m2")]
        mp = daily.groupBy("station_id", "month").agg(*maggs).toPandas()
        for r in mp.itertuples(index=False):
            r = r._asdict()
            for j, v in enumerate(MOMENT_VARS):
                if r[f"{v}__n"]:
                    mom[sidx[r["station_id"]], r["month"] - 1, j, [N, MEAN, M2]] = [
                        r[f"{v}__n"], r[f"{v}__mean"], r[f"{v}__m2"]]
        parts = []
        wet = cfg["thresholds"]["wet_day_mm"]
        for v in HIST_VARS:
            lo, w, nb = hist_spec(cfg, v)
            src = "precip_sum" if v == "precip_wet" else v
            x = F.col(src)
            d = daily.where(x >= wet) if v == "precip_wet" else daily.where(x.isNotNull())
            b = F.least(F.greatest(F.floor((x - lo) / w), F.lit(0)), F.lit(nb - 1)).cast("int")
            parts.append(d.select("station_id", "month", F.lit(v).alias("var"), b.alias("bin")))
        hc = parts[0]
        for p_ in parts[1:]:
            hc = hc.unionByName(p_)
        hc = hc.groupBy("station_id", "month", "var", "bin").count().toPandas()
        for v, gdf in hc.groupby("var"):
            si = gdf["station_id"].map(sidx).to_numpy()
            np.add.at(hist[v], (si, gdf["month"].to_numpy() - 1, gdf["bin"].to_numpy()), gdf["count"].to_numpy())
        thr = thresholds_from_arrays(mom, hist, ids, cfg)
        thr_sdf = spark.createDataFrame(thr)
        thr_sdf = thr_sdf.select(*[nan_to_null(F.col(c)).alias(c) if t == "double" else F.col(c)
                                   for c, t in thr_sdf.dtypes])
        dthr = daily.join(F.broadcast(thr_sdf), ["station_id", "month"], "left").cache()
        dthr.count()

    if compare_approx and out_extra is not None:
        acc = cfg["spark"]["approx_accuracy"]
        ap = daily.groupBy("station_id", "month").agg(
            F.percentile_approx("tmax", [0.90, 0.99], acc).alias("tmax_q"),
            F.percentile_approx("tmin", [0.01], acc).alias("tmin_q"),
            F.percentile_approx("wind_max", [0.99], acc).alias("wind_q")).toPandas()
        ap["tmax_p90_approx"] = ap["tmax_q"].map(lambda q: q[0])
        ap["tmax_p99_approx"] = ap["tmax_q"].map(lambda q: q[1])
        ap["tmin_p01_approx"] = ap["tmin_q"].map(lambda q: q[0])
        ap["wind_p99_approx"] = ap["wind_q"].map(lambda q: q[0])
        cmp_ = thr.merge(ap.drop(columns=["tmax_q", "tmin_q", "wind_q"]), on=["station_id", "month"])
        for c in ("tmax_p90", "tmax_p99", "tmin_p01", "wind_p99"):
            cmp_[f"{c}_absdiff"] = (cmp_[c] - cmp_[f"{c}_approx"]).abs()
        out_extra["percentile_comparison"] = cmp_

    # P5 detection: shared rules as Columns + gaps-and-islands with window functions -----
    with timer.phase("detect"):
        flag_parts = []
        for k, rule in enumerate(rules):
            cond = F.coalesce(rule.cond(C), F.lit(False))
            lv = [F.when(F.coalesce(c_(C), F.lit(False)), F.lit(i)).otherwise(F.lit(0))
                  for i, (_, c_) in enumerate(rule.levels[1:], start=1)]
            lvl = F.greatest(F.lit(0), *lv) if lv else F.lit(0)
            flag_parts.append(dthr.where(cond).select(
                "station_id", "date", F.lit(k).alias("rule"),
                (F.col(rule.value) * float(rule.direction)).alias("sval"), lvl.alias("lvl")))
        flags = flag_parts[0]
        for p_ in flag_parts[1:]:
            flags = flags.unionByName(p_)
        day = F.datediff(F.col("date"), F.lit("1970-01-01").cast("date"))
        w = Window.partitionBy("station_id", "rule").orderBy("date")
        ev = (flags.withColumn("day", day).withColumn("grp", F.col("day") - F.row_number().over(w))
              .groupBy("station_id", "rule", "grp")
              .agg(F.min("date").alias("start_date"), F.max("date").alias("end_date"),
                   F.count(F.lit(1)).alias("duration_days"), F.max("lvl").alias("lvl"),
                   F.max(F.struct(F.col("sval"), (-F.col("day")).alias("nd"))).alias("peak"))
              .toPandas())
        events = []
        for k, rule in enumerate(rules):
            e = ev[(ev["rule"] == k) & (ev["duration_days"] >= rule.min_len)]
            if e.empty:
                continue
            names = np.array([n for n, _ in rule.levels])
            start = pd.to_datetime(e["start_date"]).to_numpy().astype("datetime64[D]")
            events.append(pd.DataFrame({
                "event_id": [f"{s}_{rule.event_type}_{str(d).replace('-', '')}" for s, d in zip(e["station_id"], start)],
                "station_id": e["station_id"].to_numpy(), "event_type": rule.event_type, "method": rule.method,
                "severity": names[e["lvl"].to_numpy()], "start_date": start,
                "end_date": pd.to_datetime(e["end_date"]).to_numpy().astype("datetime64[D]"),
                "duration_days": e["duration_days"].to_numpy().astype(np.int64),
                "peak_value": np.round(np.array([p["sval"] for p in e["peak"]]) * rule.direction, 4),
                "peak_date": (np.array([-p["nd"] for p in e["peak"]], dtype="int64")).astype("datetime64[D]"),
            }))
        events = pd.concat(events, ignore_index=True)[EVENT_COLS] if events else pd.DataFrame(columns=EVENT_COLS)

    # P6 indices + trends with pandas UDFs ------------------------------------------------
    with timer.phase("trends"):
        ind_sdf = dthr.groupBy("station_id").applyInPandas(make_indices_fn(cfg), INDEX_SCHEMA).cache()
        indices = ind_sdf.toPandas()[INDEX_COLS]
        trends = ind_sdf.groupBy("station_id").applyInPandas(make_trends_fn(cfg), TREND_SCHEMA).toPandas()[TREND_COLS]

    with timer.phase("merge"):
        q = {k: int(qual[k].sum()) for k in qual.columns if k != "station_id"}
        daily.unpersist()
        combined = {"stats": stats, "thresholds": thr, "events": events, "indices": indices, "trends": trends,
                    "global_vec": gvec, "quality": q, "quality_rows": qual,
                    "precip_hist": hist["precip_wet"].sum(axis=(0, 1)), "rows": rows, "stations": S}
    return combined, timer.to_dict(), time.perf_counter() - t0


def main(argv=None):
    ap = base_parser(__doc__)
    ap.add_argument("--master", default=None, help='e.g. "local[8]" or spark://master:7077')
    ap.add_argument("--cores", "-p", type=int, default=os.cpu_count(), help="cores for local mode")
    ap.add_argument("--partitions", type=int, help="spark.sql.shuffle.partitions")
    ap.add_argument("--compare-approx", action="store_true", help="also compute percentile_approx and compare")
    args = ap.parse_args(argv)
    cfg, data, out = setup(args)
    master = args.master or os.environ.get("SPARK_MASTER") or f"local[{args.cores}]"
    partitions = args.partitions or (max(2 * args.cores, 4) if master.startswith("local") else cfg["spark"]["shuffle_partitions"])
    t0 = time.perf_counter()
    spark = build_session(master, cfg, partitions)
    startup = time.perf_counter() - t0
    if master.startswith("local"):
        p, nodes = args.cores, 1
    else:
        sc = spark.sparkContext
        p = int(sc.defaultParallelism)
        nodes = max(1, len(sc._jsc.sc().getExecutorMemoryStatus().keySet()) - 1)
    extra = {}
    combined, timings, total = run(spark, data, cfg, args.stations, args.compare_approx, extra)
    run_info = {"engine": "spark", "p": p, "nodes": nodes, "partition_mode": "", "schedule": master,
                "total_s": total, "startup_s": startup, "tag": args.tag}
    run_dir = write_outputs(out, combined, cfg, run_info, timings, write_files=not args.no_write, verbose=not args.quiet)
    if "percentile_comparison" in extra and not args.no_write:
        cmp_ = extra["percentile_comparison"]
        cmp_.to_csv(run_dir / "percentile_comparison.csv", index=False)
        cols = [c for c in cmp_.columns if c.endswith("_absdiff")]
        print("percentile_approx vs histogram (abs diff, °C or km/h):")
        print(cmp_[cols].describe().loc[["mean", "50%", "max"]].round(3).to_string())
    print(f"Spark startup         : {startup:.2f} s (excluded from total)") if not args.quiet else None
    spark.stop()


if __name__ == "__main__":
    main()
