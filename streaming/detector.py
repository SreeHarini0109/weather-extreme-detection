"""Spark Structured Streaming detector (step 2 of the streaming extension).

    landing dir (Parquet files from streaming/replayer.py, or any live source writing the same schema)
      -> readStream (file source, maxFilesPerTrigger)
      -> withWatermark(timestamp) + dropDuplicates(station_id, timestamp)     [stateful]
      -> cleaning expressions shared with the batch Spark engine
      -> groupBy(station_id, window(timestamp, "1 day")).agg(daily aggregates)  [stateful, append mode]
         a day is emitted ONCE, when the watermark passes its end (all its hours have arrived)
      -> foreachBatch: station-month thresholds from a batch run + the shared rule definitions,
         incremental event building (core.streaming.StreamDetector) -> alerts

Outputs (results/stream/<run>/, results/stream_latest points at the newest run):
    alerts.jsonl   one JSON alert per line (NEW / ESCALATED / ENDED), with detection latency
    daily.csv      every finalized day as it was analysed
    events.csv     closed events (same schema as the batch extreme_events.csv)
    active.csv     confirmed events still running
    metrics.csv    one row per micro-batch (input rows, rates, latency)
    progress.json  live status for the dashboard; parity.json at the end (stream vs batch events)

Fault tolerance: Spark checkpoints the file offsets and the dedup/window state; the detector state is
saved as JSON after every micro-batch and already-processed batch ids are skipped, so after a crash
`--resume` continues where it stopped without losing or duplicating alerts.

    python -m streaming.detector --baseline results/era5_latest --exit-when-done
    python -m streaming.detector --master spark://master:7077 ...   (Docker cluster)
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F  # noqa: E402

from core.cleaning import HOURLY_VARS  # noqa: E402
from core.config import load_config, resolve  # noqa: E402
from core.pipeline import read_manifest  # noqa: E402
from core.streaming import StreamDetector, alert_message, compare_with_batch  # noqa: E402
from engines.spark_engine import build_session, clean_columns, daily_aggs, heat_index_col  # noqa: E402

INPUT_DDL = "station_id string, timestamp timestamp, " + ", ".join(f"{v} double" for v in HOURLY_VARS)
DAILY_OUT = ["station_id", "date", "n_hours", "tmax", "tmin", "tmean", "rh_mean", "rh_max", "rh_min",
             "precip_sum", "precip_max_1h", "wind_max", "gust_max", "hi_max"]
COLORS = {"NEW": "\033[91m", "ESCALATED": "\033[95m", "ENDED": "\033[94m"}


def build_query(spark, landing: Path, cfg: dict, max_files: int, delay: str):
    raw = (spark.readStream.schema(INPUT_DDL).option("maxFilesPerTrigger", max_files)
           .parquet(str(landing)))
    dedup = raw.withWatermark("timestamp", delay).dropDuplicates(["station_id", "timestamp"])
    hourly = clean_columns(dedup, cfg).withColumn("hi", heat_index_col(F.col("temp_c"), F.col("humidity_pct")))
    daily = (hourly.groupBy("station_id", F.window("timestamp", "1 day")).agg(*daily_aggs())
             .withColumn("date", F.to_date(F.col("window.start"))).drop("window"))
    return daily


class Sink:
    """foreachBatch target: runs the incremental detector and writes the live output files."""

    def __init__(self, cfg, thresholds, run_dir: Path, ckpt: Path, landing: Path, names: dict, quiet: bool,
                 resume: bool):
        self.det = StreamDetector(cfg, thresholds)
        self.run_dir, self.ckpt, self.landing, self.names, self.quiet = run_dir, ckpt, landing, names, quiet
        self.state_path = ckpt / "detector_state.json"
        self.last_batch = -1
        if resume and self.state_path.exists():
            s = json.loads(self.state_path.read_text())
            self.det.load_state(s)
            self.last_batch = s["batch_id"]
            print(f"resumed detector state after batch {self.last_batch} "
                  f"({self.det.days_seen} days, {len(self.det.events)} closed events)", flush=True)
        self.n_alerts = sum(1 for _ in open(run_dir / "alerts.jsonl")) if (run_dir / "alerts.jsonl").exists() else 0
        self.lock = threading.Lock()
        self.last_wall = time.time()
        self._replay_log = None

    def completion_wall(self, dates: pd.Series) -> np.ndarray:
        """Wall-clock time at which the replayer had delivered each day's last hour (NaN if unknown)."""
        p = self.landing / "_replay_log.csv"
        if not p.exists():
            return np.full(len(dates), np.nan)
        log = pd.read_csv(p)
        if log.empty:
            return np.full(len(dates), np.nan)
        until = pd.to_datetime(log["sim_until"], utc=True).dt.tz_localize(None).to_numpy()
        need = (pd.to_datetime(dates) + pd.Timedelta(days=1)).to_numpy()
        i = np.searchsorted(until, need, side="left")
        wall = log["wall"].to_numpy()
        return np.where(i < len(wall), wall[np.minimum(i, len(wall) - 1)], np.nan)

    def __call__(self, batch_df, batch_id: int):
        if batch_id <= self.last_batch:            # already processed before a restart
            return
        t0 = time.perf_counter()
        pdf = batch_df.toPandas()
        now = time.time()
        with self.lock:
            alerts = []
            if not pdf.empty:
                pdf["date"] = pd.to_datetime(pdf["date"])
                pdf = pdf[DAILY_OUT].sort_values(["date", "station_id"]).reset_index(drop=True)
                lat = now - self.completion_wall(pdf["date"])
                pdf["latency_s"] = np.round(lat, 3)
                alerts = self.det.update(pdf)
                day_lat = dict(zip(zip(pdf["station_id"], pdf["date"].dt.strftime("%Y-%m-%d")), pdf["latency_s"]))
                pdf.assign(batch_id=batch_id, wall=round(now, 3)).to_csv(
                    self.run_dir / "daily.csv", mode="a", index=False, header=not (self.run_dir / "daily.csv").exists())
                if alerts:
                    with open(self.run_dir / "alerts.jsonl", "a") as f:
                        for a in alerts:
                            lt = day_lat.get((a["station_id"], a["date"]))
                            rec = {"wall": round(now, 3), "batch_id": batch_id,
                                   "latency_s": None if lt is None or pd.isna(lt) else float(lt),
                                   **a, "message": alert_message(a, self.names)}
                            f.write(json.dumps(rec) + "\n")
                            if not self.quiet:
                                print(f"{COLORS.get(a['kind'], '')}[{a['date']}] {rec['message']}\033[0m", flush=True)
                    self.n_alerts += len(alerts)
                self.det.closed().to_csv(self.run_dir / "events.csv", index=False)
                self.det.active().to_csv(self.run_dir / "active.csv", index=False)
            proc = time.perf_counter() - t0
            lat = pdf["latency_s"] if not pdf.empty else pd.Series(dtype=float)
            row = {"batch_id": batch_id, "wall": round(now, 3), "days": int(len(pdf)), "alerts": len(alerts),
                   "latency_median_s": float(lat.median()) if lat.notna().any() else np.nan,
                   "latency_max_s": float(lat.max()) if lat.notna().any() else np.nan,
                   "detect_s": round(proc, 4),
                   "sim_date": pdf["date"].max().strftime("%Y-%m-%d") if not pdf.empty else ""}
            mp = self.run_dir / "metrics_batches.csv"
            pd.DataFrame([row]).to_csv(mp, mode="a", index=False, header=not mp.exists())
            self.det.save(self.state_path, batch_id)
            self.last_batch = batch_id
            self.last_wall = now


def as_dict(p) -> dict:
    """PySpark 4 returns StreamingQueryProgress objects, older versions plain dicts."""
    if p is None:
        return {}
    return p if isinstance(p, dict) else json.loads(p.json)


def processed_rows(ckpt: Path, landing: Path) -> int:
    """Hourly rows in the landing files Spark has committed (from its file-source log).

    Spark's numInputRows progress metric under-counts for this plan (dedup + window over a columnar
    Parquet scan), so the row count is taken from the files themselves.
    """
    log = ckpt / "spark" / "sources" / "0"
    rp = landing / "_replay_log.csv"
    if not log.exists() or not rp.exists():
        return 0
    files = set()
    for f in log.iterdir():
        if f.name[0].isdigit():
            for line in f.read_text().splitlines()[1:]:
                if line.startswith("{"):
                    files.add(json.loads(line)["path"].rsplit("/", 1)[-1])
    rows = pd.read_csv(rp)
    return int(rows.loc[rows["file"].isin(files), "rows"].sum())


def write_json(path: Path, obj) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=1, default=str))
    tmp.replace(path)


def main(argv=None):
    cfg = load_config()
    sc = cfg["streaming"]
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--landing", default=sc["landing"])
    ap.add_argument("--checkpoint", default=sc["checkpoint"])
    ap.add_argument("--out", default=sc["output"])
    ap.add_argument("--baseline", default="results/era5_latest",
                    help="batch run whose station-month thresholds define 'normal' (and whose events parity is checked against)")
    ap.add_argument("--data", help="dataset (for station names; default: from the replay description)")
    ap.add_argument("--master", default=None, help='e.g. "local[4]" or spark://master:7077')
    ap.add_argument("--cores", "-p", type=int, default=4)
    ap.add_argument("--trigger", type=float, default=sc["trigger_seconds"], help="micro-batch interval, seconds")
    ap.add_argument("--max-files", type=int, default=sc["max_files_per_trigger"])
    ap.add_argument("--watermark", default=sc["watermark_delay"])
    ap.add_argument("--scenario", default="", help="label stored with the run")
    ap.add_argument("--resume", action="store_true", help="continue from the checkpoint (after a crash/stop)")
    ap.add_argument("--exit-when-done", action="store_true",
                    help="stop once the replay has finished and its last day has been analysed")
    ap.add_argument("--idle-timeout", type=float, default=120.0,
                    help="with --exit-when-done: give up after this many seconds without a finished micro-batch "
                         "(never while one is running, e.g. while Spark recovers from a lost executor)")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args(argv)

    landing, ckpt, out = resolve(a.landing), resolve(a.checkpoint), resolve(a.out)
    baseline = resolve(a.baseline).resolve()
    landing.mkdir(parents=True, exist_ok=True)
    latest = out.parent / "stream_latest"
    if a.resume and latest.exists():
        run_dir = latest.resolve()
    else:
        if ckpt.exists():
            shutil.rmtree(ckpt)
        stamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        run_dir = out / f"{a.scenario or 'stream'}_{stamp}"
        run_dir.mkdir(parents=True)
        if latest.is_symlink() or latest.exists():
            latest.unlink()
        latest.symlink_to(os.path.relpath(run_dir, latest.parent))
    ckpt.mkdir(parents=True, exist_ok=True)

    thresholds = pd.read_csv(baseline / "thresholds.csv")
    names = {}
    data = a.data
    if not data and (landing / "_replay.json").exists():
        data = json.loads((landing / "_replay.json").read_text())["data"]
    try:
        names = {k: v["name"] for k, v in read_manifest(resolve(data or cfg["paths"]["parquet"]))["stations"].items()}
    except Exception:
        pass

    master = a.master or os.environ.get("SPARK_MASTER") or f"local[{a.cores}]"
    t0 = time.perf_counter()
    spark = build_session(master, cfg, a.cores if master.startswith("local") else cfg["spark"]["shuffle_partitions"])
    spark.conf.set("spark.sql.streaming.noDataMicroBatches.enabled", "true")
    startup = time.perf_counter() - t0
    sink = Sink(cfg, thresholds, run_dir, ckpt, landing, names, a.quiet, a.resume)
    daily = build_query(spark, landing, cfg, a.max_files, a.watermark)
    q = (daily.writeStream.outputMode("append").foreachBatch(sink)
         .option("checkpointLocation", str(ckpt / "spark"))
         .trigger(processingTime=f"{a.trigger} seconds").queryName("weather_extremes").start())

    info = {"scenario": a.scenario, "master": master, "landing": str(landing), "baseline": str(baseline),
            "watermark": a.watermark, "trigger_s": a.trigger, "max_files_per_trigger": a.max_files,
            "startup_s": round(startup, 2), "started": time.time(), "run_dir": str(run_dir)}
    print(f"streaming query started on {master} (Spark start-up {startup:.1f} s); watching {landing}", flush=True)
    print(f"output -> {run_dir}", flush=True)
    seen, first_input = {}, None
    status = "running"
    try:
        while q.isActive:
            time.sleep(1.0)
            for p in map(as_dict, q.recentProgress):
                if p["batchId"] not in seen:
                    seen[p["batchId"]] = p
                    if first_input is None and p.get("numInputRows", 0):
                        first_input = time.time()
            last = as_dict(q.lastProgress)
            replay = json.loads((landing / "_replay.json").read_text()) if (landing / "_replay.json").exists() else {}
            done = (landing / "_DONE").exists()
            with sink.lock:
                det = sink.det
                stations = replay.get("stations") or sorted(det.last_day)
                last_days = {s: (str(np.datetime64(det.last_day[s], "D")) if s in det.last_day else None)
                             for s in stations}
                prog = {**info, "status": status, "replay": replay, "replay_done": done,
                        "batches": len(seen), "last_batch_id": last.get("batchId"),
                        "rows_in": processed_rows(ckpt, landing), "days_analysed": det.days_seen, "alerts": sink.n_alerts,
                        "closed_events": len(det.events), "active_events": int(det.active().shape[0]),
                        "late_days_dropped": det.late_days,
                        "input_rows_per_s": last.get("inputRowsPerSecond"),
                        "processed_rows_per_s": last.get("processedRowsPerSecond"),
                        "trigger_ms": (last.get("durationMs") or {}).get("triggerExecution"),
                        "watermark": (last.get("eventTime") or {}).get("watermark"),
                        "state_rows": sum(o.get("numRowsTotal", 0) for o in last.get("stateOperators", [])),
                        "last_day_per_station": last_days, "updated": time.time()}
            write_json(run_dir / "progress.json", prog)
            if a.exit_when_done and done:
                end_day = replay.get("end")
                all_done = end_day and stations and all(v is not None and v >= end_day for v in last_days.values())
                busy = bool(q.status.get("isTriggerActive")) if isinstance(q.status, dict) else False
                idle = time.time() - max(sink.last_wall, (landing / "_DONE").stat().st_mtime)
                if all_done:
                    break
                if idle > a.idle_timeout and not busy:
                    print("idle timeout: stopping before every station reached the end of the replay", flush=True)
                    status = "incomplete"
                    break
        if q.exception() is not None:
            status = "failed"
            print(f"streaming query failed: {str(q.exception()).splitlines()[0]}\n"
                  "state is checkpointed: restart with --resume to continue", flush=True)
    except KeyboardInterrupt:
        print("interrupted: state is checkpointed, continue with --resume", flush=True)
        status = "stopped"
    finally:
        q.stop()
    for p in map(as_dict, q.recentProgress):      # batches that finished after the last poll
        if p["batchId"] not in seen:
            seen[p["batchId"]] = p
    if status == "running":
        status = "finished"

    # final files: per-batch Spark metrics, parity with the batch run, closing status
    mrows = [{"batch_id": p["batchId"], "input_rows": p.get("numInputRows"),
              "input_rows_per_s": p.get("inputRowsPerSecond"), "processed_rows_per_s": p.get("processedRowsPerSecond"),
              "trigger_ms": (p.get("durationMs") or {}).get("triggerExecution"),
              "watermark": (p.get("eventTime") or {}).get("watermark")} for p in seen.values()]
    pd.DataFrame(mrows).sort_values("batch_id").to_csv(run_dir / "metrics_spark.csv", index=False) if mrows else None
    replay = json.loads((landing / "_replay.json").read_text()) if (landing / "_replay.json").exists() else {}
    parity = None
    if status == "finished" and replay and (baseline / "extreme_events.csv").exists():
        batch_ev = pd.read_csv(baseline / "extreme_events.csv")
        parity = compare_with_batch(sink.det.closed(), batch_ev, replay["start"], replay["end"], replay.get("stations"))
        write_json(run_dir / "parity.json", {**parity, "window": [replay["start"], replay["end"]],
                                             "baseline": str(baseline)})
    m = pd.read_csv(run_dir / "metrics_batches.csv") if (run_dir / "metrics_batches.csv").exists() else pd.DataFrame()
    lat = m["latency_median_s"].dropna() if not m.empty else pd.Series(dtype=float)
    summary = {**info, "status": status, "replay": replay, "batches": len(seen),
               "rows_in": processed_rows(ckpt, landing),
               "days_analysed": sink.det.days_seen, "alerts": sink.n_alerts, "closed_events": len(sink.det.events),
               "active_events": int(sink.det.active().shape[0]), "late_days_dropped": sink.det.late_days,
               "stream_seconds": round(time.time() - (first_input or info["started"]), 2),
               "latency_median_s": float(lat.median()) if len(lat) else None,
               "latency_p95_s": float(lat.quantile(0.95)) if len(lat) else None,
               "parity_ok": None if parity is None else parity["ok"], "updated": time.time()}
    write_json(run_dir / "progress.json", summary)
    spark.stop()
    print(f"\n{status}: {summary['rows_in']:,} hourly rows -> {summary['days_analysed']} station-days -> "
          f"{summary['alerts']} alerts ({summary['closed_events']} closed events, {summary['active_events']} still active)")
    if summary["latency_median_s"] is not None:
        print(f"detection latency (day's last hour delivered -> alert): median {summary['latency_median_s']:.2f} s, "
              f"p95 {summary['latency_p95_s']:.2f} s")
    if parity is not None:
        print(f"parity with batch run {baseline.name}: {parity['matched']}/{parity['batch_events']} events identical"
              f" -> {'OK' if parity['ok'] else 'MISMATCH'}")
        if not parity["ok"]:
            print("  only batch:", parity["only_batch"][:10], "\n  only stream:", parity["only_stream"][:10],
                  "\n  fields:", parity["field_mismatches"][:10])


if __name__ == "__main__":
    main()
