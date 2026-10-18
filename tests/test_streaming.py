"""Streaming extension: incremental detection must reproduce the batch events.

1. core.streaming.StreamDetector fed day-chunks of random size == core.detection.detect_events.
2. End to end: replayer -> Spark Structured Streaming detector on real ERA5 data (Cyclone Amphan
   window); every comparable closed event equals the batch run's event (parity.json ok).
"""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from core.cleaning import clean_hourly, to_daily
from core.detection import build_rules, detect_events
from core.pipeline import cached_station_meta, load_station
from core.streaming import StreamDetector, compare_with_batch
from core.thresholds import attach_thresholds, climatology_arrays, thresholds_from_arrays

ROOT = Path(__file__).resolve().parent.parent
REAL = ROOT / "data/parquet/real"
BASELINE = ROOT / "results/era5_latest"
PY = sys.executable


@pytest.mark.skipif(not REAL.exists(), reason="ERA5 data not downloaded")
@pytest.mark.parametrize("sid", ["DEL", "CCU", "MAA"])
def test_incremental_equals_batch(cfg, sid):
    rules = build_rules(cfg)
    daily = to_daily(clean_hourly(load_station(REAL, sid), cfg)[0], cfg)
    mom, hist = climatology_arrays(daily, cfg)
    thr = thresholds_from_arrays(mom, hist, [sid], cfg, cached_station_meta(REAL))
    batch = detect_events(attach_thresholds(daily, thr), sid, rules)

    det = StreamDetector(cfg, thr, rules)
    rng = np.random.default_rng(1)
    d, i, kinds = daily.assign(station_id=sid), 0, []
    while i < len(d):
        k = int(rng.integers(1, 45))
        kinds += [a["kind"] for a in det.update(d.iloc[i:i + k])]
        i += k
    res = compare_with_batch(det.closed(), batch, daily["date"].min(), daily["date"].max())
    assert res["ok"], res
    assert res["matched"] > 100
    assert kinds.count("ENDED") == len(det.closed())
    # a replayed (late) chunk is ignored, not double counted
    assert det.update(d.iloc[:30]) == [] and det.late_days == 30


def test_state_roundtrip(cfg, tmp_path):
    """Checkpointed state restores the open runs (resume after a crash)."""
    thr = pd.DataFrame({"station_id": ["X"], "month": [5], "tmax_mean": [39.5], "tmax_std": [2.0], "tmin_mean": [25.0],
                        "tmin_std": [2.0], "wind_mean": [10.0], "wind_std": [3.0], "tmax_p90": [41.0], "tmax_p99": [44.0],
                        "tmin_p01": [20.0], "n_wet": [5], "precip_wet_p99": [80.0], "precip_wet_q1": [2.0],
                        "precip_wet_q3": [10.0], "wind_p99": [40.0], "wind_q1": [8.0], "wind_q3": [14.0],
                        "imd_region": [1], "imd_hill": [0]})
    days = pd.DataFrame({"station_id": "X", "date": pd.date_range("2024-05-01", periods=6),
                         "tmax": [44.0, 45.0, 45.5, 47.5, 39.0, 39.0], "tmin": 28.0, "tmean": 35.0, "rh_mean": 30.0,
                         "rh_max": 50.0, "rh_min": 20.0, "precip_sum": 0.0, "precip_max_1h": 0.0, "wind_max": 12.0,
                         "gust_max": 20.0, "hi_max": 40.0, "n_hours": 24})
    a = StreamDetector(cfg, thr)
    first = a.update(days.iloc[:3])
    assert ("NEW", "heatwave_spell") in {(x["kind"], x["event_type"]) for x in first}   # confirmed on day 3
    a.save(tmp_path / "s.json", 0)
    b = StreamDetector(cfg, thr)
    b.load_state(json.loads((tmp_path / "s.json").read_text()))
    rest = b.update(days.iloc[3:])
    assert ("ESCALATED", "heatwave") in {(x["kind"], x["event_type"]) for x in rest}     # 47.5 >= severe 47
    ev = b.closed().set_index("event_type")
    assert ev.loc["heatwave_spell", "duration_days"] == 4 and ev.loc["heatwave", "severity"] == "severe_heatwave"


@pytest.mark.skipif(os.environ.get("SKIP_SPARK") == "1" or shutil.which("java") is None, reason="Spark disabled")
@pytest.mark.skipif(not (REAL.exists() and (BASELINE / "thresholds.csv").exists()), reason="ERA5 baseline missing")
def test_spark_streaming_end_to_end(tmp_path):
    pytest.importorskip("pyspark")
    landing, ckpt, out = tmp_path / "landing", tmp_path / "ckpt", tmp_path / "stream"
    r = subprocess.run([PY, "-m", "streaming.replayer", "--data", str(REAL), "--start", "2020-05-10", "--end", "2020-05-31",
                        "--speed", "0", "--landing", str(landing), "--quiet"], cwd=ROOT, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr[-3000:]
    r = subprocess.run([PY, "-m", "streaming.detector", "--landing", str(landing), "--checkpoint", str(ckpt),
                        "--out", str(out), "--baseline", str(BASELINE), "--exit-when-done", "--quiet", "-p", "2",
                        "--trigger", "0.5", "--max-files", "40"],
                       cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert r.returncode == 0, r.stdout[-2000:] + r.stderr[-3000:]
    run = (tmp_path / "stream_latest").resolve()
    prog = json.loads((run / "progress.json").read_text())
    parity = json.loads((run / "parity.json").read_text())
    assert prog["status"] == "finished" and prog["days_analysed"] == 6 * 22
    assert parity["ok"] and parity["matched"] >= 15, parity
    alerts = [json.loads(line) for line in open(run / "alerts.jsonl")]
    # Cyclone Amphan's landfall day at Kolkata is caught as heavy rain + gale
    assert {("CCU", "heavy_rain", "2020-05-20"), ("CCU", "high_wind", "2020-05-20")} <= {
        (a["station_id"], a["event_type"], a["start_date"]) for a in alerts if a["kind"] == "NEW"}
