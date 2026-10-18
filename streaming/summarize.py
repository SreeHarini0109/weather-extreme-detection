"""Collect every streaming run (results/stream/*, results/cluster/stream_*) into results/stream/summary.csv.

    python -m streaming.summarize
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.config import load_config, resolve  # noqa: E402


def collect(results: Path) -> pd.DataFrame:
    rows = []
    for run in sorted(list((results / "stream").glob("*/progress.json")) + list((results / "cluster").glob("stream_*/progress.json"))):
        p = json.loads(run.read_text())
        d = run.parent
        par = json.loads((d / "parity.json").read_text()) if (d / "parity.json").exists() else {}
        rep = p.get("replay") or {}
        mb = pd.read_csv(d / "metrics_batches.csv") if (d / "metrics_batches.csv").exists() else pd.DataFrame()
        wall = None
        if not mb.empty and rep.get("started"):
            wall = float(mb["wall"].max() - rep["started"])
        rows.append({
            "run": str(d.relative_to(results)), "scenario": p.get("scenario"), "master": p.get("master"),
            "stations": len(rep.get("stations", [])), "start": rep.get("start"), "end": rep.get("end"),
            "speed_days_per_s": rep.get("speed_days_per_second"), "status": p.get("status"),
            "rows_in": p.get("rows_in"), "station_days": p.get("days_analysed"), "alerts": p.get("alerts"),
            "closed_events": p.get("closed_events"), "batches": p.get("batches"),
            "replay_to_last_alert_s": None if wall is None else round(wall, 2),
            "rows_per_s": None if not wall else round(p.get("rows_in", 0) / wall, 1),
            "latency_median_s": p.get("latency_median_s"), "latency_p95_s": p.get("latency_p95_s"),
            "parity_matched": par.get("matched"), "parity_batch_events": par.get("batch_events"),
            "parity_ok": par.get("ok")})
    return pd.DataFrame(rows)


def main():
    cfg = load_config()
    results = resolve(cfg["paths"]["results"])
    df = collect(results)
    out = results / "stream" / "summary.csv"
    df.to_csv(out, index=False)
    with pd.option_context("display.width", 200, "display.max_columns", 30):
        print(df[["run", "master", "stations", "rows_in", "alerts", "rows_per_s", "latency_median_s", "latency_p95_s",
                  "parity_matched", "parity_batch_events", "parity_ok"]].to_string(index=False))
    print(f"-> {out}")


if __name__ == "__main__":
    main()
