"""Detection quality against the generator's injected ground truth.

Recall    : fraction of injected events overlapped (in time, same station) by >= 1 detected event of a
            matching type.
Precision : fraction of detected events of a type that overlap an injected event of the matching
            kind. Synthetic data also contains *natural* extremes from the noise model (e.g. heavy
            monsoon rain), so precision measures "how much of what we flag was injected", a lower bound.

    python reporting/evaluate.py --run results/sequential_latest --data data/parquet/synthetic
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.config import resolve  # noqa: E402

MATCH = {
    "heatwave": ["heatwave", "heatwave_spell", "hot_day_p99", "hot_zscore", "warm_spell"],
    "cold_wave": ["cold_wave", "cold_night_p01", "cold_zscore"],
    "cloudburst": ["cloudburst", "heavy_rain", "rain_p99", "rain_iqr"],
    "cyclone": ["high_wind", "wind_p99", "wind_zscore", "wind_iqr"],
}


def _overlap_any(a: pd.DataFrame, b: pd.DataFrame) -> np.ndarray:
    """For each row of a, does any row of b (same station) overlap [s, e]?"""
    hit = np.zeros(len(a), dtype=bool)
    if a.empty or b.empty:
        return hit
    bg = {k: g for k, g in b.groupby("station_id")}
    for i, (sid, s, e) in enumerate(zip(a["station_id"], a["s"], a["e"])):
        g = bg.get(sid)
        if g is not None and ((g["s"].to_numpy() <= e) & (g["e"].to_numpy() >= s)).any():
            hit[i] = True
    return hit


def evaluate(events: pd.DataFrame, truth: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    ev = events.assign(s=pd.to_datetime(events["start_date"]), e=pd.to_datetime(events["end_date"]))
    gt = truth.assign(s=pd.to_datetime(truth["start_date"]), e=pd.to_datetime(truth["end_date"]))
    recall_rows, prec_rows = [], []
    for kind, types in MATCH.items():
        g = gt[gt["event_type"] == kind]
        if g.empty:
            continue
        any_hit = _overlap_any(g, ev[ev["event_type"].isin(types)])
        row = {"injected_type": kind, "injected": len(g), "detected_any_method": int(any_hit.sum()),
               "recall_any": any_hit.mean()}
        for t in types:
            row[f"recall_{t}"] = _overlap_any(g, ev[ev["event_type"] == t]).mean()
        recall_rows.append(row)
        for t in types:
            d = ev[ev["event_type"] == t]
            if d.empty:
                continue
            p = _overlap_any(d, g)
            prec_rows.append({"detector": t, "matches_injected": kind, "detected": len(d),
                              "overlap_injected": int(p.sum()), "precision": p.mean(),
                              "recall": _overlap_any(g, d).mean()})
    return pd.DataFrame(recall_rows), pd.DataFrame(prec_rows)


def trend_accuracy(trends: pd.DataFrame, truth_trends: pd.DataFrame) -> pd.DataFrame:
    t = trends[trends["variable"] == "tmean_mean"][["station_id", "sen_slope_per_decade", "mk_p", "significant"]]
    return truth_trends.merge(t, on="station_id")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", default="results/sequential_latest")
    ap.add_argument("--data", default="data/parquet/synthetic")
    a = ap.parse_args()
    run, data = resolve(a.run), resolve(a.data)
    rec, prec = evaluate(pd.read_csv(run / "extreme_events.csv"), pd.read_csv(data / "ground_truth_events.csv"))
    print(rec.round(3).to_string(index=False))
    print(prec.round(3).to_string(index=False))
    ta = trend_accuracy(pd.read_csv(run / "trends.csv"), pd.read_csv(data / "ground_truth_trends.csv"))
    err = ta["sen_slope_per_decade"] - ta["true_trend_c_per_decade"]
    print(f"Trend recovery (tmean, Sen): MAE {err.abs().mean():.3f} °C/decade, "
          f"corr {np.corrcoef(ta['true_trend_c_per_decade'], ta['sen_slope_per_decade'])[0, 1]:.3f}")


if __name__ == "__main__":
    main()
