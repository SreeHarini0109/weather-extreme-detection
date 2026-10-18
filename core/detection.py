"""P5: extreme-weather detection.

Three complementary methods:
  (a) fixed meteorological thresholds (IMD heatwave/cold wave - Indian stations only, with IMD's
      plains and hill-station criteria - IMD heavy rain, cloudburst, NWS heat index, humidity, Beaufort wind),
  (b) statistical, location-relative (station-month P99/P1, z-score, IQR),
  (c) multi-day spells (heatwave spell >= 3 days, warm spell / WSDI >= 6 days above P90).

Each rule is defined ONCE as a lambda over a column namespace `c`. For pandas, `c` is the daily
DataFrame (c.tmax is a Series); for Spark, `c` is a wrapper whose attributes are Columns. Both
support the same operators, so all engines evaluate literally the same rule definitions.

Flagged days are grouped into events with the "gaps and islands" technique: consecutive flagged
days form one event; events shorter than the rule's min_len are discarded.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import numpy as np
import pandas as pd

EVENT_COLS = ["event_id", "station_id", "event_type", "method", "severity", "start_date", "end_date",
              "duration_days", "peak_value", "peak_date"]


@dataclass
class Rule:
    event_type: str
    method: str            # threshold | percentile | zscore | iqr | spell
    value: str             # daily column holding the event magnitude
    direction: int         # +1: peak = max, -1: peak = min
    cond: Callable
    levels: list = field(default_factory=list)   # [(name, cond or None), ...] increasing severity
    min_len: int = 1


def build_rules(cfg: dict) -> list[Rule]:
    t = cfg["thresholds"]
    hw, cw, hr, hi, hu, bf = (t["heatwave"], t["coldwave"], t["heavy_rain"], t["heat_index"], t["humidity"],
                              t["wind_beaufort"])
    wet, z, k = t["wet_day_mm"], t["zscore"], t["iqr_k"]

    # IMD criteria (India only; plains vs hill stations). imd_region / imd_hill are 0/1 columns
    # joined from the thresholds table; "== 1" keeps the expression valid for pandas AND Spark.
    def heatwave(c):
        plains = (c.imd_hill == 0) & (((c.tmax >= hw["tmax_min"]) & ((c.tmax - c.tmax_mean) >= hw["departure"]))
                                      | (c.tmax >= hw["absolute"]))
        hills = (c.imd_hill == 1) & (c.tmax >= hw["hill_tmax_min"]) & ((c.tmax - c.tmax_mean) >= hw["departure"])
        return (c.imd_region == 1) & (plains | hills)

    def heatwave_severe(c):
        return ((c.tmax - c.tmax_mean) >= hw["severe_departure"]) | ((c.imd_hill == 0) & (c.tmax >= hw["severe_absolute"]))

    def cold_wave(c):
        plains = (c.imd_hill == 0) & (((c.tmin <= cw["tmin_max"]) & ((c.tmin - c.tmin_mean) <= cw["departure"]))
                                      | (c.tmin <= cw["absolute"]))
        hills = (c.imd_hill == 1) & (c.tmin <= cw["hill_tmin_max"]) & ((c.tmin - c.tmin_mean) <= cw["departure"])
        return (c.imd_region == 1) & (plains | hills)

    def cold_wave_severe(c):
        return ((c.tmin - c.tmin_mean) <= cw["severe_departure"]) | ((c.imd_hill == 0) & (c.tmin <= cw["severe_absolute"]))

    hw_levels = [("heatwave", None), ("severe_heatwave", heatwave_severe)]
    bf_names = list(bf.items())

    return [
        # (a) fixed thresholds -------------------------------------------------------------
        Rule("heatwave", "threshold", "tmax", +1, heatwave, hw_levels),
        Rule("cold_wave", "threshold", "tmin", -1, cold_wave,
             [("cold_wave", None), ("severe_cold_wave", cold_wave_severe)]),
        Rule("heavy_rain", "threshold", "precip_sum", +1, lambda c: c.precip_sum >= hr["heavy"],
             [("heavy", None), ("very_heavy", lambda c: c.precip_sum >= hr["very_heavy"]),
              ("extremely_heavy", lambda c: c.precip_sum >= hr["extremely_heavy"])]),
        Rule("cloudburst", "threshold", "precip_max_1h", +1,
             lambda c: c.precip_max_1h >= t["cloudburst_mm_per_hour"], [("cloudburst", None)]),
        Rule("heat_stress", "threshold", "hi_max", +1, lambda c: c.hi_max >= hi["danger"],
             [("danger", None), ("extreme_danger", lambda c: c.hi_max >= hi["extreme_danger"])]),
        Rule("humidity_high", "threshold", "rh_mean", +1, lambda c: c.rh_mean >= hu["high_mean"],
             [("saturated", None)]),
        Rule("humidity_low", "threshold", "rh_min", -1, lambda c: c.rh_min <= hu["low_min"], [("very_dry", None)]),
        Rule("high_wind", "threshold", "wind_max", +1, lambda c: c.wind_max >= bf_names[0][1],
             [(bf_names[0][0], None)] + [(name, (lambda v: (lambda c: c.wind_max >= v))(val))
                                          for name, val in bf_names[1:]]),
        # (b) statistical, station-month relative -----------------------------------------
        Rule("hot_day_p99", "percentile", "tmax", +1, lambda c: c.tmax > c.tmax_p99, [("p99", None)]),
        Rule("cold_night_p01", "percentile", "tmin", -1, lambda c: c.tmin < c.tmin_p01, [("p01", None)]),
        Rule("rain_p99", "percentile", "precip_sum", +1,
             lambda c: (c.precip_sum >= wet) & (c.precip_sum > c.precip_wet_p99), [("p99", None)]),
        Rule("wind_p99", "percentile", "wind_max", +1, lambda c: c.wind_max > c.wind_p99, [("p99", None)]),
        Rule("hot_zscore", "zscore", "tmax", +1,
             lambda c: (c.tmax_std > 0) & (((c.tmax - c.tmax_mean) / c.tmax_std) > z), [("z>3", None)]),
        Rule("cold_zscore", "zscore", "tmin", -1,
             lambda c: (c.tmin_std > 0) & (((c.tmin - c.tmin_mean) / c.tmin_std) < -z), [("z<-3", None)]),
        Rule("wind_zscore", "zscore", "wind_max", +1,
             lambda c: (c.wind_std > 0) & (((c.wind_max - c.wind_mean) / c.wind_std) > z), [("z>3", None)]),
        Rule("rain_iqr", "iqr", "precip_sum", +1,
             lambda c: (c.precip_sum >= wet) & (c.precip_sum > (c.precip_wet_q3 + (c.precip_wet_q3 - c.precip_wet_q1) * k)),
             [("q3+3iqr", None)]),
        Rule("wind_iqr", "iqr", "wind_max", +1,
             lambda c: c.wind_max > (c.wind_q3 + (c.wind_q3 - c.wind_q1) * k), [("q3+3iqr", None)]),
        # (c) spells ------------------------------------------------------------------------
        Rule("heatwave_spell", "spell", "tmax", +1, heatwave, hw_levels, min_len=t["spells"]["heatwave_min_days"]),
        Rule("warm_spell", "spell", "tmax", +1, lambda c: c.tmax > c.tmax_p90, [("wsdi", None)],
             min_len=t["spells"]["warm_spell_min_days"]),
    ]


def _mask(series_like) -> np.ndarray:
    return np.asarray(pd.Series(series_like).fillna(False), dtype=bool)


def severity_level(rule: Rule, c) -> np.ndarray:
    lvl = np.zeros(len(c), dtype=np.int64)
    for i, (_, cond) in enumerate(rule.levels[1:], start=1):
        lvl = np.maximum(lvl, np.where(_mask(cond(c)), i, 0))
    return lvl


def detect_events(daily: pd.DataFrame, station_id: str, rules: list[Rule]) -> pd.DataFrame:
    """Detect all events for one station. `daily` = continuous daily rows with thresholds attached."""
    if daily.empty:
        return pd.DataFrame(columns=EVENT_COLS)
    daily = daily.sort_values("date").reset_index(drop=True)
    dates = daily["date"].to_numpy()
    out = []
    for rule in rules:
        flag = _mask(rule.cond(daily))
        if not flag.any():
            continue
        # gaps and islands: group = day_number - row_number over flagged days (a missing date breaks a run)
        day_num = dates.astype("datetime64[D]").astype(np.int64)
        idx = np.flatnonzero(flag)
        grp = day_num[idx] - np.arange(idx.size)
        sval = daily[rule.value].to_numpy(dtype=np.float64)[idx] * rule.direction
        lvl = severity_level(rule, daily.iloc[idx])
        f = pd.DataFrame({"grp": grp, "day": day_num[idx], "sval": sval, "lvl": lvl})
        g = f.groupby("grp", sort=True)
        ev = g.agg(start=("day", "min"), end=("day", "max"), duration=("day", "size"), lvl=("lvl", "max"))
        ev = ev[ev["duration"] >= rule.min_len]
        if ev.empty:
            continue
        # peak: max of signed value, earliest day on ties
        pk = f.sort_values(["grp", "sval", "day"], ascending=[True, False, True]).drop_duplicates("grp").set_index("grp")
        ev = ev.join(pk[["sval", "day"]].rename(columns={"day": "peak_day"}))
        names = np.array([n for n, _ in rule.levels])
        start = ev["start"].to_numpy().astype("datetime64[D]")
        out.append(pd.DataFrame({
            "event_id": [f"{station_id}_{rule.event_type}_{str(s).replace('-', '')}" for s in start],
            "station_id": station_id, "event_type": rule.event_type, "method": rule.method,
            "severity": names[ev["lvl"].to_numpy()],
            "start_date": start, "end_date": ev["end"].to_numpy().astype("datetime64[D]"),
            "duration_days": ev["duration"].to_numpy().astype(np.int64),
            "peak_value": np.round(ev["sval"].to_numpy() * rule.direction, 4),
            "peak_date": ev["peak_day"].to_numpy().astype("datetime64[D]"),
        }))
    if not out:
        return pd.DataFrame(columns=EVENT_COLS)
    return pd.concat(out, ignore_index=True)[EVENT_COLS]


def filter_owned_events(events: pd.DataFrame, own_start, own_end, is_first: bool, is_last: bool) -> pd.DataFrame:
    """Ownership rule for time-partitioned detection with halo rows.

    A rank computes events over [halo from previous rank] + [own days]. Then:
      * an event that ends on the rank's last day may continue into the next rank -> the next rank
        (which sees it through its halo) owns it, so drop it here unless this is the last rank;
      * an event that ended before (own_start - 1 day) lies wholly in the halo and was already
        reported by the previous rank -> drop it here unless this is the first rank.
    Correct as long as no event is longer than the halo.
    """
    if events.empty:
        return events
    end = pd.to_datetime(events["end_date"])
    keep = np.ones(len(events), dtype=bool)
    if not is_last:
        keep &= (end < pd.Timestamp(own_end)).to_numpy()
    if not is_first:
        keep &= (end >= pd.Timestamp(own_start) - pd.Timedelta(days=1)).to_numpy()
    return events.loc[keep].reset_index(drop=True)
