"""Streaming extension: incremental (day-by-day) extreme-event detection.

The batch engines see a station's whole history and find events with gaps-and-islands. A stream
sees days one at a time, so this module keeps, per (station, rule), the run of consecutive flagged
days that is still open and turns it into alerts as days arrive:

  NEW        the run reaches the rule's min_len  -> the event is confirmed (day 1 for most rules,
             day 3 for heatwave_spell, day 6 for warm_spell)
  ESCALATED  the run's severity level goes up (heatwave -> severe_heatwave, B8 gale -> B10 storm ...)
  ENDED      a day arrives that is not flagged (or a day is missing) -> the event is closed; the
             final record has exactly the batch EVENT_COLS schema

The flags come from the SAME rule lambdas as the batch engines (core.detection.build_rules), on the
SAME daily values (Spark aggregates them with the batch engine's expressions) and the SAME
station-month thresholds (loaded from a batch run), so every closed streamed event must equal the
batch event with the same id. tests/test_streaming.py checks this.

State is a plain dict, so the driver can checkpoint it as JSON after every micro-batch and resume.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from .detection import EVENT_COLS, Rule, _mask, build_rules, severity_level

ALERT_COLS = ["kind", "station_id", "event_type", "method", "severity", "start_date", "date",
              "duration_days", "peak_value", "peak_date", "value", "event_id"]


def _day(x) -> int:
    return int(np.datetime64(pd.Timestamp(x).date(), "D").astype(np.int64))


def _date(day: int) -> str:
    return str(np.datetime64(int(day), "D"))


class StreamDetector:
    """Feed finalized daily rows (any number of stations, in date order per station) with update()."""

    def __init__(self, cfg: dict, thresholds: pd.DataFrame, rules: list[Rule] | None = None):
        self.rules = rules or build_rules(cfg)
        self.thr = thresholds
        self.open: dict[str, dict] = {}       # "station|rule_index" -> open run
        self.last_day: dict[str, int] = {}    # station -> last day processed
        self.events: list[dict] = []          # closed events (EVENT_COLS)
        self.days_seen = 0
        self.late_days = 0

    # ------------------------------------------------------------------ public API

    def update(self, daily: pd.DataFrame) -> list[dict]:
        """Process a batch of daily rows; return the alerts it triggers (oldest first)."""
        if daily is None or daily.empty:
            return []
        d = daily.copy()
        d["date"] = pd.to_datetime(d["date"])
        d["month"] = d["date"].dt.month.astype(np.int64)
        d = d.merge(self.thr, on=["station_id", "month"], how="left", sort=False)
        d = d.sort_values(["station_id", "date"], kind="stable").reset_index(drop=True)
        # drop days at or before what we already processed for the station (late / replayed rows)
        dn = d["date"].to_numpy().astype("datetime64[D]").astype(np.int64)
        last = d["station_id"].map(self.last_day).fillna(-(1 << 62)).to_numpy(dtype=np.int64)
        fresh = dn > last
        self.late_days += int((~fresh).sum())
        d, dn = d.loc[fresh].reset_index(drop=True), dn[fresh]
        if d.empty:
            return []

        flags, levels, svals = [], [], []
        for rule in self.rules:
            f = _mask(rule.cond(d))
            flags.append(f)
            levels.append(severity_level(rule, d) if f.any() else np.zeros(len(d), dtype=np.int64))
            svals.append(d[rule.value].to_numpy(dtype=np.float64) * rule.direction)

        alerts = []
        sids = d["station_id"].to_numpy()
        for i in range(len(d)):
            sid, day = sids[i], int(dn[i])
            for k, rule in enumerate(self.rules):
                alerts += self._step(sid, k, rule, day, bool(flags[k][i]), int(levels[k][i]), float(svals[k][i]))
            self.last_day[sid] = day
            self.days_seen += 1
        return alerts

    def active(self) -> pd.DataFrame:
        """Confirmed events that are still running (they may still grow)."""
        rows = [self._record(key, run) for key, run in self.open.items() if run["announced"]]
        return pd.DataFrame(rows, columns=EVENT_COLS)

    def closed(self) -> pd.DataFrame:
        return pd.DataFrame(self.events, columns=EVENT_COLS)

    # ------------------------------------------------------------------ state machine

    def _step(self, sid, k, rule, day, flagged, lvl, sval) -> list[dict]:
        key = f"{sid}|{k}"
        run = self.open.get(key)
        out = []
        if run is not None and (not flagged or day != run["end"] + 1):
            out += self._close(key, run, day)
            run = None
        if not flagged:
            return out
        if run is None:
            run = {"start": day, "end": day, "n": 1, "peak": sval, "peak_day": day, "lvl": lvl, "announced": False}
            self.open[key] = run
        else:
            run["end"], run["n"] = day, run["n"] + 1
            if sval > run["peak"]:                 # earliest day wins ties, as in batch
                run["peak"], run["peak_day"] = sval, day
            if lvl > run["lvl"]:
                run["lvl"] = lvl
                if run["announced"]:
                    out.append(self._alert("ESCALATED", key, run, day, sval))
        if not run["announced"] and run["n"] >= rule.min_len:
            run["announced"] = True
            out.append(self._alert("NEW", key, run, day, sval))
        return out

    def _close(self, key, run, day) -> list[dict]:
        del self.open[key]
        if not run["announced"]:
            return []
        rec = self._record(key, run)
        self.events.append(rec)
        return [self._alert("ENDED", key, run, day, None)]

    def _record(self, key, run) -> dict:
        sid, k = key.rsplit("|", 1)
        rule = self.rules[int(k)]
        start = _date(run["start"])
        return {"event_id": f"{sid}_{rule.event_type}_{start.replace('-', '')}", "station_id": sid,
                "event_type": rule.event_type, "method": rule.method, "severity": rule.levels[run["lvl"]][0],
                "start_date": start, "end_date": _date(run["end"]), "duration_days": run["n"],
                "peak_value": round(run["peak"] * rule.direction, 4), "peak_date": _date(run["peak_day"])}

    def _alert(self, kind, key, run, day, sval) -> dict:
        rec = self._record(key, run)
        rule = self.rules[int(key.rsplit("|", 1)[1])]
        return {"kind": kind, "station_id": rec["station_id"], "event_type": rec["event_type"],
                "method": rec["method"], "severity": rec["severity"], "start_date": rec["start_date"],
                "date": _date(day), "duration_days": rec["duration_days"], "peak_value": rec["peak_value"],
                "peak_date": rec["peak_date"],
                "value": None if sval is None else round(sval * rule.direction, 4), "event_id": rec["event_id"]}

    # ------------------------------------------------------------------ checkpointing

    def state(self) -> dict:
        return {"open": self.open, "last_day": self.last_day, "events": self.events,
                "days_seen": self.days_seen, "late_days": self.late_days}

    def load_state(self, s: dict) -> None:
        self.open, self.last_day, self.events = s["open"], s["last_day"], s["events"]
        self.days_seen, self.late_days = s.get("days_seen", 0), s.get("late_days", 0)

    def save(self, path: Path, batch_id: int) -> None:
        tmp = Path(path).with_suffix(".tmp")
        tmp.write_text(json.dumps({"batch_id": batch_id, **self.state()}))
        tmp.replace(path)


def alert_message(a: dict, names: dict | None = None) -> str:
    """Human-readable one-liner for the console / dashboard."""
    who = (names or {}).get(a["station_id"], a["station_id"])
    what = a["event_type"].replace("_", " ")
    if a["kind"] == "NEW":
        return (f"NEW {what} at {who} from {a['start_date']} ({a['severity']}, "
                f"{a['value']} on {a['date']})")
    if a["kind"] == "ESCALATED":
        return f"{what} at {who} escalated to {a['severity']} on {a['date']} (day {a['duration_days']}, {a['value']})"
    return (f"{what} at {who} ended: {a['start_date']} to {_end(a)}, "
            f"{a['duration_days']} days, peak {a['peak_value']} on {a['peak_date']} ({a['severity']})")


def _end(a: dict) -> str:
    return _date(_day(a["start_date"]) + int(a["duration_days"]) - 1)


def compare_with_batch(stream_events: pd.DataFrame, batch_events: pd.DataFrame, start, end,
                       stations=None) -> dict:
    """Closed streamed events vs batch events inside the replayed window.

    Comparable events start AFTER the first replayed day (an event already running when the replay
    began is seen by the stream only from that day, so its start, id and length differ) and end
    BEFORE the last replayed day (the stream cannot know whether it continues afterwards).
    """
    ws, we = pd.Timestamp(start), pd.Timestamp(end)

    def window(ev):
        ev = ev.copy()
        for c in ("start_date", "end_date", "peak_date"):
            ev[c] = pd.to_datetime(ev[c])
        return ev[(ev["start_date"] > ws) & (ev["end_date"] < we)]

    b, s = window(batch_events), window(stream_events)
    if stations is not None:
        b = b[b["station_id"].isin(stations)]
    key = ["event_id"]
    cols = ["severity", "end_date", "duration_days", "peak_value", "peak_date"]
    m = b.merge(s, on=key, how="outer", suffixes=("_batch", "_stream"), indicator=True)
    only_b = m[m["_merge"] == "left_only"]["event_id"].tolist()
    only_s = m[m["_merge"] == "right_only"]["event_id"].tolist()
    both = m[m["_merge"] == "both"]
    diff = []
    for c in cols:
        x, y = both[f"{c}_batch"], both[f"{c}_stream"]
        bad = ~((x == y) | (pd.isna(x) & pd.isna(y)))
        if c == "peak_value":
            bad = ~np.isclose(x.astype(float), y.astype(float), atol=1e-6, equal_nan=True)
        diff += [f"{e}: {c} batch={a} stream={b_}" for e, a, b_ in zip(both["event_id"][bad], x[bad], y[bad])]
    return {"batch_events": int(len(b)), "stream_events": int(len(s)), "matched": int(len(both)),
            "only_batch": only_b, "only_stream": only_s, "field_mismatches": diff,
            "ok": not only_b and not only_s and not diff}
