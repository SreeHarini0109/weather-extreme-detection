"""Phase timer shared by all engines (P1 load ... P6 trends, plus comm / merge / write)."""
from __future__ import annotations

import time
from contextlib import contextmanager

PHASES = ["load", "clean", "stats", "thresholds", "detect", "trends"]


class PhaseTimer:
    def __init__(self, clock=time.perf_counter):
        self.clock = clock
        self.times: dict[str, float] = {}

    @contextmanager
    def phase(self, name: str):
        t0 = self.clock()
        try:
            yield
        finally:
            self.times[name] = self.times.get(name, 0.0) + (self.clock() - t0)

    def add(self, other: dict[str, float]):
        for k, v in other.items():
            self.times[k] = self.times.get(k, 0.0) + v

    def to_dict(self) -> dict[str, float]:
        return {k: round(v, 6) for k, v in self.times.items()}


def max_per_phase(timings: list[dict[str, float]]) -> dict[str, float]:
    """Per-phase maximum across workers: the slowest worker determines the phase time."""
    out: dict[str, float] = {}
    for t in timings:
        for k, v in t.items():
            out[k] = max(out.get(k, 0.0), v)
    return out
