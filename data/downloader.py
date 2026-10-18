"""Stage 1 (real data): parallel, rate-limited downloader for the Open-Meteo Historical Weather API (ERA5).

Free, no API key. One request per station per `chunk_years` block; files are cached in data/raw/openmeteo
so re-running only fetches what is missing. A thread pool overlaps network latency while a shared
token bucket keeps the request rate under the configured limit.

    python data/downloader.py --stations data/stations.csv --start 1995-01-01 --end 2024-12-31
    python data/to_parquet.py --input data/raw/openmeteo --output data/parquet/real
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.config import load_config, resolve  # noqa: E402

API = "https://archive-api.open-meteo.com/v1/archive"
HOURLY = "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_gusts_10m"


class RateLimiter:
    def __init__(self, per_second: float):
        self.interval = 1.0 / per_second
        self.lock = threading.Lock()
        self.next_t = time.monotonic()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            t = max(self.next_t, now)
            self.next_t = t + self.interval
        time.sleep(max(0.0, t - now))


def chunks(start: date, end: date, years: int):
    y = start.year
    while y <= end.year:
        a = max(start, date(y, 1, 1))
        b = min(end, date(y + years - 1, 12, 31))
        yield a, b
        y += years


def fetch(row, a: date, b: date, out_dir: Path, limiter: RateLimiter, retries: int = 5) -> str:
    out = out_dir / f"{row['station_id']}_{a.year}-{b.year}.json"
    if out.exists() and out.stat().st_size > 1000:
        return f"cached {out.name}"
    params = {"latitude": row["lat"], "longitude": row["lon"], "start_date": a.isoformat(),
              "end_date": b.isoformat(), "hourly": HOURLY, "timezone": "GMT",
              "wind_speed_unit": "kmh", "precipitation_unit": "mm", "temperature_unit": "celsius"}
    for attempt in range(retries):
        limiter.wait()
        try:
            r = requests.get(API, params=params, timeout=120)
            if r.status_code == 429:
                time.sleep(30 * (attempt + 1))
                continue
            r.raise_for_status()
            js = r.json()
            tmp = out.with_suffix(".tmp")
            with open(tmp, "w") as f:
                json.dump(js, f)
            tmp.rename(out)
            return f"ok {out.name} ({len(js['hourly']['time']):,} h)"
        except (requests.RequestException, ValueError, KeyError) as e:
            err = e
            time.sleep(2 ** attempt)
    return f"FAILED {out.name}: {err}"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config")
    ap.add_argument("--stations")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--out")
    ap.add_argument("--threads", type=int)
    ap.add_argument("--limit", type=int, help="only the first N stations (testing)")
    ap.add_argument("--ids", nargs="*", help="only these station ids, e.g. --ids DEL BOM MAA")
    a = ap.parse_args(argv)
    cfg = load_config(a.config)
    d = cfg["downloader"]
    st = pd.read_csv(resolve(a.stations or cfg["paths"]["stations"]))
    if a.ids:
        st = st[st["station_id"].isin(a.ids)]
    if a.limit:
        st = st.head(a.limit)
    start = date.fromisoformat(a.start or d["start"])
    end = date.fromisoformat(a.end or d["end"])
    out_dir = resolve(a.out or cfg["paths"]["raw_openmeteo"])
    out_dir.mkdir(parents=True, exist_ok=True)
    limiter = RateLimiter(d["requests_per_second"])
    jobs = [(row, ca, cb) for row in st.to_dict("records") for ca, cb in chunks(start, end, d["chunk_years"])]
    print(f"Downloading {len(jobs)} chunks for {len(st)} stations -> {out_dir}")
    with ThreadPoolExecutor(max_workers=a.threads or d["threads"]) as ex:
        futs = [ex.submit(fetch, row, ca, cb, out_dir, limiter) for row, ca, cb in jobs]
        for i, fut in enumerate(as_completed(futs), 1):
            print(f"[{i}/{len(jobs)}] {fut.result()}")


if __name__ == "__main__":
    main()
