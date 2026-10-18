"""One-command streaming demo: start the Spark streaming detector, then replay a scenario into it.

    python -m streaming.demo --scenario heatwave_2024            # ERA5, May-June 2024, 1 sim-day/s
    python -m streaming.demo --scenario amphan_2020 --speed 2
    python -m streaming.demo --scenario synthetic_2024 --master spark://master:7077   (Docker cluster)
    python -m streaming.demo --list

Watch it live in the dashboard's "Live stream" tab:
    streamlit run reporting/dashboard.py
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.config import load_config, resolve  # noqa: E402


def main(argv=None):
    cfg = load_config()
    sc = cfg["streaming"]
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scenario", default="heatwave_2024", help="name from config.yaml streaming.scenarios")
    ap.add_argument("--list", action="store_true", help="list the scenarios and exit")
    ap.add_argument("--speed", type=float, default=sc["speed_days_per_second"], help="simulated days per second")
    ap.add_argument("--stations", nargs="*")
    ap.add_argument("--start"), ap.add_argument("--end")
    ap.add_argument("--master", default=None)
    ap.add_argument("--cores", "-p", type=int, default=4)
    ap.add_argument("--quiet", action="store_true", help="no per-alert console lines")
    a = ap.parse_args(argv)
    scen = sc["scenarios"]
    if a.list:
        for k, v in scen.items():
            print(f"{k:16s} {v['start']} .. {v['end']}  {v['title']}")
        return
    s = scen[a.scenario]
    start, end = a.start or s["start"], a.end or s["end"]
    landing = resolve(sc["landing"])
    if landing.exists():
        shutil.rmtree(landing)
    landing.mkdir(parents=True)

    py = sys.executable
    det_cmd = [py, "-m", "streaming.detector", "--baseline", s["baseline"], "--data", s["data"],
               "--scenario", a.scenario, "--exit-when-done", "-p", str(a.cores)]
    if a.master:
        det_cmd += ["--master", a.master]
    if a.quiet:
        det_cmd += ["--quiet"]
    rep_cmd = [py, "-m", "streaming.replayer", "--data", s["data"], "--start", start, "--end", end,
               "--speed", str(a.speed), "--keep"]
    if a.stations:
        rep_cmd += ["--stations", *a.stations]

    print(f"== {s['title']}: {start} .. {end} at {a.speed:g} simulated days/s ==", flush=True)
    det = subprocess.Popen(det_cmd, cwd=ROOT)
    latest = resolve(sc["output"]).parent / "stream_latest"
    t0 = time.time()
    # wait until the streaming query is up (it writes progress.json on its first poll)
    while time.time() - t0 < 180 and det.poll() is None:
        p = latest / "progress.json"
        if p.exists() and p.stat().st_mtime > t0:
            break
        time.sleep(0.5)
    if det.poll() is not None:
        sys.exit("detector exited before the stream started")
    rep = subprocess.Popen(rep_cmd, cwd=ROOT)
    try:
        rep.wait()
        det.wait()
    except KeyboardInterrupt:
        rep.terminate()
        det.send_signal(2)
        det.wait()
    prog = json.loads((latest / "progress.json").read_text())
    print(f"results in {latest.resolve()} (status {prog.get('status')}, parity {prog.get('parity_ok')})")


if __name__ == "__main__":
    main()
