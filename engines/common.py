"""CLI plumbing shared by all engines."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.config import load_config, resolve  # noqa: E402


def base_parser(description: str) -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=description)
    ap.add_argument("--config", help="path to config.yaml")
    ap.add_argument("--data", help="partitioned parquet directory (default: paths.parquet)")
    ap.add_argument("--out", help="results root (default: paths.results)")
    ap.add_argument("--stations", nargs="*", help="restrict to these station ids")
    ap.add_argument("--tag", default="", help="free-text tag stored in benchmark.csv")
    ap.add_argument("--no-write", action="store_true", help="skip writing result files (benchmark row only)")
    ap.add_argument("--quiet", action="store_true")
    return ap


def setup(args):
    cfg = load_config(args.config)
    data = resolve(args.data or cfg["paths"]["parquet"])
    out = resolve(args.out or cfg["paths"]["results"])
    return cfg, data, out
