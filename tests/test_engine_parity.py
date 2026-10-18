"""All engines must produce identical results (stats within 1e-6, identical event sets).

Runs every engine as a subprocess on a small synthetic dataset. MPI tests are skipped if mpirun is
missing; Spark is skipped if SKIP_SPARK=1 or Java/pyspark is unavailable.
"""
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests.parity import compare_runs

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable


def run(cmd, **kw):
    r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=900, **kw)
    assert r.returncode == 0, r.stdout[-2000:] + r.stderr[-4000:]
    return r


@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("parity")
    raw, pq_dir, res = tmp / "raw", tmp / "parquet", tmp / "results"
    run([PY, "data/generator.py", "--stations", "4", "--years", "9", "--start-year", "2010", "--workers", "4",
         "--out", str(raw)])
    run([PY, "data/to_parquet.py", "--input", str(raw), "--output", str(pq_dir), "--workers", "4"])
    run([PY, "-m", "engines.sequential", "--data", str(pq_dir), "--out", str(res), "--quiet"])
    return pq_dir, res


def latest(res: Path, prefix: str) -> Path:
    return sorted(p for p in res.glob(f"{prefix}*") if p.is_dir() and not p.is_symlink())[-1]


def check(res, prefix):
    errs = compare_runs(latest(res, "sequential_p1"), latest(res, prefix))
    assert not errs, "\n".join(errs)


@pytest.mark.parametrize("schedule", ["static", "dynamic"])
def test_multiprocessing(dataset, schedule):
    pq_dir, res = dataset
    run([PY, "-m", "engines.parallel_mp", "--data", str(pq_dir), "--out", str(res), "-p", "3",
         "--schedule", schedule, "--quiet"])
    check(res, "multiprocessing_p3")


@pytest.mark.skipif(shutil.which("mpirun") is None, reason="mpirun not installed")
@pytest.mark.parametrize("mode,np_", [("station", 3), ("dynamic", 3), ("time", 4)])
def test_mpi(dataset, mode, np_):
    pq_dir, res = dataset
    run(["mpirun", "--oversubscribe", "-np", str(np_), PY, "-m", "engines.mpi_engine", "--data", str(pq_dir),
         "--out", str(res), "--mode", mode, "--quiet"])
    check(res, f"mpi_p{np_}_{mode}")


@pytest.mark.skipif(os.environ.get("SKIP_SPARK") == "1" or shutil.which("java") is None, reason="Spark disabled")
def test_spark(dataset):
    pytest.importorskip("pyspark")
    pq_dir, res = dataset
    run([PY, "-m", "engines.spark_engine", "--data", str(pq_dir), "--out", str(res), "-p", "2", "--quiet"])
    check(res, "spark_p2")
