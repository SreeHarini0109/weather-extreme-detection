#!/usr/bin/env bash
# Full benchmark suite. Every run appends one row to results/benchmark.csv; analyze.py takes medians.
#
#   bash benchmark/run_benchmarks.sh [--repeats 3] [--max-p 8] [--quick] [--no-spark]
#
# Experiments (tag column):
#   strong     fixed 13.1M rows (data/parquet/synthetic), p = 1,2,4,8 for every engine/mode
#   weak       ~1.58M rows per worker (6 stations x p), multiprocessing + MPI station mode
#   size       1.6M .. 35.5M rows (subsets of data/parquet/large) at fixed p
#   balance    heterogeneous station sizes (data/parquet/large): block vs round-robin vs LPT vs dynamic
set -euo pipefail
cd "$(dirname "$0")/.."

REPEATS=3; MAXP=8; QUICK=0; SPARK=1
while [[ $# -gt 0 ]]; do
  case $1 in
    --repeats) REPEATS=$2; shift 2;;
    --max-p) MAXP=$2; shift 2;;
    --quick) QUICK=1; REPEATS=1; shift;;
    --no-spark) SPARK=0; shift;;
    *) echo "unknown option $1"; exit 1;;
  esac
done

PY=${PYTHON:-python3}
MPIRUN="mpirun --oversubscribe"
SYN=data/parquet/synthetic
LARGE=data/parquet/large
PS=(1 2 4 8); [[ $MAXP -ge 10 ]] && PS+=(10)
COMMON="--no-write --quiet"

stations() { $PY -c "import json,sys; s=sorted(json.load(open('$1/manifest.json'))['stations']); print(' '.join(s[:int(sys.argv[1])]))" "$2"; }

run() { echo "+ $*"; "$@"; }

for r in $(seq 1 "$REPEATS"); do
  echo "=== repeat $r / $REPEATS ==="
  # --- strong scaling --------------------------------------------------------
  run $PY -m engines.sequential --data $SYN $COMMON --tag strong
  for p in "${PS[@]}"; do
    run $PY -m engines.parallel_mp --data $SYN -p "$p" $COMMON --tag strong
    run $MPIRUN -np "$p" $PY -m engines.mpi_engine --data $SYN --mode station $COMMON --tag strong
    [[ $p -ge 2 ]] && run $MPIRUN -np "$p" $PY -m engines.mpi_engine --data $SYN --mode time $COMMON --tag strong
    [[ $p -ge 2 ]] && run $MPIRUN -np $((p + 1)) $PY -m engines.mpi_engine --data $SYN --mode dynamic $COMMON --tag strong
    [[ $SPARK -eq 1 ]] && run $PY -m engines.spark_engine --data $SYN -p "$p" $COMMON --tag strong
  done
  [[ $QUICK -eq 1 ]] && continue

  # --- weak scaling (6 stations ~ 1.58M rows per worker) --------------------
  for p in "${PS[@]}"; do
    ST=$(stations $SYN $((6 * p)))
    [[ $p -eq 1 ]] && run $PY -m engines.sequential --data $SYN --stations $ST $COMMON --tag "weak"
    run $PY -m engines.parallel_mp --data $SYN -p "$p" --stations $ST $COMMON --tag "weak"
    run $MPIRUN -np "$p" $PY -m engines.mpi_engine --data $SYN --mode station --stations $ST $COMMON --tag "weak"
  done

  # --- data size scaling at fixed p ----------------------------------------
  for n in 10 50 100 200; do
    ST=$(stations $LARGE $n)
    run $PY -m engines.sequential --data $LARGE --stations $ST $COMMON --tag "size"
    run $PY -m engines.parallel_mp --data $LARGE -p 8 --stations $ST $COMMON --tag "size"
    run $MPIRUN -np 8 $PY -m engines.mpi_engine --data $LARGE --mode station --stations $ST $COMMON --tag "size"
    [[ $SPARK -eq 1 ]] && run $PY -m engines.spark_engine --data $LARGE -p 8 --stations $ST $COMMON --tag "size"
  done

  # --- load balancing on heterogeneous station sizes -----------------------
  for p in 4 8; do
    for b in block round_robin lpt; do
      run $MPIRUN -np "$p" $PY -m engines.mpi_engine --data $LARGE --mode station --balance $b $COMMON --tag "balance"
    done
    run $MPIRUN -np $((p + 1)) $PY -m engines.mpi_engine --data $LARGE --mode dynamic $COMMON --tag "balance"
    run $PY -m engines.parallel_mp --data $LARGE -p "$p" --schedule static --order name $COMMON --tag "balance"
    run $PY -m engines.parallel_mp --data $LARGE -p "$p" --schedule dynamic --order lpt $COMMON --tag "balance"
  done
done

run $PY benchmark/format_benchmark.py
run $PY benchmark/analyze.py
