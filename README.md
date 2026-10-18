# Distributed Weather Data Processing & Extreme Weather Detection

Parallel and Distributed Systems Laboratory project. Ingests hourly weather observations (temperature,
rainfall, humidity, wind), computes min / max / average statistics, detects extreme weather with fixed
meteorological thresholds, statistical (percentile / z-score / IQR) methods and multi-day spells, finds
long-term trends (OLS, Mann–Kendall, Sen's slope), and measures processing time for four engines:

| Engine | Module | Parallelism |
|---|---|---|
| Sequential baseline (T₁) | `engines/sequential.py` | none |
| Shared memory | `engines/parallel_mp.py` | `multiprocessing.Pool`, static vs dynamic scheduling |
| Message passing | `engines/mpi_engine.py` | `mpi4py`, station / time (halo exchange) / dynamic master–worker modes |
| Big data | `engines/spark_engine.py` | PySpark DataFrames, histogram percentiles, broadcast join, window functions, pandas UDFs |

Design docs: [implementation_plan.md](implementation_plan.md), [project_workflow.md](project_workflow.md).
All weather logic lives in `core/`; engines only decide how work is split and combined, and the parity
tests check that **every engine produces identical results**.

## Quick start (local machine)

```bash
pip install -r requirements.txt            # + OpenMPI (brew install open-mpi) and Java 17 for Spark

# 1. data: 50 stations x 30 years hourly = 13.1M rows with injected extremes + ground truth
python data/generator.py --workers 8
python data/to_parquet.py                  # -> data/parquet/synthetic (partitioned by station_id)

# 2. run any engine (results -> results/<engine>_p<p>_<mode>_<timestamp>/, plus results/<engine>_latest)
python -m engines.sequential
python -m engines.parallel_mp --workers 8
mpirun -np 8 python -m engines.mpi_engine --mode station      # or --mode time | dynamic
python -m engines.spark_engine --cores 8 --compare-approx

# 3. tests, benchmarks, report, dashboard
pytest tests/ -v
bash benchmark/run_benchmarks.sh --repeats 3                 # appends to results/benchmark.csv
python benchmark/analyze.py                                  # speedup tables + plots
python reporting/make_report.py --run results/mpi_latest      # results/report.html (self-contained)
streamlit run reporting/dashboard.py
```

Other data:
* Real ERA5 data (Open-Meteo, no key): `python data/downloader.py` then
  `python data/to_parquet.py --input data/raw/openmeteo --output data/parquet/real` and pass `--data data/parquet/real`.
* Bigger synthetic data: `python data/generator.py --rows-target 100M`; unequal station lengths (for
  load-balancing experiments): `--heterogeneous`.

## Multi-node cluster (Docker Compose: master + 3 workers)

```bash
docker compose -f docker/docker-compose.yml up -d --build
docker exec -it master mpirun --hostfile /app/docker/hostfile -np 4 hostname          # 4 different hosts
docker exec -it master mpirun --hostfile /app/docker/hostfile -np 8 \
    python -m engines.mpi_engine --mode station
docker exec -it master python -m engines.spark_engine --master spark://master:7077   # Spark UI: localhost:8080
docker compose -f docker/docker-compose.yml down
```
Fault-tolerance demo: start a Spark run, then `docker stop worker2`; Spark recomputes the lost partitions
from lineage and the job still completes. Lab LAN: run the same image on each PC, put their IPs in
`docker/hostfile`, and start workers against `spark://<master-ip>:7077`.

## Results summary (13.1M rows, Apple M4: 4 performance + 6 efficiency cores)

| Engine | best wall time | speedup vs sequential (11.0 s) |
|---|---|---|
| MPI station mode (LPT) | 2.9 s at p=10 | 3.8× (3.3× at p=4, near-linear on the P-cores) |
| MPI dynamic master–worker | 3.0 s at p=11 | 3.7× |
| multiprocessing | 3.3 s at p=8 | 3.3× |
| MPI time mode (halo exchange) | 4.8 s at p=4 | 2.3×, then slows: per-rank detection overhead + communication grow |
| Spark local[p] | 24 s at p=4 | 0.45× — JVM/shuffle overhead dominates on one machine at this size |

Medians of 3 runs (`results/speedup_table.csv`, plots in `results/plots/`, full report `results/report.html`).
Detection vs injected ground truth: recall 90 % heatwaves, 96 % cold waves, 100 % cloudbursts and cyclones;
true warming trends recovered with MAE 0.05 °C/decade. Verified on the Docker cluster: MPI across 4 containers
and Spark on 3 workers give identical results; stopping a Spark worker mid-job still completes correctly.

## Pipeline (identical in every engine)

| Phase | Work | Parallel pattern |
|---|---|---|
| P1 load | read own Parquet partitions (column pruning; year filter uses row-group stats) | map |
| P2 clean | dedupe, range checks (NaN / clip), hourly → daily (Tmax, Tmin, RH, rain, wind, heat index) | map |
| P3 stats | count / mean / M2 / min / max (+ when) per station × {overall, year, month} and global | map + reduce (Chan's merge, custom `MPI.Op`) |
| P4 thresholds | station-month climatology; P1/P90/P99/IQR from fixed-bin histograms | local histograms → Allreduce(SUM) |
| P5 detect | 8 threshold rules, 4 percentile, 3 z-score, 2 IQR, 2 spell rules → gaps-and-islands events | map (+ halo exchange in time mode) |
| P6 trends | yearly ETCCDI-style indices (TXx, Rx1day, Rx5day, CDD, CWD, heatwave days, …) → OLS, Mann–Kendall, Sen | map → gather |

## Outputs (`results/<run>/`)

`summary_stats.csv` (min / max / mean / std + when, per station × overall / year / month, and global) ·
`global_records.json` · `extreme_events.csv` · `extreme_summary.csv` · `climate_indices.csv` · `trends.csv` ·
`thresholds.csv` · `data_quality.csv` · `precip_histogram.csv` · `timings.json` · `run_config.yaml`.
`results/benchmark.csv` gets one row per run (engine, p, nodes, mode, rows, phase times, total).

## Project layout

```
config.yaml            thresholds, histogram bins, paths, generator settings
data/                  stations.csv, generator.py, downloader.py, to_parquet.py
core/                  cleaning, aggregates, thresholds, detection, trends, pipeline, outputs, timing
engines/               sequential, parallel_mp, mpi_engine, spark_engine
benchmark/             run_benchmarks.sh, analyze.py, format_benchmark.py
reporting/             make_report.py, evaluate.py (vs ground truth), dashboard.py, style.py
tests/                 aggregates, detection (+ halo, ground truth), trends, engine parity
docker/                Dockerfile, docker-compose.yml, hostfile, entrypoint.sh
```

## Notes / design decisions

* **Exactly mergeable statistics.** Partial (count, mean, M2, min, max, t_min, t_max) records are merged
  with Chan's formula; MPI uses a custom `MPI.Op` on a 7-double contiguous derived datatype so records
  are never split when MPI segments the buffer.
* **Distributed percentiles.** Fixed-bin histograms (bin edges offset by half a bin so 0.1-precision
  values fall mid-bin) are summed across workers; percentiles are then identical regardless of how the
  data was partitioned. Spark's `percentile_approx` is compared via `--compare-approx`.
* **Halo exchange.** In MPI time mode each rank receives the previous rank's last `halo_days` (31) daily
  rows via `sendrecv`; an ownership rule ensures events crossing a chunk boundary are reported exactly
  once (tested in `tests/test_detection.py`).
* **Cross-engine determinism.** Heat index is written once (`core.cleaning.hi_parts`) and evaluated on
  numpy arrays and Spark Columns with the same operation order; NaN is converted to null in Spark (Spark
  orders NaN above all numbers); ties on min/max/peaks resolve to the earliest time everywhere.
* **Limitations.** Daily aggregation uses UTC days. The IMD heatwave / cold-wave rules are applied to every
  station (they are designed for India). The departure from normal uses the station-month mean (IMD uses
  daily normals). Synthetic data is a statistical model, not a climate model. ERA5 underestimates local
  extremes (~25 km grid).
