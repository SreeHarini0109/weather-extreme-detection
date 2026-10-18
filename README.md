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
| Streaming (extension) | `streaming/` | Spark Structured Streaming: watermarks, stateful dedup + windowed daily aggregation, live alerts |

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

## Streaming extension (live alerts)

The batch engines analyse stored history. `streaming/` adds near-real-time detection: a **replayer** feeds
stored hourly data back as a live feed (no free live feed exists for these stations; ERA5 arrives ~5 days
late), **Spark Structured Streaming** turns it into daily values, and the same detection rules raise alerts
as days complete.

```bash
python -m streaming.demo --list                          # scenarios from config.yaml
python -m streaming.demo --scenario heatwave_2024        # May-June 2024 heatwave, 1 simulated day/s
streamlit run reporting/dashboard.py                     # "Live stream" tab refreshes every 2 s
```

```
replayer.py  -> data/stream/landing/part-*.parquet (6 simulated hours per file, atomic rename)
detector.py  -> readStream -> withWatermark(2 h) + dropDuplicates -> shared cleaning expressions
             -> groupBy(station, window(1 day)) [append mode: a day is emitted once, when complete]
             -> foreachBatch: batch-run thresholds + shared rules -> core/streaming.py StreamDetector
                (open runs per station x rule -> NEW / ESCALATED / ENDED alerts)
             -> results/stream_latest/{alerts.jsonl, daily.csv, events.csv, active.csv, progress.json, parity.json}
```

At the end of a run every closed streamed event is compared with the batch run's events (`parity.json`).
All runs so far are identical (`results/stream/summary.csv`, `python -m streaming.summarize`):

| Run | Rows | Alerts | Detection latency (median / p95) | Parity |
|---|---|---|---|---|
| 2024 heatwave, ERA5 6 cities, local[4], 1 day/s | 8,784 | 232 | 2.3 s / 2.5 s | 101/101 |
| Cyclone Amphan 2020 / Michaung 2023, 2 days/s | 3,168 / 2,304 | 50 / 30 | 2.0 s / 2.2 s | 22/22, 15/15 |
| Synthetic 50 stations, Apr-Jul 2024, max speed | 146,407 | 1,893 | ~14,000 rows/s | 865/865 |
| Docker cluster, 3 workers, worker2 stopped mid-stream | 8,784 | 232 | 3.0 s / 15.6 s (recovery) | 101/101 |
| Detector killed (SIGKILL), restarted with `--resume` | 8,784 | 232 | 2.2 s / 10.2 s | 101/101 |

Latency = wall-clock time from the replayer delivering a day's last hour to that day's alerts (watermark +
trigger interval + processing). Thresholds come from a batch run (`--baseline`), i.e. the station's
"normal" is learnt offline and applied online. On the cluster:
`docker exec -w /app master python3 -m streaming.demo --scenario amphan_2020 --master spark://master:7077`.

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
from lineage and the job still completes. The same works for the streaming demo (state is checkpointed under
`data/stream/checkpoint`, shared by the `/app` mount).

**Lab PCs on a LAN:** see [docker/lan/README.md](docker/lan/README.md): one container per PC with host
networking, `start_node.sh`, `make_hostfile.sh`, `sync_project.sh` and an end-to-end `check_cluster.sh`.

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

Real data (ERA5 via Open-Meteo, 6 Indian cities, 2015–2024, `data/parquet/real`): the unchanged pipeline finds
the May–June 2024 North India heatwave (Phalodi 48.1 °C), Cyclone Amphan (Kolkata, 20 May 2020) and Cyclone
Michaung (Chennai, 4 Dec 2023). It misses the Dec 2015 Chennai floods because ERA5 smooths that rain to 41–60 mm/day
(below the 64.5 mm IMD threshold). Shareable results page: `python reporting/make_page.py --synthetic <run> --era5 <run>`.

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
core/                  cleaning, aggregates, thresholds, detection, streaming, trends, pipeline, outputs, timing
engines/               sequential, parallel_mp, mpi_engine, spark_engine
streaming/             replayer, detector (Spark Structured Streaming), demo, summarize
benchmark/             run_benchmarks.sh, analyze.py, format_benchmark.py
reporting/             make_report.py, evaluate.py (vs ground truth), dashboard.py, style.py
tests/                 aggregates, detection (+ halo, ground truth), trends, engine parity, streaming
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
* **Regional rules.** IMD heatwave / cold-wave criteria apply only to stations in `thresholds.imd.countries`
  (India), with IMD's hill-station criteria above 1000 m and the absolute plains cold-wave rule (Tmin ≤ 4 °C);
  elsewhere heat and cold extremes come from the station-relative detectors. `apply_everywhere: true` restores
  global application.
* **Streaming = batch.** The stream reuses the batch Spark engine's cleaning and daily-aggregation
  expressions and the same rule lambdas; `StreamDetector` keeps one open run per station × rule, which is
  gaps-and-islands done incrementally. Events already running when a replay starts, or still running at
  its end, are excluded from the parity check (their extent is unknown to the stream).
* **Limitations.** Daily aggregation uses UTC days. The departure from normal uses the station-month mean
  (IMD uses daily normals). Synthetic data is a statistical model, not a climate model. ERA5 underestimates local
  extremes (~25 km grid).
