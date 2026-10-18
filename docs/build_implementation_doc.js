// Builds docs/Implementation_Phases.docx for the weather project.
const fs = require("fs");
const path = require("path");
const {
  Document, Packer, Paragraph, TextRun, HeadingLevel, AlignmentType, Table, TableRow, TableCell, WidthType,
  ShadingType, BorderStyle, ImageRun, LevelFormat, TableOfContents, PageBreak, Footer, PageNumber, Header,
} = require("docx");

const ROOT = "/Users/sreehariniganishkaa/weather";
const F = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "facts.json"), "utf8"));
const PLOTS = path.join(ROOT, "results", "plots");
const CONTENT_W = 9026; // A4 width 11906 - 2 x 1440 margins (DXA)
const ACCENT = "1D5FAE", HEAT = "C2410C", GREY = "4A5866", LINE = "C9D2DB", HEAD_FILL = "E8EEF5";

// ---------------------------------------------------------------- helpers
const runs = (text, base = {}) => {
  // **bold** and `code` inline markup
  const out = [];
  const re = /(\*\*[^*]+\*\*|`[^`]+`)/g;
  let last = 0, m;
  while ((m = re.exec(text))) {
    if (m.index > last) out.push(new TextRun({ text: text.slice(last, m.index), ...base }));
    const t = m[0];
    if (t.startsWith("**")) out.push(new TextRun({ text: t.slice(2, -2), bold: true, ...base }));
    else out.push(new TextRun({ text: t.slice(1, -1), font: "Consolas", size: 19, color: "1F2D3A", ...base }));
    last = m.index + t.length;
  }
  if (last < text.length) out.push(new TextRun({ text: text.slice(last), ...base }));
  return out;
};
const P = (text, opts = {}) => new Paragraph({ children: runs(text), spacing: { after: 140, line: 300 }, ...opts });
const H1 = (t) => new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun(t)], pageBreakBefore: true });
const H2 = (t) => new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(t)] });
const H3 = (t) => new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun(t)] });
const B = (text, level = 0) => new Paragraph({ numbering: { reference: "bullets", level }, children: runs(text),
  spacing: { after: 80, line: 290 } });
const N = (text) => new Paragraph({ numbering: { reference: "steps", level: 0 }, children: runs(text),
  spacing: { after: 80, line: 290 } });
const Code = (lines) => lines.map((l, i) => new Paragraph({
  children: [new TextRun({ text: l || " ", font: "Consolas", size: 18, color: "1F2D3A" })],
  shading: { type: ShadingType.CLEAR, fill: "F2F5F8", color: "auto" },
  spacing: { after: i === lines.length - 1 ? 160 : 0, line: 260 },
  indent: { left: 200, right: 200 },
}));
const Note = (text) => new Paragraph({
  children: runs(text), spacing: { before: 60, after: 160, line: 290 }, indent: { left: 280 },
  border: { left: { style: BorderStyle.SINGLE, size: 18, color: HEAT, space: 10 } },
});

const border = { style: BorderStyle.SINGLE, size: 4, color: LINE };
const borders = { top: border, bottom: border, left: border, right: border };
function table(header, rows, widths) {
  const total = widths.reduce((a, b) => a + b, 0);
  const cell = (text, i, head) => new TableCell({
    borders, width: { size: widths[i], type: WidthType.DXA },
    shading: head ? { fill: HEAD_FILL, type: ShadingType.CLEAR, color: "auto" } : undefined,
    margins: { top: 70, bottom: 70, left: 110, right: 110 },
    children: [new Paragraph({ children: runs(String(text), { size: 19, bold: head || undefined }),
      spacing: { after: 0, line: 260 } })],
  });
  return [new Table({
    width: { size: total, type: WidthType.DXA }, columnWidths: widths,
    rows: [new TableRow({ tableHeader: true, children: header.map((h, i) => cell(h, i, true)) }),
      ...rows.map((r) => new TableRow({ children: r.map((c, i) => cell(c, i, false)) }))],
  }), new Paragraph({ children: [], spacing: { after: 120 } })];
}
function pngSize(file) {
  const b = fs.readFileSync(file);
  return { w: b.readUInt32BE(16), h: b.readUInt32BE(20) };
}
function figure(name, caption, widthIn = 6.2) {
  const file = path.join(PLOTS, name);
  if (!fs.existsSync(file)) return [];
  const { w, h } = pngSize(file);
  const W = Math.round(widthIn * 96);
  return [
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 120, after: 60 },
      children: [new ImageRun({ type: "png", data: fs.readFileSync(file), transformation: { width: W, height: Math.round(W * h / w) },
        altText: { title: caption, description: caption, name } })] }),
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 220 },
      children: [new TextRun({ text: caption, italics: true, size: 18, color: GREY })] }),
  ];
}
const fmt = (x, d = 2) => Number(x).toFixed(d);
const pct = (x) => `${(100 * x).toFixed(1)} %`;

// ---------------------------------------------------------------- facts
const rec = F.records, era = F.era_records;
const sp = F.speedup;
const seqT = sp.find((r) => r.label === "sequential").total_s;
const best = sp.filter((r) => r.label !== "sequential").sort((a, b) => b.speedup - a.speedup)[0];
const spRow = (label, p) => sp.find((r) => r.label === label && r.p === p);
const recall = Object.fromEntries(F.recall.map((r) => [r.injected_type, r]));
const q = F.quality;

// ---------------------------------------------------------------- content
const children = [];

// Title page
children.push(
  new Paragraph({ spacing: { before: 2600, after: 200 }, children: [new TextRun({ text: "PARALLEL AND DISTRIBUTED SYSTEMS LABORATORY", size: 20, color: HEAT, bold: true, characterSpacing: 40 })] }),
  new Paragraph({ spacing: { after: 300 }, children: [new TextRun({ text: "Distributed Weather Data Processing and Extreme Weather Detection", size: 52, bold: true, color: "14202B" })] }),
  new Paragraph({ spacing: { after: 600 }, children: [new TextRun({ text: "Implementation report: every phase from data generation to cluster deployment", size: 28, color: GREY })] }),
  ...table(["Item", "Value"], [
    ["Dataset", "13.15 million hourly readings, 50 stations, 1995–2024 (synthetic) + 526,032 ERA5 hours, 6 Indian cities, 2015–2024"],
    ["Engines", "Sequential, multiprocessing, MPI (mpi4py), Apache Spark (PySpark)"],
    ["Best speedup", `${fmt(best.speedup)}× (${best.label}, p = ${best.p}) over ${fmt(seqT)} s sequential`],
    ["Correctness", "All engines produce identical results (24 automated tests incl. cross-engine parity)"],
    ["Repository", "github.com/SreeHarini0109/weather-extreme-detection"],
  ], [2200, 6826]),
  new Paragraph({ children: [new PageBreak()] }),
  new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun("Contents")] }),
  new TableOfContents("Contents", { hyperlink: true, headingStyleRange: "1-2" }),
  new Paragraph({ children: [new TextRun({ text: "Right-click the table and choose Update Field if page numbers are missing.", italics: true, size: 18, color: GREY })] }),
);

// 1. Overview
children.push(
  H1("1. Project overview"),
  P("Weather records are large (millions of hourly readings per decade across many stations), and most analysis of them can run independently per station or per block of years. That makes the problem a natural fit for parallel and distributed processing. The project builds one weather-analysis pipeline and runs it on four execution engines so their processing times can be compared fairly."),
  H2("1.1 What the system produces"),
  B("**Statistics:** minimum, maximum, mean and standard deviation of temperature, humidity, rainfall, wind and gusts, with the time each extreme occurred, per station, per year, per month and globally."),
  B("**Extreme weather records:** heatwaves, cold waves, heavy rain, cloudbursts, heat stress, humidity extremes and gales, detected with fixed meteorological thresholds, station-relative statistics and multi-day spell rules."),
  B("**Trends:** yearly climate indices tested with linear regression, the Mann–Kendall test and Sen's slope."),
  B("**Processing time:** per-phase timings for every run, speedup, efficiency, the Karp–Flatt serial fraction and an Amdahl's-law fit."),
  H2("1.2 Architecture and the central design rule"),
  P("All weather logic lives in one shared package, `core/`. The engines in `engines/` only decide how the work is split across processes or machines and how partial results are combined. Because every engine calls the same rules, the results can be compared one to one, and an automated parity test checks that every engine produces identical output."),
  ...table(["Folder", "Contents"], [
    ["`data/`", "stations.csv, synthetic generator, ERA5 downloader, Parquet converter"],
    ["`core/`", "cleaning, mergeable statistics, thresholds, detection rules, trends, pipeline, outputs, timing"],
    ["`engines/`", "sequential.py, parallel_mp.py, mpi_engine.py, spark_engine.py"],
    ["`tests/`", "unit tests, ground-truth detection test, halo test, cross-engine parity tests"],
    ["`benchmark/`", "run_benchmarks.sh, analyze.py, format_benchmark.py"],
    ["`reporting/`", "evaluation against ground truth, report generator, results page, Streamlit dashboard"],
    ["`docker/`", "image, 4-node Compose cluster, lab-LAN deployment kit"],
  ], [1800, 7226]),
  H2("1.3 Implementation phases at a glance"),
  ...table(["Phase", "Work", "Main output"], [
    ["1", "Environment and configuration", "config.yaml, toolchain check"],
    ["2", "Data acquisition", "synthetic generator with ground truth; ERA5 downloader"],
    ["3", "Data preparation", "station-partitioned Parquet, manifest, quality report"],
    ["4", "Core analytics (P1–P6)", "cleaning, statistics, thresholds, detection, trends"],
    ["5", "Sequential engine", "baseline T₁ and reference results"],
    ["6", "Multiprocessing engine", "shared-memory parallel version"],
    ["7", "MPI engine", "station, time (halo) and dynamic modes"],
    ["8", "Spark engine", "DataFrame implementation and its optimisation"],
    ["9", "Testing and validation", "unit, parity, ground truth, real-event checks"],
    ["10", "Benchmarking", "strong, weak, size and load-balancing experiments"],
    ["11", "Cluster deployment", "Docker cluster, fault tolerance, lab-LAN kit"],
    ["12", "Real-data validation (ERA5)", "known Indian extremes checked"],
    ["13", "Regional rule refinement", "IMD criteria for Indian plains and hills only"],
    ["14", "Reporting", "report, results page, dashboard"],
  ], [900, 4000, 4126]),
);

// Phase 1
children.push(
  H1("2. Phase 1: Environment and configuration"),
  P("The first step was confirming the toolchain on the development machine (Apple M4 with 4 performance and 6 efficiency cores, 16 GB RAM) before writing code, because MPI and Spark are the parts most likely to fail on a laptop."),
  ...table(["Component", "Version / check"], [
    ["Python", "3.13 (Anaconda) locally; 3.11 inside Docker"],
    ["Data", "pandas 2.3, NumPy 2.4, PyArrow 23"],
    ["MPI", "Open MPI 5.0.9 + mpi4py 4.1; smoke test of a custom Allreduce operator on 4 ranks"],
    ["Spark", "PySpark 4.2 + Java 17 locally (3.5.3 in Docker); smoke test of applyInPandas"],
    ["Other", "SciPy, Matplotlib, Plotly, Streamlit, pytest, Docker 28"],
  ], [2200, 6826]),
  H2("2.1 One configuration file"),
  P("`config.yaml` holds everything that affects results, so all engines read identical settings: data paths, generator settings, cleaning ranges, every meteorological threshold (IMD heatwave and heavy-rain categories, cloudburst, heat index, humidity, Beaufort wind), the z-score and IQR factors, spell lengths, histogram bin layouts, trend-test settings, the MPI halo size and Spark settings. Each run stores a copy of the configuration next to its results (`run_config.yaml`) for reproducibility."),
);

// Phase 2
children.push(
  H1("3. Phase 2: Data acquisition"),
  P("The plan uses two data sources: a synthetic generator, which can produce any volume and has known ground truth, and real ERA5 reanalysis data from the Open-Meteo archive API."),
  H2("3.1 Station list"),
  P("`data/stations.csv` lists 50 real cities: 25 in India (including Delhi, Mumbai, Chennai, Kolkata, Phalodi, Cherrapunji, Shimla, Srinagar) and 25 worldwide (London, New York, Tokyo, Sydney, Dubai, Moscow, Singapore and others). Each row has latitude, longitude, elevation, country and the climate parameters the generator needs: annual mean temperature, seasonal and daily temperature ranges, the day of peak heat, annual rainfall, monsoon timing and strength, base humidity, typical wind speed and whether the city is coastal."),
  H2("3.2 Synthetic generator (`data/generator.py`)"),
  P("Each station is generated hour by hour from a statistical model:"),
  ...table(["Variable", "Model"], [
    ["Temperature", "annual mean + seasonal cosine + daily cosine (local solar time) + long-term trend + AR(1) noise with 3-day memory"],
    ["Rainfall", "two stages: a wet/dry day drawn with a monsoon-shaped seasonal weight, then a Gamma-distributed amount spread over the hours of the day"],
    ["Humidity", "falls as temperature rises (daily cycle and anomalies), rises in the wet season and during rain, clipped to 5–100 %"],
    ["Wind", "Weibull (shape 2) speeds with 12-hour persistence via a Gaussian copula, daily cycle; gusts = wind × 1.3–1.8"],
  ], [2000, 7026]),
  P("**Injected extremes with ground truth.** On top of the model, the generator inserts heatwaves (+6 to +10 °C for 3–7 days), cold waves (−6 to −10 °C for 3–6 days), cloudbursts (100–180 mm in one hour) and cyclones at coastal stations (75–150 km/h winds with 80–250 mm/day of rain). Their rate grows by 3 % per year, so the data contains a real \"extremes are increasing\" signal. Every injected event and every station's true warming trend is written to `ground_truth_events.csv` and `ground_truth_trends.csv`, which later lets us measure detection accuracy. Above 48 °C a soft saturation keeps injected heatwaves on already-hot stations physically plausible."),
  P("**Deliberate data-quality problems.** About 0.1 % missing values per variable, sentinel spikes (999.9 °C, 135 % humidity, −1 mm rain, 999 km/h wind) and duplicate rows are injected, so the cleaning phase has real work to do."),
  P("**Parallel and reproducible.** Each station is generated by its own process with a seed derived from the station ID, so the generator itself runs in parallel and always produces the same data. The default run creates 50 stations × 30 years = 13,150,250 rows (2,628 injected events) in about 3 seconds. A `--heterogeneous` option gives stations different record lengths (10–30 years), which created a 200-station, 35.5-million-row dataset with unequal station sizes for the load-balancing experiment."),
  H2("3.3 ERA5 downloader (`data/downloader.py`)"),
  P("The downloader requests hourly temperature, humidity, precipitation, wind speed and gusts from the Open-Meteo archive API in 10-year blocks per station. A thread pool overlaps network waits while a shared rate limiter keeps requests under the free-tier limit; files already downloaded are skipped, so an interrupted download can resume. Because the free quota makes 50 stations × 30 years a multi-day download, the project used 6 Indian cities × 2015–2024 (526,032 hours)."),
);

// Phase 3
const fcsv = F.format.find((r) => r.format === "csv" && r.columns === "all");
const fpq = F.format.find((r) => r.format === "parquet" && r.columns.startsWith("6"));
children.push(
  H1("4. Phase 3: Data preparation"),
  P("`data/to_parquet.py` converts raw files (synthetic Parquet/CSV or ERA5 JSON) into one unified schema and writes Apache Parquet partitioned by station, one folder per station (`station_id=DEL/part-0.parquet`)."),
  B("**Unified schema:** station name, latitude, longitude, elevation, UTC timestamp, temperature (°C), humidity (%), precipitation (mm/h), wind and gust (km/h), stored as 32-bit floats."),
  B("**Row groups of about one year (8,784 rows)** so a reader that needs only certain years can skip the rest using Parquet's min/max statistics. The MPI time-partitioned mode relies on this."),
  B("**manifest.json** records each station's row count, file size, first and last timestamp, coordinates, elevation and country. Engines use it for load balancing without opening the data."),
  B("**data_quality_report.csv** counts missing, out-of-range and duplicate values per station. Values are only counted here; the cleaning itself happens inside each engine so it is parallelised and timed like every other phase."),
  P(`**Why Parquet:** reading the same 2.6 million rows took ${fmt(fcsv.seconds)} s from CSV but ${fmt(fpq.seconds, 3)} s from Parquet when only the 6 needed columns were read, about ${Math.round(fcsv.seconds / fpq.seconds)}× faster, and the Parquet files were ${fmt(fcsv.mib / fpq.mib, 1)}× smaller.`),
);

// Phase 4
children.push(
  H1("5. Phase 4: Core analytics pipeline (P1–P6)"),
  P("Every engine runs the same six phases. They are implemented once in `core/` and timed separately so each run reports where its time went."),
  ...table(["Phase", "Work", "Parallel pattern"], [
    ["P1 Load", "read the station's Parquet partition (only needed columns)", "map: each worker reads its own data"],
    ["P2 Clean", "deduplicate, round, range checks, hourly → daily values", "map"],
    ["P3 Statistics", "count / mean / M2 / min / max with times, all levels", "map + reduce (mergeable aggregates)"],
    ["P4 Thresholds", "station-month climatology and percentiles", "local histograms → sum (Allreduce)"],
    ["P5 Detection", "19 rules turned into multi-day events", "map (+ halo exchange in time mode)"],
    ["P6 Trends", "yearly indices, regression, Mann–Kendall, Sen's slope", "map → gather"],
  ], [1700, 4200, 3126]),
  H2("5.1 P2: Cleaning and daily aggregation"),
  ...table(["Field", "Valid range", "Action"], [
    ["temperature", "−90 to 60 °C", "out of range → missing"],
    ["humidity", "0–100 %", "within 2 % of a limit → clipped to the limit, otherwise missing"],
    ["precipitation", "0–500 mm/h", "out of range (including negatives) → missing"],
    ["wind, gust", "0–400 km/h", "out of range → missing"],
    ["duplicate timestamps", "", "keep the first"],
  ], [2000, 2400, 4626]),
  P(`Invalid values become missing rather than being deleted, so no hours are lost. On the synthetic dataset this removed ${q.duplicates.toLocaleString()} duplicate rows and blanked ${(q.temp_c_flagged + q.humidity_pct_flagged + q.precip_mm_flagged + q.wind_kmh_flagged + q.gust_kmh_flagged).toLocaleString()} impossible values. Hourly data is then aggregated to daily values (UTC days): Tmax, Tmin, Tmean, mean / max / min humidity, rainfall total, wettest hour, maximum wind and gust, and the maximum NWS heat index (Rothfusz regression with its standard adjustments). The daily table is extended to a continuous date range so a missing day always breaks a multi-day event.`),
  H2("5.2 P3: Mergeable statistics"),
  P("Each worker computes partial records of (count, mean, M2, min, max, time of min, time of max). Records from different workers combine **exactly** with Chan's parallel-variance formula, so no raw data needs to move:"),
  ...Code(["n    = nA + nB", "d    = meanB − meanA", "mean = meanA + d · nB / n", "M2   = M2A + M2B + d² · nA · nB / n          (std = √(M2 / n))"]),
  P("Ties for the minimum or maximum are resolved to the earliest time in every engine, so all engines report the same date for a record. Statistics are produced for each station overall, per year and per month, plus a global level for all stations together."),
  H2("5.3 P4: Thresholds from mergeable histograms"),
  P("Station-relative detection needs percentiles (P1, P90, P99, quartiles) per station and month. Exact percentiles normally require all values in one place. Instead, every worker fills fixed-bin histograms (0.1 °C, 0.1 km/h and 0.5 mm bins). Histograms from different workers simply add up, and the percentile is read from the cumulative counts. The bin edges are offset by half a bin so values recorded to 0.1 precision always fall in the middle of a bin, which makes the result exact for such data and identical however the data was split."),
  H2("5.4 P5: Extreme-event detection"),
  P("Each rule is written once as an expression over columns. The same expression works on pandas columns and on Spark columns, so every engine evaluates literally the same rules."),
  ...table(["Method", "Rules"], [
    ["Fixed thresholds", "IMD heatwave and cold wave (Indian stations; separate criteria for hills above 1000 m), IMD heavy rain (64.5 / 115.6 / 204.5 mm categories), cloudburst ≥ 100 mm in one hour, NWS heat index ≥ 41 °C, humidity ≥ 95 % mean or ≤ 10 % minimum, Beaufort gale and above (≥ 62 km/h, B8–B12)"],
    ["Percentile", "hottest day above the station-month P99, coldest night below P1, wet day above P99, wind above P99"],
    ["Z-score", "|value − station-month mean| / std > 3 for Tmax, Tmin and wind"],
    ["IQR", "above Q3 + 3 × IQR for wet-day rain and wind"],
    ["Spells", "heatwave spell = 3+ consecutive heatwave days; warm spell (WSDI) = 6+ days above P90"],
  ], [1800, 7226]),
  P("Flagged days become events with the \"gaps and islands\" technique: within the flagged days of one station and rule, `day number − row number` is constant across a run of consecutive days, so grouping by that value yields one event per run, with its start, end, duration, worst severity and peak value."),
  H2("5.5 P6: Trends"),
  P("For each station and year the pipeline computes climate indices in the style of the international ETCCDI set: mean Tmax / Tmin / Tmean, hottest day (TXx), coldest night (TNn), total rain, wettest day (Rx1day), wettest 5 days (Rx5day), longest dry and wet runs (CDD, CWD), and counts of heatwave, heavy-rain, P99-hot and gale days. Each index is tested with ordinary least squares (slope per decade), the non-parametric Mann–Kendall test with tie correction (significance at p < 0.05) and Sen's slope. Mann–Kendall is implemented in the project itself rather than taken from an extra library."),
);

// Phase 5 + 6
children.push(
  H1("6. Phase 5: Sequential engine"),
  P("`engines/sequential.py` is a plain loop over stations that runs P1–P6 for each one and then merges the small per-station results. It serves two purposes: it is the correctness reference that every other engine is compared against, and its run time is T₁, the baseline for all speedup figures."),
  P(`On the 13.15-million-row dataset the median sequential time was ${fmt(seqT)} s. Profiling showed the statistics phase dominated, so it was rewritten with NumPy group-by primitives (np.bincount and a single grouped min/max), which cut it from 9.2 s to about 6.4 s before any parallelism.`),
  P("Every run writes the same output set: summary_stats.csv, global_records.json, extreme_events.csv, extreme_summary.csv, climate_indices.csv, trends.csv, thresholds.csv, data_quality.csv, precip_histogram.csv, timings.json and run_config.yaml, and appends one row to results/benchmark.csv."),
  H1("7. Phase 6: Multiprocessing engine"),
  P("`engines/parallel_mp.py` uses a pool of worker processes on one machine. Each task processes one whole station and returns only its small results; the main process merges them. Two scheduling strategies are compared:"),
  B("**Static:** `pool.map` with chunks of size ⌈stations / p⌉, so each worker receives one block up front."),
  B("**Dynamic:** `imap_unordered` with chunk size 1, so idle workers pull the next station. With longest-processing-time ordering the biggest stations are queued first."),
  P(`Per-phase times are summed per worker process and the slowest worker defines each phase. Best result: ${fmt(spRow("multiprocessing", 8).total_s)} s at p = 8 (${fmt(spRow("multiprocessing", 8).speedup)}× speedup).`),
);

// Phase 7 MPI
children.push(
  H1("8. Phase 7: MPI engine"),
  P("`engines/mpi_engine.py` uses mpi4py and is the centre of the laboratory work. It offers three partitioning modes."),
  H2("8.1 Station mode (default)"),
  N("Rank 0 reads the manifest and assigns stations with **longest-processing-time (LPT) bin packing**: stations sorted by row count, each given to the least-loaded rank (block and round-robin assignments are available for comparison)."),
  N("`scatter` sends each rank its station list; `bcast` sends the configuration."),
  N("Every rank runs P1–P6 for its own stations, with no communication."),
  N("Global statistics are combined with `Reduce` using a **custom MPI operator** that applies Chan's merge. The partial records are sent as a derived contiguous datatype of 7 doubles, so MPI never splits a record when it segments the buffer."),
  N("`gather` collects events, trends, indices and per-rank timings at rank 0, which writes the outputs."),
  H2("8.2 Time mode (domain decomposition with halo exchange)"),
  P("Each rank owns a block of years for all stations and reads only those years thanks to the Parquet row groups. This needs considerably more communication:"),
  B("Monthly and overall statistics are partial on every rank, so they are gathered and merged with Chan's formula at the root."),
  B("Station-month moments are combined with `Allreduce` using the custom operator, and the percentile histograms with `Allreduce(SUM)` on NumPy buffers, so every rank ends up with identical thresholds."),
  B("**Halo exchange:** a heatwave can cross the boundary between two ranks. Each rank receives the previous rank's last 31 days with `sendrecv` and runs detection on halo + own days. An ownership rule then reports each boundary-crossing event exactly once: a rank drops events that end on its last day (the next rank sees them through its halo) and events that ended inside the halo (the previous rank already reported them)."),
  H2("8.3 Dynamic master–worker mode"),
  P("Rank 0 only hands out work: workers send a request, receive the next station (largest first) with tagged `send`/`recv`, and send results back. This balances itself on machines of different speeds, at the cost of one rank that does not compute."),
  P(`Best results: station mode ${fmt(spRow("mpi/station", 4).speedup)}× at p = 4 and ${fmt(spRow("mpi/station", 10).speedup)}× at p = 10; time mode peaks at ${fmt(spRow("mpi/time", 4).speedup)}× at p = 4.`),
);

// Phase 8 Spark
children.push(
  H1("9. Phase 8: Spark engine"),
  P("`engines/spark_engine.py` implements the same pipeline with PySpark DataFrames:"),
  B("**P1:** read the station partitions and hash-partition by station once. Every later grouping includes the station, so Spark reuses this partitioning instead of shuffling again."),
  B("**P2:** the cleaning rules and heat index as column expressions, then a group-by to daily values."),
  B("**P3:** one aggregation at (station, year, month) level, merged up to the other levels with the same Chan merge used everywhere else."),
  B("**P4:** histogram counts with `groupBy(station, month, bin).count()`, fed to the shared percentile code; thresholds return to the executors through a **broadcast join**. `percentile_approx` (Greenwald–Khanna) can be compared with `--compare-approx`."),
  B("**P5:** the shared rule expressions as Spark columns, then gaps-and-islands with the `row_number` window function."),
  B("**P6:** `applyInPandas` runs the shared indices and trend code per station."),
  H2("9.1 Making Spark agree exactly with pandas"),
  B("Spark treats NaN as larger than every number (NaN > 0 is true), while pandas treats NaN as missing. All NaNs are converted to nulls before rules run."),
  B("Rounding uses the same arithmetic as NumPy (`rint(x · 10ᵈ) / 10ᵈ`) instead of Spark's decimal rounding."),
  B("The heat-index formula is written once and evaluated in the same operation order on both sides, because floating-point arithmetic is not associative."),
  H2("9.2 Performance optimisation"),
  P("The first version needed 79 s at 8 cores. Profiling found four causes, each fixed while keeping identical results:"),
  ...table(["Problem", "Fix", "Effect"], [
    ["min/max over a struct (value, time) forced Spark's sort-based aggregation", "pack value and time into one 64-bit integer so the fast hash aggregation applies", "10.1 s → 2.5 s per aggregation"],
    ["8 separate passes over the hourly data for the statistic levels", "one (station, year, month) pass, merged in pandas", "stats phase 28 s → 5–7 s"],
    ["11 quality counters inside the daily aggregation made its generated code too large", "a separate small per-station aggregation", "daily aggregation 19 s → 2 s"],
    ["caching the wide hourly table cost about 60 s; slow decimal rounding", "cache only the narrow deduplicated table; NumPy-style rounding; JVM flag allowing large generated methods to be compiled", "total 79 s → about 25 s"],
  ], [3300, 3800, 1926]),
  P(`Spark still remains slower than plain Python on one machine at this data size (median ${fmt(spRow("spark", 4).total_s)} s at p = 4). JVM start-up, row-by-row processing and shuffles cost more than vectorised NumPy for 13 million rows; Spark's advantages are fault tolerance and scaling across many machines.`),
);

// Phase 9 testing
children.push(
  H1("10. Phase 9: Testing and validation"),
  P("The test suite (`pytest tests/`, 24 tests) covers four levels:"),
  ...table(["Level", "What is checked"], [
    ["Unit: statistics", "merging partial statistics of random splits equals the statistics of the whole; commutativity; earliest-time tie breaking; multi-way merge of frames"],
    ["Unit: detection", "IMD boundaries (64.4 mm is not heavy rain, 64.5 mm is), Beaufort categories, cloudburst, heatwave severity and spells, zero-variance z-scores, regional IMD rules (plains, hills, outside India), histogram percentiles vs exact nearest-rank values"],
    ["Halo test", "series split into chunks with halos and the ownership rule produce exactly the same events as the unsplit series, including a 10-day heatwave crossing a boundary"],
    ["Ground truth", "injected heatwaves, cloudbursts and cyclones are found (recall ≥ 90 % on a test dataset)"],
    ["Engine parity", "multiprocessing (static and dynamic), MPI (station, dynamic, time) and Spark each match the sequential engine: statistics within 10⁻⁶, identical event sets, thresholds, indices, trends and records"],
    ["Unit: trends", "Sen's slope and OLS exact on linear data; Mann–Kendall detects trends, has about 5 % false positives on noise, and agrees with SciPy's Kendall tau"],
  ], [2000, 7026]),
  H2("10.1 Detection quality against injected ground truth (full dataset)"),
  ...table(["Injected event", "Injected", "Found by ≥ 1 matching detector", "Recall"],
    F.recall.map((r) => [r.injected_type.replace("_", " "), r.injected, r.detected_any_method, pct(r.recall_any)]),
    [2600, 1600, 3000, 1826]),
  P(`Trend recovery: the estimated Sen slopes of mean temperature match the generator's true warming rates with a mean absolute error of ${F.trend_mae} °C per decade (correlation ${F.trend_corr}). ${F.warming} of ${F.n_tm} stations show significant warming of mean Tmax (median ${F.median_sen >= 0 ? "+" : ""}${F.median_sen} °C per decade).`),
  Note("Precision against the injected set is a lower bound: the synthetic climate also produces natural extremes (for example heavy monsoon rain), which are correct detections but not in the ground-truth list."),
);

// Phase 10 benchmarking
const rowsFor = ["sequential", "multiprocessing", "mpi/station", "mpi/time", "mpi/dynamic", "spark"];
const bench = sp.filter((r) => rowsFor.includes(r.label) && [1, 2, 4, 8, 10, 3, 5, 9, 11].includes(r.p))
  .sort((a, b) => rowsFor.indexOf(a.label) - rowsFor.indexOf(b.label) || a.p - b.p);
children.push(
  H1("11. Phase 10: Benchmarking"),
  P("`benchmark/run_benchmarks.sh` runs every configuration three times and `benchmark/analyze.py` takes the median, computes speedup S = T₁/Tₚ, efficiency E = S/p, the Karp–Flatt serial fraction and an Amdahl's-law fit, and draws the plots. Benchmarks were run with nothing else on the machine; an earlier run disturbed by other work was discarded and archived."),
  ...table(["Engine / mode", "p", "Median s", "Speedup", "Efficiency"],
    bench.map((r) => [r.label, r.p, fmt(r.total_s), fmt(r.speedup), fmt(r.efficiency)]), [2600, 900, 1800, 1800, 1926]),
  Note("For MPI dynamic mode p counts all ranks, including rank 0, which only dispatches work. These benchmarks were run before the regional rule refinement of Phase 13, which changes only detection logic."),
  ...figure("speedup.png", "Figure 1. Strong scaling on 13.15 M rows (median of 3 runs)."),
  ...figure("phase_breakdown.png", "Figure 2. Where the time goes: per-phase times of the slowest worker."),
  H2("11.1 Observations"),
  B("**Station partitioning scales best.** Up to 4 processes MPI station mode is close to linear (3.35× at p = 4, efficiency 0.84)."),
  B("**Hybrid cores flatten the curve.** The M4 has only 4 performance cores; extra processes run on efficiency cores at roughly half speed, so efficiency drops above p = 4."),
  B("**Time partitioning pays for communication.** Halo exchange and histogram Allreduce add traffic, and every rank still checks all 50 stations, so time mode stops improving after p = 4."),
  B("**Load balancing:** on stations of unequal size (35.5 M rows), LPT assignment beat block and round-robin assignment, and dynamic scheduling was competitive."),
  B("**Weak scaling** (constant work per process) kept above 0.83 efficiency up to 4 processes."),
  B("**Data-size scaling** (1.9–35.5 M rows) is linear for all engines; the parallel engines stay about 3.3× faster than sequential at every size."),
);

// Phase 11 cluster
children.push(
  H1("12. Phase 11: Cluster deployment"),
  H2("12.1 Docker Compose cluster"),
  P("One Docker image (Python 3.11, Open MPI, mpi4py, PySpark 3.5, Java 17) is used for every node. `docker/docker-compose.yml` starts a master and three workers on a private network with the project folder shared by all containers. MPI starts remote ranks over passwordless SSH using a hostfile; Spark runs in standalone mode (`spark://master:7077`)."),
  ...table(["Run", "Ranks / cores", "Nodes", "Time (s)", "Result"], [
    ...F.cluster.map((r, i) => [`${r.engine}${r.partition_mode ? " / " + r.partition_mode : ""} (${r.tag})`, r.p, r.nodes, fmt(r.total_s),
      i === 0 ? "completed (parity not separately checked)"
        : r.tag === "fault-tolerance" ? "worker stopped mid-run; parity OK"
        : r.tag === "lan-sim" ? "LAN settings (SSH 2222); parity OK" : "parity OK"]),
  ], [3300, 1400, 1000, 1300, 2026]),
  P("Absolute times are slower than on the bare machine because all four containers share one 8-CPU virtual machine and read data through a virtualised shared folder; the purpose of these runs is to show the same code running distributed with identical results."),
  H2("12.2 Fault tolerance"),
  P("During a Spark run, worker 2 was stopped 24 s into the job. The master logged the lost worker and executor, Spark recomputed the lost partitions from their lineage, and the job completed with results identical to the sequential engine. MPI has no such built-in recovery; a failed rank aborts the job, which is a standard comparison point between the two frameworks."),
  H2("12.3 Lab-LAN deployment kit (`docker/lan/`)"),
  P("For running on separate lab PCs, each PC runs one container with host networking so it keeps its own LAN address. The containers' SSH server listens on port 2222 so the PC's own SSH keeps port 22, and MPI is told to use that port. Scripts start a node, copy code and data to the workers, build the MPI hostfile from the PCs' addresses and run an end-to-end check (MPI reaches every PC, Spark sees every worker, results match). These settings were verified on the local 4-container cluster; running on physical lab PCs is the remaining step."),
);

// Phase 12 ERA5
children.push(
  H1("13. Phase 12: Validation on real ERA5 data"),
  P(`The unchanged pipeline was run on ERA5 data for Delhi, Mumbai, Chennai, Kolkata, Phalodi and Lucknow (2015–2024). It found the hottest reading of ${era.highest_temperature.value} °C at Phalodi on ${era.highest_temperature.time.slice(0, 10)}, the wettest day of ${era.max_daily_rainfall.value} mm at Chennai on ${era.max_daily_rainfall.time.slice(0, 10)} and the strongest gust of ${era.max_wind_gust.value} km/h at Kolkata on ${era.max_wind_gust.time.slice(0, 10)}. MPI time mode on the same data matched the sequential engine.`),
  ...table(["Real event", "Result", "What the pipeline found"], [
    ["North India heatwave, May–June 2024", "detected", "heatwave spells of 4–13 days at Delhi, Phalodi and Lucknow; peak 48.1 °C at Phalodi"],
    ["Cyclone Amphan, Kolkata, 20 May 2020", "detected", "gale (68.9 km/h daily max wind) and heavy rain (104 mm)"],
    ["Cyclone Michaung, Chennai, 4 Dec 2023", "detected", "extremely heavy rain, 254.9 mm"],
    ["Chennai floods, 1–2 Dec 2015", "not detected", "ERA5 shows only 41 mm and 60 mm, below the 64.5 mm heavy-rain line"],
  ], [3000, 1500, 4526]),
  ...figure("era5_heatwave_2024.png", "Figure 3. ERA5 daily Tmax in spring 2024 with the detected heatwave spells shaded."),
  Note("The 2015 miss is a property of the data, not the detector: ERA5 averages rain over a grid cell of about 25 km, which smooths out short, intense downpours. The same smoothing explains why ERA5 peaks at 48.1 °C at Phalodi while the station measured about 50 °C."),
);

// Phase 13 regional + 14 reporting
children.push(
  H1("14. Phase 13: Regional rule refinement"),
  P("The first version applied the Indian (IMD) heatwave and cold-wave rules to every station, which produced unrealistic \"cold waves\" in Moscow or Chicago. The rules were refined to follow IMD's own definitions:"),
  B("IMD heatwave and cold-wave rules apply only to stations in India (configurable list of countries)."),
  B("**Plains:** heatwave when Tmax ≥ 40 °C with a departure ≥ 4.5 °C, or Tmax ≥ 45 °C; cold wave when Tmin ≤ 10 °C with a departure ≤ −4.5 °C, or Tmin ≤ 4 °C (severe at ≤ 2 °C)."),
  B("**Hill stations (elevation ≥ 1000 m):** heatwave when Tmax ≥ 30 °C with a departure ≥ 4.5 °C; cold wave when Tmin ≤ 0 °C with a departure ≤ −4.5 °C."),
  B("Stations outside India rely on the station-relative detectors (P99, z-score, warm spells)."),
  P("The station's country and elevation travel through the thresholds table, so all four engines apply the regional rules without engine-specific code. Effect: IMD cold-wave events fell from 5,792 to 1,462, and their precision against injected cold waves doubled (8.9 % → 17.2 %); recall on injected events moved from 90 % to 86 % for heatwaves and from 96 % to 88 % for cold waves, because outside India only the station-relative detectors look for them."),
  H1("15. Phase 14: Reporting"),
  B("**`reporting/make_report.py`** produces a self-contained HTML and Markdown report with records, event tables, detection quality, trends and all performance plots."),
  B("**`reporting/make_page.py`** builds the shareable results page, including the ERA5 reality check."),
  B("**`reporting/dashboard.py`** (Streamlit) offers a station explorer with detected events shaded on the temperature, rain and wind series, a world map of station trends, records and the benchmark charts."),
  B("**`reporting/evaluate.py`** computes recall and precision against the injected ground truth."),
  ...figure("extremes_per_year.png", "Figure 4. Fixed-threshold extreme events per year across all stations."),
);

// Conclusion + how to run
children.push(
  H1("16. Results summary and limitations"),
  H2("16.1 Summary"),
  B(`Global records on the synthetic data: hottest ${rec.highest_temperature.value} °C (${rec.highest_temperature.station_id}), coldest ${rec.lowest_temperature.value} °C (${rec.lowest_temperature.station_id}), wettest day ${rec.max_daily_rainfall.value} mm (${rec.max_daily_rainfall.station_id}), strongest gust ${rec.max_wind_gust.value} km/h (${rec.max_wind_gust.station_id}).`),
  B(`${F.events_total.toLocaleString()} extreme events detected: ${Object.entries(F.events_by_method).map(([k, v]) => `${v.toLocaleString()} ${k}`).join(", ")}.`),
  B(`Best processing time ${fmt(best.total_s)} s (${best.label}, p = ${best.p}) versus ${fmt(seqT)} s sequential: ${fmt(best.speedup)}× faster, with identical results from every engine.`),
  B("Distributed runs on a 4-node Docker cluster and recovery from a lost Spark worker were demonstrated."),
  H2("16.2 Limitations"),
  B("Synthetic data comes from a statistical weather model, not a physical climate model."),
  B("Days are UTC days, and \"departure from normal\" uses the station-month mean instead of IMD's daily normals."),
  B("ERA5 underestimates local extremes, especially short, intense rain."),
  B("Cluster timings come from containers sharing one virtual machine; physical lab PCs remain to be tested with the LAN kit."),
  H1("17. How to run"),
  ...Code([
    "pip install -r requirements.txt            # plus Open MPI and Java 17",
    "python data/generator.py --workers 8        # 13.1 M synthetic rows",
    "python data/to_parquet.py",
    "python -m engines.sequential",
    "python -m engines.parallel_mp --workers 8",
    "mpirun -np 8 python -m engines.mpi_engine --mode station     # or time | dynamic",
    "python -m engines.spark_engine --cores 8",
    "pytest tests/ -v",
    "bash benchmark/run_benchmarks.sh --repeats 3 && python benchmark/analyze.py",
    "python reporting/make_report.py --run results/sequential_latest",
    "streamlit run reporting/dashboard.py",
    "docker compose -f docker/docker-compose.yml up -d --build   # 4-node cluster",
  ]),
);

// ---------------------------------------------------------------- document
const doc = new Document({
  creator: "Weather Extremes project", title: "Implementation Phases",
  description: "Distributed weather data processing and extreme weather detection: implementation report",
  styles: {
    default: { document: { run: { font: "Calibri", size: 22, color: "1A2530" } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 34, bold: true, font: "Calibri", color: "14202B" },
        paragraph: { spacing: { before: 120, after: 220 }, outlineLevel: 0,
          border: { bottom: { style: BorderStyle.SINGLE, size: 8, color: ACCENT, space: 6 } } } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 27, bold: true, font: "Calibri", color: ACCENT },
        paragraph: { spacing: { before: 260, after: 120 }, outlineLevel: 1 } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 23, bold: true, font: "Calibri", color: "14202B" },
        paragraph: { spacing: { before: 200, after: 100 }, outlineLevel: 2 } },
    ],
  },
  numbering: { config: [
    { reference: "bullets", levels: [
      { level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 280 } } } },
      { level: 1, format: LevelFormat.BULLET, text: "–", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 1000, hanging: 280 } } } }] },
    { reference: "steps", levels: [
      { level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 320 } } } }] },
  ] },
  features: { updateFields: true },
  sections: [{
    properties: { page: { size: { width: 11906, height: 16838 }, margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 } } },
    headers: { default: new Header({ children: [new Paragraph({ alignment: AlignmentType.RIGHT,
      children: [new TextRun({ text: "Distributed Weather Data Processing and Extreme Weather Detection", size: 16, color: GREY })] })] }) },
    footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER,
      children: [new TextRun({ children: ["Page ", PageNumber.CURRENT, " of ", PageNumber.TOTAL_PAGES], size: 16, color: GREY })] })] }) },
    children,
  }],
});

const out = path.join(ROOT, "docs", "Implementation_Phases.docx");
fs.mkdirSync(path.dirname(out), { recursive: true });
Packer.toBuffer(doc).then((buf) => { fs.writeFileSync(out, buf); console.log("wrote", out, (buf.length / 1e6).toFixed(2), "MB"); });
