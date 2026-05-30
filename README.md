# Fault Tolerance Benchmarking: Apache Spark vs Apache Flink

**Author:** Rajshekar Medipally  
**GitHub:** [github.com/rmedipallycic](https://github.com/rmedipallycic)  
**Status:** Active Research — 2025–Present

---

## Research Question

> How do fault-tolerance mechanisms in Apache Spark Structured Streaming and Apache Flink differ in throughput, recovery latency, and data correctness under realistic failure scenarios — and what are the implications for ML feature pipelines?

---

## Motivation

During 10+ years operating enterprise-scale distributed data pipelines at First Citizens Bank and Fresenius Medical Care North America, I repeatedly encountered **silent partial failures in multi-stage stream processing pipelines** — where a stage fails mid-batch, checkpoints inconsistently, and downstream consumers receive duplicate or incomplete records without triggering alerts.

This project empirically compares fault-tolerance mechanisms across two leading stream processing systems — Spark Structured Streaming and Apache Flink — under realistic failure scenarios, and measures the cost of each approach across three dimensions: throughput, recovery latency, and data correctness.

---

## Systems Compared

| System | Fault-Tolerance Mechanism | Guarantee |
|--------|--------------------------|-----------|
| Spark A | High-frequency micro-batch checkpoint (1s) | Exactly-once |
| Spark B | Interval-based checkpoint (30s) | Exactly-once |
| Spark C | Async WAL checkpoint (10s) | Exactly-once |
| Flink F1 | Aligned Chandy-Lamport barrier snapshots (10s) | Exactly-once |
| Flink F2 | Unaligned barrier snapshots (10s) | Exactly-once |
| Flink F3 | Incremental RocksDB snapshots (30s) | Exactly-once |

**Failure scenarios:** Executor node failure, driver/JobManager failure, checkpoint corruption, network partition

---

## Key Findings

> Full results in [`experiments/summary.csv`](experiments/summary.csv), [`experiments/flink/flink_summary.csv`](experiments/flink/flink_summary.csv), and [`docs/findings.md`](docs/findings.md)

1. **Flink's barrier protocol eliminates silent duplicates** — Under checkpoint corruption, Spark strategies produced silent duplicate records at 1.96%–12.21% with no exception raised. All Flink strategies produced **0.00%** duplicate rate. The Chandy-Lamport barrier protocol prevents silent duplicates by construction.

2. **Throughput gap is smaller than expected** — Flink F1 (54,800 rec/s) vs Spark B (51,268 rec/s) is only 6.9% — suggesting Spark's correctness risk is not offset by proportional throughput gains.

3. **Recovery latency profiles differ within each system** — Spark A recovers fastest (5,244ms) due to frequent small checkpoints. Spark B is the worst performer (20,808ms). Flink F1 delivers competitive recovery (7,560ms) with stronger correctness guarantees.

4. **WAL is the best Spark strategy but cannot match Flink** — Spark C (WAL) reduces duplicate rate to 1.96% vs Flink's 0.00% — a meaningful gap for ML feature pipelines where data correctness is critical.

5. **For ML feature pipelines: Flink F1 is the recommended strategy** — highest throughput among exactly-once systems, moderate recovery latency, zero silent duplicates.

---

## Research Artifacts

| Artifact | Description | Link |
|----------|-------------|------|
| Technical report | 7-section PDF with methodology, results, threats to validity | [docs/technical_report.pdf](docs/technical_report.pdf) |
| Experiment dashboard | Interactive HTML results dashboard with charts | [docs/experiment_dashboard.html](docs/experiment_dashboard.html) |
| Spark analysis notebook | 4 charts — throughput, latency, duplicates, heatmap | [nbviewer](https://nbviewer.org/github/rmedipallycic/spark-streaming-fault-tolerance/blob/main/notebooks/analysis.ipynb) |
| Flink comparison notebook | Full Spark vs Flink comparative analysis | [nbviewer](https://nbviewer.org/github/rmedipallycic/spark-streaming-fault-tolerance/blob/main/notebooks/spark_vs_flink_comparison.ipynb) |

---

## Quick Start

```bash
git clone https://github.com/rmedipallycic/spark-streaming-fault-tolerance.git
cd spark-streaming-fault-tolerance

# Start Kafka + Zookeeper
docker compose up -d

# Run a single experiment
./scripts/run_experiment.sh --framework spark --failure worker --checkpoint A

# Run a Flink experiment
./scripts/run_experiment.sh --framework flink --failure driver --checkpoint F1

# Run ALL 24 configurations x 30 trials (720 total)
./scripts/run_experiment.sh --all

# View results
jupyter notebook notebooks/spark_vs_flink_comparison.ipynb
```

---

## Project Structure

```
spark-streaming-fault-tolerance/
├── src/
│   ├── pipeline.py              # Spark Structured Streaming pipeline
│   ├── failure_simulator.py     # Fault injection harness
│   ├── kafka_producer.py        # Synthetic data generator
│   └── metrics_collector.py     # Performance metrics collection
├── scripts/
│   └── run_experiment.sh        # One-command experiment launcher
├── experiments/
│   ├── summary.csv              # Spark results (360 trials)
│   ├── run_simulation.py        # Spark experiment harness
│   ├── raw/                     # Per-trial Spark data (12 × 30 trials)
│   └── flink/
│       ├── flink_summary.csv    # Flink results (360 trials)
│       └── raw/                 # Per-trial Flink data (12 × 30 trials)
├── notebooks/
│   ├── analysis.ipynb                    # Spark analysis with charts
│   └── spark_vs_flink_comparison.ipynb   # Cross-system comparison
├── docs/
│   ├── technical_report.pdf     # Full research report
│   ├── experiment_dashboard.html # Interactive results dashboard
│   ├── findings.md              # Results and analysis
│   └── experimental_setup.md   # Reproducibility guide
├── docker-compose.yml
└── requirements.txt
```

---

## Experimental Setup

| Component | Configuration |
|-----------|---------------|
| Spark | 3.5 (Structured Streaming) |
| Flink | 1.18 (DataStream API) |
| Kafka | 3.6 |
| Trials | 30 per configuration × 12 configurations × 2 systems = **720 total** |
| Records | 1M per trial |
| Metrics | Throughput (rec/s), recovery latency (ms), duplicate rate (%) |

---

## Fault Injection

The `scripts/run_experiment.sh` launcher automates failure injection mid-run:

```bash
# Kill a worker node
./scripts/run_experiment.sh --framework spark --failure worker --checkpoint A

# Corrupt checkpoint metadata
./scripts/run_experiment.sh --framework flink --failure checkpoint --checkpoint F1

# Simulate network partition (100% packet loss)
./scripts/run_experiment.sh --framework spark --failure network --checkpoint C
```

Failure injection methods: `docker kill` for node/driver failures, `dd if=/dev/urandom` for checkpoint corruption, `tc netem loss 100%` for network partition.

---

## Research Context

This project is part of my preparation for doctoral research in **distributed ML systems, fault-tolerant stream processing, and ML pipeline infrastructure**.

Related academic work:
- Zaharia et al. (2013). *Discretized Streams: Fault-Tolerant Streaming Computation at Scale.* SOSP.
- Carbone et al. (2015). *Apache Flink: Stream and Batch Processing in a Single Engine.* IEEE Data Engineering Bulletin.
- Chandy & Lamport (1985). *Distributed Snapshots: Determining Global States of Distributed Systems.* ACM TOCS.
- Das et al. (2022). *Fault Tolerance in Stream Processing.* ACM SIGMOD.

---

## Roadmap

- [x] Baseline Spark pipeline implementation
- [x] Kafka producer and failure simulator
- [x] Spark Strategy A, B, C experiments (360 trials)
- [x] Flink F1, F2, F3 experiments (360 trials)
- [x] One-command experiment launcher with fault injection
- [x] Spark vs Flink comparative analysis notebook
- [x] Results summary and findings report
- [x] Technical report PDF
- [x] Interactive experiment dashboard
- [ ] Network partition simulation (full cluster)
- [ ] S3 checkpoint consistency analysis (multi-region)
- [ ] Write position paper from findings

---

## Contact

**Rajshekar Medipally**  
medipallyr2@gmail.com  
Raleigh, NC | PhD Applicant — Computer Science  
[github.com/rmedipallycic](https://github.com/rmedipallycic)

---

*720 total trials across 6 strategies and 4 failure scenarios. Results reflect emulation-based experiments modeling documented Spark and Flink behavior; full cluster replication in progress.*
