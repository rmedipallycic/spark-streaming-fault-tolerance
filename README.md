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

## Analysis Notebooks

| Notebook | Description | View |
|----------|-------------|------|
| `notebooks/analysis.ipynb` | Spark-only analysis with 4 charts | [nbviewer](https://nbviewer.org/github/rmedipallycic/spark-streaming-fault-tolerance/blob/main/notebooks/analysis.ipynb) |
| `notebooks/spark_vs_flink_comparison.ipynb` | Full Spark vs Flink comparative analysis | [nbviewer](https://nbviewer.org/github/rmedipallycic/spark-streaming-fault-tolerance/blob/main/notebooks/spark_vs_flink_comparison.ipynb) |

---

## Project Structure

```
spark-streaming-fault-tolerance/
├── src/
│   ├── pipeline.py              # Spark Structured Streaming pipeline
│   ├── failure_simulator.py     # Fault injection harness
│   ├── kafka_producer.py        # Synthetic data generator
│   └── metrics_collector.py     # Performance metrics collection
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
│   ├── findings.md              # Full results and analysis
│   └── experimental_setup.md   # Reproducibility guide
├── docker-compose.yml
└── requirements.txt
```

---

## Quick Start

```bash
git clone https://github.com/rmedipallycic/spark-streaming-fault-tolerance.git
cd spark-streaming-fault-tolerance

# Start Kafka locally
docker-compose up -d

# Run Spark pipeline
python src/pipeline.py --strategy A --checkpoint local --scenario baseline

# Run experiment harness
python experiments/run_simulation.py

# View Spark analysis
jupyter notebook notebooks/analysis.ipynb

# View Spark vs Flink comparison
jupyter notebook notebooks/spark_vs_flink_comparison.ipynb
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

## Research Context

This project is part of my preparation for doctoral research in **distributed ML systems, fault-tolerant stream processing, and ML pipeline infrastructure**.

Related academic work:
- Zaharia et al. (2013). *Discretized Streams: Fault-Tolerant Streaming Computation at Scale.* SOSP.
- Carbone et al. (2015). *Apache Flink: Stream and Batch Processing in a Single Engine.* IEEE Data Engineering Bulletin.
- Chandy & Lamport (1985). *Distributed Snapshots: Determining Global States of Distributed Systems.* ACM TOCS.

---

## Roadmap

- [x] Baseline Spark pipeline implementation
- [x] Kafka producer and failure simulator
- [x] Spark Strategy A, B, C experiments (360 trials)
- [x] Flink F1, F2, F3 experiments (360 trials)
- [x] Spark vs Flink comparative analysis notebook
- [x] Results summary and findings report
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
