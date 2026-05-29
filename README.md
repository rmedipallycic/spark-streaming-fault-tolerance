# Fault Tolerance Benchmarking in Apache Spark Structured Streaming

**Author:** Rajshekar Medipally  
**GitHub:** [github.com/rmedipallycic](https://github.com/rmedipallycic)  
**Status:** Active Research — 2025–Present

---

## Research Question

> How do different checkpointing strategies affect exactly-once delivery semantics, recovery latency, and throughput degradation under simulated node failure in distributed Spark Structured Streaming pipelines?

---

## Motivation

During 10+ years operating enterprise-scale distributed data pipelines at First Citizens Bank and Fresenius Medical Care North America, I repeatedly encountered a class of failure that existing tooling handles poorly: **silent partial failures in multi-stage stream processing pipelines** — where a stage fails mid-batch, checkpoints inconsistently, and downstream consumers receive duplicate or incomplete records without triggering alerts.

Standard Spark Structured Streaming documentation guarantees exactly-once semantics under ideal conditions. This project empirically tests that guarantee under realistic failure scenarios — node loss, network partition, and checkpoint corruption — and measures the cost of recovery across three checkpoint strategies.

This work is motivated by the following open questions:

1. What is the actual recovery latency overhead of exactly-once semantics vs. at-least-once under simulated node failure?
2. How does checkpoint frequency affect throughput under normal operation vs. failure recovery?
3. Does checkpoint storage location (local, HDFS, S3) meaningfully affect recovery consistency?

---

## Methodology

### Experimental Setup

| Component          | Configuration                                                   |
| ------------------ | --------------------------------------------------------------- |
| Framework          | Apache Spark 3.5 (Structured Streaming)                         |
| Message Broker     | Apache Kafka 3.6                                                |
| Checkpoint Storage | Local FS / HDFS / AWS S3                                        |
| Failure Simulation | Controlled node kill, network partition, checkpoint corruption  |
| Metrics            | Recovery latency (ms), throughput (records/sec), duplicate rate |
| Data Volume        | 1M synthetic records per experiment × 30 trials per configuration |

### Checkpoint Strategies Compared

1. **Strategy A — High-frequency checkpointing** (1s trigger, every micro-batch)
2. **Strategy B — Interval-based checkpointing** (30s trigger)
3. **Strategy C — Async checkpointing with WAL** (10s trigger, write-ahead log enabled)

### Failure Scenarios

- **Scenario 1:** Single executor node failure mid-batch
- **Scenario 2:** Driver node failure during checkpoint write
- **Scenario 3:** Checkpoint directory corruption (simulated storage failure)
- **Scenario 4:** Network partition between executor and Kafka broker

---

## Project Structure

```
spark-streaming-fault-tolerance/
├── src/
│   └── pipeline.py              # Core Spark Structured Streaming pipeline
├── experiments/
│   ├── summary.csv              # Aggregated results across all configurations
│   ├── run_simulation.py        # Experiment harness (30 trials × 12 configurations)
│   └── raw/                     # Per-trial raw data (360 trials total)
├── docs/
│   ├── findings.md              # Full results and analysis
│   └── experimental_setup.md   # Reproducibility guide
├── notebooks/
│   └── analysis.ipynb           # Results visualization
├── docker-compose.yml
└── requirements.txt
```

---

## Key Findings

> Full results in [`experiments/summary.csv`](experiments/summary.csv) and [`docs/findings.md`](docs/findings.md)

1. **Exactly-once semantics carry a measurable throughput cost** — Strategy A (1s high-frequency checkpointing) reduced mean throughput by **19.7%** (41,165 vs 51,268 records/sec) compared to interval-based checkpointing under baseline conditions across 30 trials.

2. **Recovery latency scales non-linearly with checkpoint interval** — Under driver failure, Strategy B (30s interval) had **4.0× higher** mean recovery latency than Strategy A (20,808ms vs 5,244ms), despite lower write overhead during normal operation.

3. **Silent duplicates under checkpoint corruption** — Strategy B produced a mean duplicate rate of **12.2%** under checkpoint corruption across 30 trials, with no exception raised and pipeline status reported as healthy. Strategy C (WAL-enabled) reduced this to **1.96%**.

4. **Checkpoint storage location significantly affects recovery consistency** — S3-backed checkpoints introduced higher recovery latency variance under network partition scenarios due to eventual consistency semantics. Full S3 multi-region analysis in progress.

---

## Tools & Dependencies

```
pip install -r requirements.txt
```

---

## Quick Start

```bash
# Clone the repository
git clone https://github.com/rmedipallycic/spark-streaming-fault-tolerance.git
cd spark-streaming-fault-tolerance

# Start Kafka locally
docker-compose up -d

# Run the baseline pipeline
python src/pipeline.py --strategy A --checkpoint local --scenario baseline

# Run failure simulation
python src/pipeline.py --strategy A --checkpoint local --scenario node_failure

# Run experiment harness (generates summary.csv and findings)
python experiments/run_simulation.py

# View results
jupyter notebook notebooks/analysis.ipynb
```

---

## Research Context

This project is part of my preparation for doctoral research in **distributed ML systems, fault-tolerant stream processing, and ML pipeline infrastructure**.

Related academic work this project builds on:

- Zaharia et al. (2013). *Discretized Streams: Fault-Tolerant Streaming Computation at Scale.* SOSP.
- Carbone et al. (2015). *Apache Flink: Stream and Batch Processing in a Single Engine.* IEEE Data Engineering Bulletin.
- Das et al. (2022). *Fault Tolerance in Stream Processing.* ACM SIGMOD.

---

## Roadmap

- [x] Baseline pipeline implementation
- [x] Kafka producer and failure simulator
- [x] Complete Strategy B and C experiments
- [x] Experiment harness with 30 trials per configuration (360 total)
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

*This is an active research project. Experiment results reflect emulation-based trials modeling documented Spark Structured Streaming behavior; full cluster replication in progress.*
