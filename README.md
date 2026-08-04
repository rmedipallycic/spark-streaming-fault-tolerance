# Fault Tolerance Benchmarking: Apache Spark vs Apache Flink

**Author:** Rajshekar Medipally  
**GitHub:** [github.com/rmedipallycic](https://github.com/rmedipallycic)  
**Status:** Active Research — 2025–Present

---

## Research Question

How do fault-tolerance mechanisms in Apache Spark Structured Streaming and Apache Flink differ in throughput, recovery latency, and data correctness under realistic failure scenarios — and what are the implications for ML feature pipelines?

---

## Novel Contribution: Adaptive Checkpoint Selection

Existing checkpointing strategies use fixed intervals — Spark A (1s), Spark B (30s), Spark C (10s WAL). This is suboptimal: a 30s interval has low overhead during stable operation but catastrophic replay cost after failure; a 1s interval minimizes replay but introduces constant overhead.

This project proposes and evaluates **Adaptive Checkpoint Selection** — a feedback control algorithm that dynamically adjusts the checkpoint interval based on three pipeline health signals:

- **Throughput variance** — coefficient of variation of recent throughput samples
- **Error pressure** — exceptions per 1,000 records in the current window
- **Failure recency** — exponential decay signal since the last detected failure

The algorithm computes a risk score [0, 1] every 5 seconds and adjusts accordingly:

- Risk > 0.45 → tighten interval by 30% (more frequent checkpoints)
- 4 consecutive stable windows → relax interval by 20%
- Bounded: 2s ≤ interval ≤ 45s

**Results across 480 trials (30 per configuration):**

| Strategy | Throughput | Recovery (driver) | Dup rate (corruption) |
|---|---|---|---|
| Strategy A (1s fixed) | 46,170 rec/s | 5,061ms | 0.183% |
| Strategy B (30s fixed) | 46,058 rec/s | 25,334ms | 2.961% |
| Strategy C (10s WAL) | 45,739 rec/s | 4,993ms | 0.207% |
| **Adaptive (proposed)** | **45,794 rec/s** | **12,264ms** | **0.036%** |

The adaptive algorithm achieves the lowest duplicate rate of all Spark strategies (0.036%) while maintaining competitive throughput — the right trade-off for ML feature pipelines where data correctness is critical.

**Implementation:** `src/adaptive_checkpoint.py`  
**Results:** `experiments/experiments/adaptive/adaptive_summary.csv`

---

## Systems Compared

| System | Fault-Tolerance Mechanism | Configured correctness target |
|---|---|---|
| Spark A | High-frequency micro-batch checkpoint (1s) | Exactly-once |
| Spark B | Interval-based checkpoint (30s) | Exactly-once |
| Spark C | Async WAL checkpoint (10s) | Exactly-once |
| Adaptive | Risk-based dynamic interval (2–45s) | Exactly-once |
| Flink F1 | Aligned Chandy-Lamport barrier snapshots (10s) | Exactly-once |
| Flink F2 | Unaligned barrier snapshots (10s) | Exactly-once |
| Flink F3 | Incremental RocksDB snapshots (30s) | Exactly-once |

These entries describe the configured correctness target, not an unconditional end-to-end guarantee. Actual behavior depends on source replayability, sink semantics, checkpoint durability, and the injected failure mode; the experiments below measure when observed behavior diverges from that target.

**Failure scenarios:** Executor node failure, driver/JobManager failure, checkpoint corruption, network partition

---

## Key Findings

> Full results in `experiments/summary.csv`, `experiments/flink/flink_summary.csv`, `experiments/experiments/adaptive/adaptive_summary.csv`, and `docs/findings.md`

1. **Adaptive checkpointing achieves the lowest Spark duplicate rate** — 0.036% under checkpoint corruption, 5× lower than Strategy C (WAL) and 82× lower than Strategy B, while maintaining competitive throughput.

2. **No silent duplicates were observed in the tested Flink configurations** — All Flink strategies produced a 0.00% duplicate rate in these trials, compared with 0.036%–2.961% for the Spark strategies under checkpoint corruption. This is an empirical result for the tested workloads and failure injections, not a universal guarantee for every source, sink, or deployment.

3. **Throughput gap between systems is smaller than expected** — Flink F1 (54,800 rec/s) vs Spark B (51,268 rec/s) is only 6.9%, suggesting Spark's correctness risk is not offset by proportional throughput gains.

4. **Recovery latency scales non-linearly with checkpoint interval** — Spark B (30s fixed) takes 25,334ms to recover from driver failure vs 5,061ms for Strategy A. Adaptive falls between these at 12,264ms with dramatically better correctness.

5. **For ML feature pipelines:** Flink F1 is the recommended production strategy — zero duplicates, highest throughput among exactly-once systems. Adaptive checkpointing is the recommended strategy when Spark must be used.

---

## Statistical Rigor

All results include 95% confidence intervals from 30 independent trials per configuration. Key comparisons use Welch's t-test with Cohen's d effect sizes. Full statistical analysis: `experiments/statistical_analysis.csv`.

| Comparison | t-statistic | p-value | Cohen's d | Interpretation |
|---|---|---|---|---|
| Spark B vs Adaptive (dup rate) | 23.4 | < 0.001 | 6.04 | Large effect |
| Spark A vs Adaptive (dup rate) | 17.7 | < 0.001 | 4.57 | Large effect |
| Spark C vs Adaptive (dup rate) | 12.2 | < 0.001 | 3.16 | Large effect |
| Flink F1 vs Spark B (throughput) | 7.2 | < 0.001 | 1.87 | Large effect |

---

## Research Artifacts

| Artifact | Description | Link |
|---|---|---|
| Workshop paper | ACM-style submission draft (8 sections) | `docs/workshop_paper.pdf` |
| Technical report | 7-section PDF with methodology, results, threats to validity | `docs/technical_report.pdf` |
| Experiment dashboard | Interactive HTML results dashboard with charts | `docs/experiment_dashboard.html` |
| Spark analysis notebook | 4 charts — throughput, latency, duplicates, heatmap | [nbviewer](https://nbviewer.org/github/rmedipallycic/spark-streaming-fault-tolerance/blob/main/notebooks/analysis.ipynb) |
| Flink comparison notebook | Full Spark vs Flink comparative analysis | [nbviewer](https://nbviewer.org/github/rmedipallycic/spark-streaming-fault-tolerance/blob/main/notebooks/spark_vs_flink_comparison.ipynb) |
| Cluster validation results | AWS EMR real-hardware results | `experiments/cluster-results/` |

---

## Quick Start

```bash
git clone https://github.com/rmedipallycic/spark-streaming-fault-tolerance.git
cd spark-streaming-fault-tolerance

# Start Kafka + Zookeeper
docker compose up -d

# Run a single experiment
./scripts/run_experiment.sh --framework spark --failure worker --checkpoint A

# Run adaptive checkpointing experiment
./scripts/run_experiment.sh --framework spark --failure driver --checkpoint adaptive

# Run ALL configurations x 30 trials
./scripts/run_experiment.sh --all

# Run adaptive algorithm standalone
python src/adaptive_checkpoint.py

# View results
jupyter notebook notebooks/spark_vs_flink_comparison.ipynb
