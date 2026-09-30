# Fault Tolerance in Spark Structured Streaming and Flink: A Simulation Study and Adaptive Checkpointing Prototype

**Author:** Rajshekar Medipally
**GitHub:** [github.com/rmedipallycic](https://github.com/rmedipallycic)
**Status:** Work in progress. Current results are from simulation; live-system experiments are planned.

---

## Project Status (read first)

All numerical results in this repository come from **seeded simulation scripts**, not from measured Spark, Flink, Docker, or AWS EMR executions.

- `experiments/run_simulation.py` and `src/adaptive_checkpoint.py` generate throughput, recovery latency, duplicate rates, and data loss using random-number models.
- In these models, each strategy's duplicate behavior is **parameterized directly by the author**. The results therefore reflect the model's assumptions, not observed system behavior.
- No live fault-injection experiments have been run yet. The launcher script (`scripts/run_experiment.sh`) is not yet functional (see Known Issues).
- Draft papers in `docs/` were written before a provenance audit and overstate what was measured. They are being revised and should not be cited.

What this repository currently offers:
- A research question and an experimental design for measuring fault tolerance in stream processing
- The design of an adaptive checkpointing algorithm (ACS)
- A reproducible simulation harness that illustrates the intended comparison
- A plan for live Docker and cluster experiments

---

## Research Question

> How do fault-tolerance mechanisms in Apache Spark Structured Streaming and Apache Flink differ in throughput, recovery latency, and data correctness under failure — and what are the implications for ML feature pipelines?

---

## Proposed Contribution: Adaptive Checkpoint Selection (ACS)

Fixed checkpoint intervals trade overhead against recovery cost. A long interval (e.g., 30s) is cheap during stable operation but increases replay and duplicate exposure after a failure. A short interval (e.g., 1s) reduces replay but adds constant overhead.

ACS is a proposed feedback controller that adjusts the checkpoint interval using three pipeline health signals:

- **Throughput variance** — coefficient of variation of recent throughput samples
- **Error pressure** — exceptions per 1,000 records in the current window
- **Failure recency** — exponential decay signal since the last detected failure

Every 5 seconds, ACS computes a risk score in [0, 1]:

- **Risk > 0.45** → tighten the interval by 30%
- **4 consecutive stable windows** → relax the interval by 20%
- **Bounds:** 2s ≤ interval ≤ 45s

Weights (0.4 / 0.3 / 0.3) and thresholds were chosen heuristically. They have not been tuned or validated against a live system.

**Engineering note:** A running Structured Streaming query cannot change its trigger interval. A live ACS implementation will require query restarts or a custom trigger. This is an open design problem for the next phase.

> Implementation (simulation): [`src/adaptive_checkpoint.py`](src/adaptive_checkpoint.py)

---

## Simulation Results

### ACS vs. fixed-interval Spark strategies (simulated)

Source: `src/adaptive_checkpoint.py`, 480 simulated trials (4 strategies × 4 scenarios × 30 trials). Summary: `experiments/experiments/adaptive/adaptive_summary.csv`.

| Strategy | Baseline throughput | Recovery, driver failure | Dup rate, checkpoint corruption | Dup rate, driver failure | Dup rate, node failure |
|----------|--------------------:|-------------------------:|--------------------------------:|-------------------------:|-----------------------:|
| Strategy A (1s fixed) | 50,049 rec/s | 5,061 ms | 0.183% | 0.188% | 0.177% |
| Strategy B (30s fixed) | 49,909 rec/s | 25,334 ms | 2.961% | 4.432% | 5.414% |
| Strategy C (10s WAL) | 49,951 rec/s | 4,993 ms | 0.207% | 0.102% | 0.218% |
| ACS (proposed) | 50,010 rec/s | 12,264 ms | 0.036% | 0.066% | 0.009% |

**Interpretation — model behavior, not system measurements:**

- **Throughput:** Baseline throughput is nearly identical across strategies (~50,000 rec/s) because the simulator does not model checkpoint overhead differences. These results say nothing about the throughput cost of ACS.
- **Recovery latency:** Recovery time in the simulator equals the time since the last checkpoint, evaluated on a 5-second simulation tick. This is why Strategy A (1s interval) shows ~5,000 ms rather than ~1,000 ms. ACS shows longer driver-failure recovery than Strategies A and C in this model.
- **Duplicate rate:** ACS has the lowest duplicate rate in all three failure scenarios. In this simulator, ACS's duplicate rate is computed from its replay window, while the fixed strategies use constant duplicate-rate parameters. The advantage therefore follows from how the model was written. It illustrates the mechanism ACS is designed to exploit; live experiments are needed to test whether it exists in real Spark.

Note: `experiments/summary.csv` comes from a separately parameterized simulator (`run_simulation.py`). Its values for Strategies A, B, and C differ from the table above.

### Spark vs. Flink (simulated)

Source: `experiments/run_simulation.py` (360 Spark trials) and the Flink simulation (360 trials).

- **Baseline throughput:** Flink F1 54,759 rec/s vs. Spark B 51,268 rec/s, a 6.8% difference in the simulation.
- **Duplicates:** The Flink simulation produces zero duplicates and zero data loss **by construction**. Its model does not generate duplicates under any scenario. This reflects the documented design of Flink's Chandy-Lamport barrier snapshots. It is **not** an experimental finding about Flink.

### Simulation statistics

Welch's t-tests recalculated independently from the raw simulation data:

| Comparison | Welch's t | p-value | Cohen's d |
|------------|----------:|--------:|----------:|
| Spark B vs. ACS (duplicate rate) | 28.94 | < 0.001 | 7.47 |
| Spark A vs. ACS (duplicate rate) | 27.36 | < 0.001 | 7.06 |
| Spark C vs. ACS (duplicate rate) | 30.66 | < 0.001 | 7.92 |
| Flink F1 vs. Spark B (throughput) | 5.90 | < 0.001 | 1.52 |

These statistics describe separation between **simulated distributions whose means were set by the model**. They do not establish differences between real systems.

---

## Systems Modeled

| System | Mechanism | Intended correctness target |
|--------|-----------|-----------------------------|
| Spark A | Micro-batch checkpoint, 1s | Exactly-once |
| Spark B | Micro-batch checkpoint, 30s | Exactly-once |
| Spark C | Checkpoint with write-ahead log, 10s | Exactly-once |
| ACS | Risk-based dynamic interval, 2–45s | Exactly-once |
| Flink F1 | Aligned barrier snapshots, 10s | Exactly-once |
| Flink F2 | Unaligned barrier snapshots, 10s | Exactly-once |
| Flink F3 | Incremental RocksDB snapshots, 30s | Exactly-once |

Failure scenarios modeled: executor failure, driver/JobManager failure, checkpoint corruption, network partition.

---

## Planned Live Experiments

The next phase replaces simulated outputs with measured ones:

1. Run Spark Structured Streaming with Kafka in Docker Compose.
2. Inject failures: `docker kill` (executor/driver), overwriting the latest offset file (checkpoint corruption), `tc netem` (network partition).
3. Write sink records with unique IDs and compute duplicate rate = (output count − unique count) / expected count.
4. Preserve logs, checkpoint directory snapshots before and after injection, and per-trial JSON outputs.
5. Implement ACS against live Spark and compare it with fixed intervals.
6. Repeat on a multi-node cluster.

---

## Known Issues

- `src/flink_pipeline.py` is missing; the launcher's Flink path does not work.
- `src/pipeline.py` does not accept the launcher's `--trial` argument.
- No per-trial JSON output is produced, so the launcher's expected result files never exist.
- `experiments/run_simulation.py` ignores the `--input` and `--output` arguments the launcher passes.
- The aggregation step generates new simulation data instead of reading measured trial outputs.
- `experiments/statistical_analysis.csv` was generated by a separate script that sampled around fixed means. It is not derived from the trial files, and its values do not match them. It should not be used.
- `experiments/cluster-results/` contains copies of simulation outputs produced while running the scripts on an AWS EMR node. Because the scripts are seeded simulations, this shows only that they run in that environment. It is not a validation of results.

---

## How to Run the Simulations

```bash
git clone https://github.com/rmedipallycic/spark-streaming-fault-tolerance.git
cd spark-streaming-fault-tolerance
pip install -r requirements.txt

# Spark/Flink simulation harness
python experiments/run_simulation.py

# ACS simulation
python src/adaptive_checkpoint.py
```

---

## Project Structure

spark-streaming-fault-tolerance/
├── src/
│ ├── pipeline.py # Spark Structured Streaming pipeline (runner interface incomplete)
│ ├── adaptive_checkpoint.py # ACS algorithm + simulation
│ ├── failure_simulator.py # Fault injection helpers
│ ├── kafka_producer.py # Synthetic data generator
│ └── metrics_collector.py # Metrics collection
├── scripts/
│ └── run_experiment.sh # Experiment launcher (not yet functional)
├── experiments/
│ ├── run_simulation.py # Spark/Flink simulation harness
│ ├── summary.csv # Simulated Spark results
│ ├── raw/ # Simulated per-trial Spark data
│ ├── experiments/adaptive/ # Simulated ACS results (480 rows)
│ ├── flink/ # Simulated Flink results (360 rows)
│ └── cluster-results/ # Simulation outputs from a run on AWS EMR
├── notebooks/ # Analysis of simulation outputs
├── docs/ # Drafts under revision (see Project Status)
├── docker-compose.yml
└── requirements.txt


---

## Related Work

- Zaharia et al. (2013). *Discretized Streams: Fault-Tolerant Streaming Computation at Scale.* SOSP.
- Carbone et al. (2015). *Apache Flink: Stream and Batch Processing in a Single Engine.* IEEE Data Engineering Bulletin.
- Carbone et al. (2017). *State Management in Apache Flink.* VLDB.
- Chandy & Lamport (1985). *Distributed Snapshots: Determining Global States of Distributed Systems.* ACM TOCS.
- Karimov et al. (2018). *Benchmarking Distributed Stream Processing Engines.* ICDE.

---

## Roadmap

- [x] Research question and experimental design
- [x] ACS algorithm design
- [x] Simulation harness (Spark, Flink, ACS)
- [x] Provenance audit of simulation results
- [ ] Fix runner interface (`--trial`, JSON output, real aggregator)
- [ ] Implement Flink runner
- [ ] Live Docker Compose experiments with preserved logs
- [ ] Live ACS implementation against Spark
- [ ] Multi-node cluster experiments
- [ ] Revise draft papers using measured results

---

## Contact

**Rajshekar Medipally**
rmedipallycic@gmail.com
PhD Applicant — Computer Science
[github.com/rmedipallycic](https://github.com/rmedipallycic)
