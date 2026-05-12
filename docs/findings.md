# Research Findings: Fault Tolerance in Apache Spark Structured Streaming

**Author:** Rajshekar Medipally  
**Project:** [spark-streaming-fault-tolerance](https://github.com/rmedipallycic/spark-streaming-fault-tolerance)  
**Status:** Preliminary — Active Research 2025–Present

---

## Overview

This document summarizes preliminary findings from benchmarking fault tolerance and exactly-once delivery semantics in Apache Spark Structured Streaming across three checkpoint strategies, three storage backends, and four controlled failure scenarios. Results are drawn from 24 benchmark runs processing 1M–5M synthetic event records.

---

## Key Findings

### Finding 1 — Exactly-Once Semantics Carry a Measurable Throughput Cost

High-frequency checkpointing (Strategy A) reduced throughput by **18–24%** compared to at-least-once delivery under normal operation. This cost is non-trivial in enterprise settings where pipeline SLAs are measured in milliseconds.

| Strategy | Normal Throughput (rec/s) | Overhead vs Baseline |
|---|---|---|
| A — High-Frequency | 4,820 | −18–24% vs no checkpoint |
| B — Interval-Based | 5,940 | −8–12% |
| C — Async WAL | 5,210 | −12–16% |

**Implication:** The throughput cost of exactly-once semantics is strategy-dependent and significant. Systems that assume "free" exactly-once guarantees underestimate the performance trade-off.

---

### Finding 2 — Recovery Latency Does Not Scale Linearly with Checkpoint Frequency

Counterintuitively, high-frequency checkpointing (Strategy A) did not always produce the fastest recovery. Under driver failure scenarios, Strategy A showed **31.87s recovery latency** vs Strategy C's **22.78s**, despite Strategy A checkpointing more frequently.

| Strategy | Node Failure Recovery (s) | Driver Failure Recovery (s) |
|---|---|---|
| A — High-Frequency | 18.42 | 31.87 |
| B — Interval-Based | 24.11 | 42.56 |
| C — Async WAL | 12.34 | 22.78 |

**Implication:** Checkpoint frequency alone does not determine recovery speed. The write-ahead log in Strategy C provides faster and more consistent recovery across all failure types.

---

### Finding 3 — Checkpoint Storage Backend Significantly Affects Recovery Consistency

S3-backed checkpoints introduced unpredictable recovery behavior under network partition scenarios due to **eventual consistency semantics**. Strategy B on S3 produced the worst observed outcome: **112.45s recovery latency** and **28 duplicate records**.

| Storage | Strategy | Scenario | Recovery (s) | Duplicates |
|---|---|---|---|---|
| Local | C | Network Partition | — | 0 |
| HDFS | C | Network Partition | 28.34 | 0 |
| S3 | A | Network Partition | 87.34 | 14 |
| S3 | B | Network Partition | 112.45 | 28 |
| S3 | C | Network Partition | 54.23 | 0 |

**Implication:** S3's eventual consistency model is fundamentally incompatible with strong exactly-once guarantees in Spark Structured Streaming without additional coordination mechanisms. HDFS provides stronger consistency guarantees and should be preferred for fault-critical pipelines.

---

### Finding 4 — Silent Duplicates Under Checkpoint Corruption (Strategy B)

Strategy B (interval-based checkpointing) produced **silent duplicate records** in checkpoint corruption trials — an average of **47 duplicates per run** — without triggering Spark's internal duplicate detection mechanism.

This is the most significant finding from a production reliability standpoint: corruption scenarios did not cause pipeline failure (which would be detectable) but instead caused silent data quality degradation (which is not).

| Strategy | Scenario | Duplicates | Data Loss | Detected by Spark? |
|---|---|---|---|---|
| A | Checkpoint Corruption | 0 | 0 | N/A — pipeline failed |
| B | Checkpoint Corruption | 47 (avg) | 0 | ❌ No |
| C | Checkpoint Corruption | 0 | 0 | ✅ WAL recovered cleanly |

**Implication:** Interval-based checkpointing without WAL creates a window of undetected data corruption under checkpoint storage failures. Production pipelines relying on Strategy B require additional application-level duplicate detection.

---

### Finding 5 — WAL Strategy Provides Best Overall Fault Tolerance Profile

Strategy C (Async WAL Checkpointing) produced the best overall fault tolerance across all scenarios:
- Lowest recovery latency under node and driver failure
- Zero duplicates across all failure scenarios
- Clean recovery from checkpoint corruption
- Significant throughput improvement over S3 under network partition

The trade-off is checkpoint storage size: **187.4 MB** per run vs **124.3 MB** (Strategy A) and **98.7 MB** (Strategy B). At 5M records, WAL checkpoint size grew to **937.1 MB** — a 5× increase — suggesting storage cost becomes significant at scale.

---

## Open Research Questions

These findings surface the following open questions that motivate further doctoral-level research:

1. **Formal specification of exactly-once semantics** — Under what formal conditions are exactly-once guarantees preserved vs violated in heterogeneous distributed stream processing? Can these conditions be specified as checkable invariants?

2. **Checkpoint corruption detection** — Why does Strategy B fail to detect checkpoint corruption that Strategy C recovers from? What formal property of WAL enables this detection, and can it be replicated without the storage overhead?

3. **S3 consistency and streaming semantics** — Can additional coordination mechanisms (distributed locks, conditional writes, version checking) restore strong exactly-once guarantees for S3-backed checkpoints? At what performance cost?

4. **Scalability of fault tolerance overhead** — Checkpoint storage cost grows non-linearly with data volume under WAL. What is the theoretical relationship between data volume, checkpoint frequency, and storage overhead? Can this be optimized?

5. **ML pipeline fault propagation** — How do the failure modes documented here propagate through downstream ML feature pipelines? Does silent data corruption in stream processing produce detectable anomalies in ML model behavior?

---

## Methodology Notes

- All experiments run on local Spark cluster (4 executors, 4GB each) with Kafka 3.6 broker
- Synthetic event data generated by `src/kafka_producer.py` at 1,000 records/second steady rate
- Failures injected by `src/failure_simulator.py` with 30-second delay after pipeline start
- Recovery detected by monitoring query status every 10 seconds
- Duplicate detection: post-run deduplication on `event_id` field
- Results in `experiments/summary.csv`

---

## Next Steps

- [ ] Extend to heterogeneous multi-node cluster (simulate production environment)
- [ ] Add ML feature pipeline downstream to measure corruption propagation
- [ ] Implement formal checkpoint consistency invariant checker
- [ ] Run federated learning training loop on top of fault-prone stream pipeline
- [ ] Submit findings as position paper to workshop on ML systems reliability

---

## References

1. Zaharia et al. (2013). *Discretized Streams: Fault-Tolerant Streaming Computation at Scale.* SOSP.
2. Carbone et al. (2015). *Apache Flink: Stream and Batch Processing in a Single Engine.* IEEE Data Engineering Bulletin.
3. Das et al. (2022). *Fault Tolerance in Stream Processing.* ACM SIGMOD.
4. Apache Spark Documentation. *Structured Streaming Programming Guide — Fault Tolerance Semantics.* spark.apache.org.
5. Huang & Bhatt (2023). *Exactly-Once Semantics in Distributed Streaming Systems: A Survey.* ACM Computing Surveys.

---
