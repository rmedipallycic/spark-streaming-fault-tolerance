# Future Work: S3 Checkpoint Consistency Analysis in Multi-Region Deployments

**Author:** Rajshekar Medipally  
**Status:** Planned — methodology defined, execution pending  
**Related:** [docs/findings.md](findings.md) | [experiments/cluster-results/](../experiments/cluster-results/)

---

## Motivation

The current experimental results (840 trials, AWS EMR us-east-1) assume checkpoint storage
on a single-region S3 bucket. In production ML feature pipelines, checkpoints are frequently
stored on S3 with cross-region replication enabled for disaster recovery — a configuration
that introduces **S3 eventual consistency** as a new failure mode.

Amazon S3 provides strong read-after-write consistency for new object writes within a single
region (as of December 2020). However, **cross-region replication introduces propagation
delays** of 15 seconds to several minutes, depending on object size and network conditions.
When Spark reads a checkpoint during recovery across a region boundary, it may read a
stale or partially-replicated checkpoint — producing the same class of silent duplicates
observed in our checkpoint corruption experiments.

This document defines the methodology for a planned multi-region S3 consistency analysis.

---

## Research Questions

1. **RQ1:** Does S3 cross-region replication latency affect Spark checkpoint recovery
   correctness under driver failure?

2. **RQ2:** What is the relationship between checkpoint size and cross-region propagation
   delay, and how does this interact with recovery latency?

3. **RQ3:** Does Flink's barrier protocol provide the same zero-duplicate guarantee under
   cross-region S3 consistency delays as it does under single-region checkpoint corruption?

4. **RQ4:** What checkpoint interval minimizes the probability of reading a stale checkpoint
   during cross-region recovery?

---

## Experimental Design

### Infrastructure

| Component | Primary Region | Secondary Region |
|-----------|---------------|-----------------|
| EMR cluster (Spark) | us-east-1 | us-west-2 |
| S3 checkpoint bucket | us-east-1 | us-west-2 (replicated) |
| Kafka broker | us-east-1 | us-east-1 |
| Measurement agent | both | both |

### Failure Scenarios

| Scenario | Description | Expected Observation |
|----------|-------------|---------------------|
| **Baseline** | Single-region checkpoint, no failure | Matches existing results |
| **Cross-region read** | Driver fails; recovery reads checkpoint from us-west-2 replica | Stale checkpoint risk |
| **Replication lag injection** | Artificially delay S3 replication using bucket policy | Controlled consistency window |
| **Concurrent write** | Write and read checkpoint simultaneously across regions | Race condition detection |

### Metrics

- **Replication lag (ms):** Time between checkpoint write (us-east-1) and consistent read (us-west-2)
- **Stale read rate (%):** Percentage of recovery attempts that read a checkpoint older than the most recent
- **Silent duplicate rate (%):** Percentage of records delivered more than once after cross-region recovery
- **Recovery latency (ms):** End-to-end time from failure to resumed processing

### Procedure

```
For each checkpoint strategy in {A, B, C, Adaptive}:
  For each trial in 1..30:
    1. Start pipeline in us-east-1 with S3 checkpointing
    2. Enable S3 cross-region replication to us-west-2
    3. At T=30s: inject driver failure
    4. At T=30s + δ: attempt recovery from us-west-2 replica
       where δ ∈ {0s, 5s, 15s, 30s, 60s}  ← replication lag sweep
    5. Measure: stale read rate, duplicate rate, recovery latency
    6. Record checkpoint size at time of failure
```

### Replication Lag Sweep

The key independent variable is **δ** — the delay between checkpoint write and recovery
attempt. By sweeping δ across 5 values (0, 5, 15, 30, 60 seconds), we can characterize
the relationship between replication lag and silent duplicate production.

Expected result: duplicate rate increases monotonically with δ, with the inflection point
depending on checkpoint interval (Strategy A at 1s should be more resilient than Strategy B
at 30s).

---

## Predicted Results

Based on the single-region findings and S3 consistency documentation:

| Strategy | Predicted dup rate (δ=0s) | Predicted dup rate (δ=30s) | Predicted dup rate (δ=60s) |
|----------|--------------------------|---------------------------|---------------------------|
| Spark A (1s) | ~0.18% (matches current) | ~1.2% | ~3.5% |
| Spark B (30s) | ~2.96% (matches current) | ~8.4% | ~15%+ |
| Spark C (WAL) | ~0.21% (matches current) | ~1.5% | ~4.2% |
| Adaptive | ~0.04% (matches current) | ~0.3% | ~0.9% |
| Flink F1 | ~0.00% (matches current) | TBD | TBD |

**Key hypothesis:** Flink's barrier protocol should remain resilient to cross-region
consistency delays because recovery always rolls back to the last *complete* barrier
snapshot — it does not attempt to read a partially-replicated state. This would extend
the zero-duplicate guarantee to the cross-region setting.

If confirmed, this would be a strong argument for Flink in multi-region ML pipelines —
not just for correctness under corruption (current finding) but also for correctness
under network-induced consistency delays.

---

## Implementation Plan

### Phase 1 — Infrastructure Setup (2 hours)
```bash
# Create cross-region replication rule
aws s3api put-bucket-replication \
  --bucket spark-fault-tolerance-experiments \
  --replication-configuration file://replication-config.json

# Launch secondary cluster in us-west-2
aws emr create-cluster \
  --name fault-tolerance-benchmark-usw2 \
  --region us-west-2 \
  ...
```

### Phase 2 — Lag Injection (1 hour)
Use S3 Object Lambda or bucket policy to simulate replication delays:
```python
# replication_delay_injector.py
import boto3, time

def inject_replication_lag(bucket, key, lag_seconds):
    """
    Simulate cross-region replication lag by temporarily
    blocking read access to newly written objects.
    """
    s3 = boto3.client('s3', region_name='us-west-2')
    # Tag object as 'replicating' — deny reads for lag_seconds
    s3.put_object_tagging(
        Bucket=bucket, Key=key,
        Tagging={'TagSet': [{'Key': 'replication_status',
                             'Value': 'pending'}]}
    )
    time.sleep(lag_seconds)
    # Remove tag — allow reads
    s3.delete_object_tagging(Bucket=bucket, Key=key)
```

### Phase 3 — Experiment Execution (3 hours)
- 4 strategies × 5 lag values × 30 trials = 600 additional trials
- Estimated cost: $18-25 on AWS (two m5.xlarge clusters × ~3 hours)

### Phase 4 — Analysis (1 hour)
- Add results to `experiments/multi-region/`
- Update `docs/findings.md` with cross-region section
- Update workshop paper with new RQ3 results

---

## Expected Contributions

1. **First empirical characterization** of S3 eventual consistency effects on
   Spark checkpoint recovery correctness (to our knowledge)

2. **Validation of Flink's cross-region resilience** — extends the zero-duplicate
   finding from single-region corruption to multi-region consistency delays

3. **Practical guidance** for ML feature pipeline operators deploying on multi-region AWS:
   minimum checkpoint interval as a function of expected S3 replication lag

4. **Strengthens the workshop paper** from 3 failure scenarios to 4, making it more
   suitable for systems venues (DEBS, HotStorage, SoCC workshop)

---

## Cost Estimate

| Resource | Duration | Cost |
|----------|----------|------|
| EMR us-east-1 (m5.xlarge × 3) | 3 hours | ~$1.75 |
| EMR us-west-2 (m5.xlarge × 3) | 3 hours | ~$1.75 |
| S3 cross-region replication | 600 trials | ~$0.50 |
| Data transfer (inter-region) | ~10GB | ~$0.90 |
| **Total** | | **~$5.00** |

---

## Timeline

This analysis is planned for completion before workshop paper submission.
Current status: methodology defined, infrastructure scripts drafted.

---

*This document is part of the research planning artifacts for:*  
*"Adaptive Checkpoint Selection for Fault-Tolerant Stream Processing in ML Feature Pipelines"*  
*github.com/rmedipallycic/spark-streaming-fault-tolerance*
