# Future Work: S3 Checkpoint Consistency in Multi-Region Deployments

**Author:** Rajshekar Medipally
**Status:** Planned. Depends on the live single-region experiments listed in the README roadmap.

---

## Motivation

Spark Structured Streaming and Flink both persist checkpoints to durable storage, commonly Amazon S3. Within a single region, S3 has provided strong read-after-write consistency since December 2020. Cross-region replication, which operators use for disaster recovery, is asynchronous: a checkpoint written in one region becomes readable in another only after a replication delay.

If a job recovers in a second region using a replicated checkpoint, it may read a checkpoint that is older than the most recent one written in the primary region. That could lead to replayed or duplicated records. Whether this happens in practice, and how it interacts with checkpoint interval, has not been measured in this project.

This study is planned to follow the live single-region experiments. It will use the same fault-injection and duplicate-detection method.

---

## Research Questions

1. Does recovering from a cross-region replica of a Spark checkpoint produce duplicate or missing records, compared with recovering in the primary region?
2. How does the delay between checkpoint write and recovery attempt affect the likelihood of recovering from a stale checkpoint?
3. How does checkpoint interval (fixed short, fixed long, ACS) interact with replication delay?
4. Does Flink's snapshot-based recovery behave differently under the same conditions?

---

## Experimental Design

### Infrastructure

| Component | Primary region | Secondary region |
|-----------|----------------|------------------|
| Stream processing cluster | us-east-1 | us-west-2 (for recovery) |
| Checkpoint bucket | us-east-1 | us-west-2 (cross-region replica) |
| Kafka source | us-east-1 | us-east-1 |

### Procedure

For each checkpoint strategy (Spark fixed 1s, Spark fixed 30s, ACS, Flink):

1. Run the pipeline in us-east-1 with checkpoints written to the primary bucket.
2. At a fixed point in the run, stop the job to simulate a regional failure.
3. After a controlled delay (for example 0, 5, 15, 30, and 60 seconds), start recovery in us-west-2 from the replicated checkpoint bucket.
4. Record which checkpoint version the recovering job read, whether it was the latest one written, and the replication status of the relevant objects (S3 reports replication status per object).
5. Count output records by unique ID and compute duplicate and missing-record rates.
6. Preserve logs, checkpoint directory listings from both buckets, and per-trial outputs.

The delay between failure and recovery is the main independent variable. It is controlled by when recovery starts, not by modifying S3 behavior.

### Metrics

- Replication delay: time between checkpoint write in the primary bucket and availability in the replica
- Stale-checkpoint rate: fraction of recoveries that read a checkpoint older than the latest one written
- Duplicate rate and missing-record rate after recovery
- Recovery latency

---

## Prerequisites

This study cannot start until the following are complete:

- A working live experiment runner with per-trial output (see Known Issues in the README)
- Live single-region fault-injection results for the same strategies
- A live ACS implementation, if ACS is included

---

## Expected Contribution

If the experiments are completed, they would provide measurements of how cross-region checkpoint replication affects recovery correctness for Spark and Flink, and practical guidance on checkpoint interval choices for multi-region deployments. No results exist yet.

---

## Cost Estimate

A small experiment (two 3-node clusters in two regions for about three hours, plus replication and inter-region transfer) is expected to cost on the order of $5–15 on AWS on-demand pricing. This is an estimate, not a measured cost.
