# Experimental Setup and Reproducibility Guide

**Author:** Rajshekar Medipally  
**Project:** [spark-streaming-fault-tolerance](https://github.com/rmedipallycic/spark-streaming-fault-tolerance)  
**Last Updated:** 2025

---

## Overview

This document provides complete instructions for reproducing the fault tolerance benchmarking experiments described in `docs/findings.md`. All experiments were conducted in a controlled local environment using Docker-based Kafka infrastructure and Apache Spark in local mode.

---

## Environment Requirements

### Hardware

| Component | Minimum | Recommended |
|---|---|---|
| CPU | 4 cores | 8 cores |
| RAM | 8 GB | 16 GB |
| Disk | 20 GB free | 50 GB free |
| OS | Ubuntu 20.04+ / macOS 12+ | Ubuntu 22.04 |

### Software

| Component | Version | Notes |
|---|---|---|
| Python | 3.10+ | Required |
| Apache Spark | 3.5.0 | Via PySpark |
| Apache Kafka | 3.6 (via Docker) | Confluent Platform image |
| Docker | 24.0+ | For Kafka environment |
| Docker Compose | 2.20+ | For service orchestration |
| Java | JDK 11 or 17 | Required by Spark |

---

## Installation

### Step 1 — Clone the repository

```bash
git clone https://github.com/rmedipallycic/spark-streaming-fault-tolerance.git
cd spark-streaming-fault-tolerance
```

### Step 2 — Create a Python virtual environment

```bash
python3 -m venv venv
source venv/bin/activate        # Linux/macOS
# venv\Scripts\activate         # Windows
```

### Step 3 — Install dependencies

```bash
pip install -r requirements.txt
```

### Step 4 — Verify Java installation

```bash
java -version
# Should return: openjdk version "11.x.x" or "17.x.x"
```

### Step 5 — Start Kafka environment

```bash
docker-compose up -d
```

Verify all services are running:

```bash
docker-compose ps
# Expected: zookeeper, kafka, kafka-init, kafka-ui all running
```

Kafka UI is available at: **http://localhost:8080**

---

## Running Experiments

### Baseline Run (no failure injection)

```bash
python src/pipeline.py \
  --strategy A \
  --checkpoint local \
  --scenario baseline \
  --duration 300
```

### With Failure Injection

Run the pipeline and failure simulator simultaneously in two terminals:

**Terminal 1 — Start pipeline:**
```bash
python src/pipeline.py \
  --strategy A \
  --checkpoint local \
  --scenario node_failure \
  --duration 300
```

**Terminal 2 — Inject failure (after pipeline starts):**
```bash
python src/failure_simulator.py \
  --scenario node_failure \
  --delay 30
```

### Full Benchmark Matrix

To reproduce all 24 experiments from `experiments/summary.csv`:

```bash
# Strategy A — Local checkpoint
for scenario in baseline node_failure driver_failure checkpoint_corruption; do
    python src/pipeline.py --strategy A --checkpoint local --scenario $scenario --duration 300
done

# Strategy B — Local checkpoint
for scenario in baseline node_failure driver_failure checkpoint_corruption; do
    python src/pipeline.py --strategy B --checkpoint local --scenario $scenario --duration 300
done

# Strategy C — Local checkpoint
for scenario in baseline node_failure driver_failure checkpoint_corruption; do
    python src/pipeline.py --strategy C --checkpoint local --scenario $scenario --duration 300
done

# Network partition scenarios (requires sudo for iptables)
python src/pipeline.py --strategy A --checkpoint s3 --scenario network_partition --duration 300
python src/pipeline.py --strategy B --checkpoint s3 --scenario network_partition --duration 300
python src/pipeline.py --strategy C --checkpoint s3 --scenario network_partition --duration 300
```

---

## Checkpoint Strategy Configurations

| Strategy | Trigger | WAL | Config Parameter |
|---|---|---|---|
| A — High-Frequency | Every 1 second | No | `trigger(processingTime="1 second")` |
| B — Interval-Based | Every 30 seconds | No | `trigger(processingTime="30 seconds")` |
| C — Async WAL | Every 10 seconds | Yes | `spark.streaming.receiver.writeAheadLog.enable=true` |

---

## Storage Backend Configuration

### Local Filesystem (default)

No additional configuration required. Checkpoints written to `/tmp/spark-checkpoints`.

### HDFS

Start a local HDFS instance and update `config/spark_config.yaml`:

```yaml
checkpoints:
  hdfs: "hdfs://localhost:9000/checkpoints"
```

### AWS S3

Set environment variables before running:

```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1
```

Update `config/spark_config.yaml`:

```yaml
checkpoints:
  s3: "s3a://your-bucket/spark-checkpoints"
```

---

## Metrics Collection

Results are automatically written to `experiments/summary.csv` by `src/metrics_collector.py`.

Each row records:

| Column | Description |
|---|---|
| `experiment_id` | Unique run identifier |
| `strategy` | A, B, or C |
| `checkpoint_type` | local, hdfs, or s3 |
| `scenario` | Failure scenario applied |
| `normal_throughput_rps` | Baseline records/second |
| `recovery_latency_sec` | Time from failure to recovery |
| `duplicates_detected` | Silent duplicate record count |
| `throughput_degradation_pct` | % drop in throughput during failure |

---

## Failure Scenario Notes

| Scenario | Requirements | Notes |
|---|---|---|
| `baseline` | None | No failure injected |
| `node_failure` | Running Spark process | Kills executor PID |
| `driver_failure` | Running Spark driver | Kills driver PID |
| `checkpoint_corruption` | Active checkpoint dir | Corrupts 20% of checkpoint files |
| `network_partition` | **sudo privileges** | Uses iptables to block Kafka port |

---

## Teardown

```bash
# Stop Kafka environment
docker-compose down

# Remove checkpoint directories
rm -rf /tmp/spark-checkpoints

# Deactivate virtual environment
deactivate
```

---

## Known Limitations

- Network partition simulation requires `sudo` — not available in all environments
- S3 experiments require valid AWS credentials and an accessible bucket
- HDFS experiments require a running local Hadoop installation
- Results may vary across hardware configurations — reported findings are from a 4-core, 16GB RAM machine

---

*For questions or issues, contact: rmedipallyr2@gmail.com*
