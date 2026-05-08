"""
pipeline.py
-----------
Core Spark Structured Streaming pipeline for fault tolerance benchmarking.
Tests exactly-once delivery semantics under three checkpoint strategies.

Author: Rajshekar Medipally
GitHub: https://github.com/rmedipallycic
Research: Distributed ML Systems | Fault-Tolerant Stream Processing
"""

import argparse
import logging
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Schema ───────────────────────────────────────────────────────────────────

EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(),  True),
    StructField("timestamp",  LongType(),    True),
    StructField("value",      DoubleType(),  True),
    StructField("source",     StringType(),  True),
])

# ─── Checkpoint Strategies ────────────────────────────────────────────────────

CHECKPOINT_STRATEGIES = {
    "A": {
        "name":        "High-Frequency Checkpointing",
        "trigger":     "1 second",
        "description": "Checkpoint on every micro-batch — maximum durability, highest overhead",
    },
    "B": {
        "name":        "Interval-Based Checkpointing",
        "trigger":     "30 seconds",
        "description": "Checkpoint every N seconds — balanced throughput vs durability",
    },
    "C": {
        "name":        "Async WAL Checkpointing",
        "trigger":     "10 seconds",
        "description": "Write-ahead log enabled — async checkpoint with WAL for recovery",
    },
}

# ─── Checkpoint Locations ─────────────────────────────────────────────────────

CHECKPOINT_LOCATIONS = {
    "local": "/tmp/spark-checkpoints",
    "hdfs":  "hdfs://localhost:9000/checkpoints",
    "s3":    "s3a://your-bucket/spark-checkpoints",
}

# ─── Spark Session ────────────────────────────────────────────────────────────

def create_spark_session(strategy: str, checkpoint_type: str) -> SparkSession:
    """Create and configure Spark session for the given strategy."""
    builder = (
        SparkSession.builder
        .appName(f"FaultToleranceBenchmark-Strategy{strategy}-{checkpoint_type}")
        .config("spark.sql.streaming.checkpointLocation",
                CHECKPOINT_LOCATIONS[checkpoint_type])
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
    )

    # WAL config for Strategy C
    if strategy == "C":
        builder = builder.config("spark.streaming.receiver.writeAheadLog.enable", "true")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session created — Strategy {strategy} | Checkpoint: {checkpoint_type}")
    return spark


# ─── Pipeline ─────────────────────────────────────────────────────────────────

def run_pipeline(
    strategy:        str  = "A",
    checkpoint_type: str  = "local",
    kafka_broker:    str  = "localhost:9092",
    kafka_topic:     str  = "fault-tolerance-test",
    scenario:        str  = "baseline",
    output_path:     str  = "experiments/",
    duration_secs:   int  = 300,
):
    """
    Run the streaming pipeline with the specified strategy and failure scenario.

    Args:
        strategy:        Checkpoint strategy (A, B, or C)
        checkpoint_type: Checkpoint storage (local, hdfs, s3)
        kafka_broker:    Kafka bootstrap server address
        kafka_topic:     Kafka topic to consume from
        scenario:        Failure scenario (baseline, node_failure, checkpoint_corruption)
        output_path:     Directory to write results CSV
        duration_secs:   How long to run the pipeline (seconds)
    """

    strat_config = CHECKPOINT_STRATEGIES[strategy]
    logger.info(f"Starting pipeline — {strat_config['name']}")
    logger.info(f"Scenario: {scenario} | Duration: {duration_secs}s")

    spark = create_spark_session(strategy, checkpoint_type)

    # ── Read from Kafka ────────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_broker)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON payload ─────────────────────────────────────────────────────
    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("processing_time", current_timestamp())
    )

    # ── Windowed aggregation ───────────────────────────────────────────────────
    aggregated = (
        parsed
        .withWatermark("processing_time", "10 seconds")
        .groupBy(window(col("processing_time"), "10 seconds", "5 seconds"), col("source"))
        .count()
    )

    # ── Write results ──────────────────────────────────────────────────────────
    checkpoint_dir = os.path.join(
        CHECKPOINT_LOCATIONS[checkpoint_type],
        f"strategy_{strategy}_{scenario}"
    )
    results_path = os.path.join(output_path, f"results_strategy_{strategy}_{scenario}.csv")

    query = (
        aggregated.writeStream
        .format("csv")
        .option("path", results_path)
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
        .trigger(processingTime=strat_config["trigger"])
        .start()
    )

    logger.info(f"Pipeline running — checkpoint at: {checkpoint_dir}")
    logger.info(f"Results writing to: {results_path}")

    # ── Run for duration ───────────────────────────────────────────────────────
    start_time = time.time()
    try:
        while query.isActive:
            elapsed = time.time() - start_time
            if elapsed >= duration_secs:
                logger.info(f"Duration reached ({duration_secs}s). Stopping pipeline.")
                query.stop()
                break

            status = query.status
            logger.info(
                f"[{elapsed:.0f}s] Active: {status['isDataAvailable']} | "
                f"Trigger active: {status['isTriggerActive']}"
            )
            time.sleep(10)

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user.")
        query.stop()

    finally:
        end_time = time.time()
        logger.info(f"Pipeline stopped. Total runtime: {end_time - start_time:.2f}s")
        spark.stop()


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Fault Tolerance Benchmark — Spark Structured Streaming"
    )
    parser.add_argument(
        "--strategy", choices=["A", "B", "C"], default="A",
        help="Checkpoint strategy: A (high-freq), B (interval), C (async WAL)"
    )
    parser.add_argument(
        "--checkpoint", choices=["local", "hdfs", "s3"], default="local",
        help="Checkpoint storage location"
    )
    parser.add_argument(
        "--broker", default="localhost:9092",
        help="Kafka bootstrap server"
    )
    parser.add_argument(
        "--topic", default="fault-tolerance-test",
        help="Kafka topic to consume"
    )
    parser.add_argument(
        "--scenario",
        choices=["baseline", "node_failure", "driver_failure", "checkpoint_corruption", "network_partition"],
        default="baseline",
        help="Failure scenario to run"
    )
    parser.add_argument(
        "--output", default="experiments/",
        help="Output directory for results CSV"
    )
    parser.add_argument(
        "--duration", type=int, default=300,
        help="Pipeline run duration in seconds"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    logger.info("=" * 60)
    logger.info("Fault Tolerance Benchmarking — Spark Structured Streaming")
    logger.info(f"Strategy:   {args.strategy} — {CHECKPOINT_STRATEGIES[args.strategy]['name']}")
    logger.info(f"Checkpoint: {args.checkpoint}")
    logger.info(f"Scenario:   {args.scenario}")
    logger.info(f"Duration:   {args.duration}s")
    logger.info("=" * 60)

    run_pipeline(
        strategy=args.strategy,
        checkpoint_type=args.checkpoint,
        kafka_broker=args.broker,
        kafka_topic=args.topic,
        scenario=args.scenario,
        output_path=args.output,
        duration_secs=args.duration,
    )
