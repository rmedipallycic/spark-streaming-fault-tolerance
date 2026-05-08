"""
kafka_producer.py
-----------------
Synthetic data generator for fault tolerance benchmarking.
Produces configurable volumes of event records to a Kafka topic.

Author: Rajshekar Medipally
GitHub: https://github.com/rmedipallycic
Research: Distributed ML Systems | Fault-Tolerant Stream Processing
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime
from confluent_kafka import Producer, KafkaException

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Data Sources ─────────────────────────────────────────────────────────────

SOURCES = [
    "banking-transactions",
    "patient-monitoring",
    "iot-sensors",
    "clickstream",
    "api-gateway",
]

# ─── Kafka Producer Config ────────────────────────────────────────────────────

def create_producer(broker: str) -> Producer:
    """Create and return a configured Kafka producer."""
    config = {
        "bootstrap.servers": broker,
        "client.id":         "fault-tolerance-benchmark-producer",
        "acks":              "all",
        "retries":           3,
        "batch.size":        16384,
        "linger.ms":         5,
    }
    producer = Producer(config)
    logger.info(f"Kafka producer connected to: {broker}")
    return producer


# ─── Event Generation ─────────────────────────────────────────────────────────

def generate_event(source: str = None) -> dict:
    """Generate a synthetic event record."""
    return {
        "event_id":  str(uuid.uuid4()),
        "timestamp": int(datetime.utcnow().timestamp() * 1000),
        "value":     round(random.uniform(0.0, 1000.0), 4),
        "source":    source or random.choice(SOURCES),
    }


def delivery_callback(err, msg):
    """Callback to track delivery success and failures."""
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        pass  # Suppress per-message logging for high-throughput runs


# ─── Producer Run ─────────────────────────────────────────────────────────────

def run_producer(
    broker:        str   = "localhost:9092",
    topic:         str   = "fault-tolerance-test",
    rate:          int   = 1000,
    total_records: int   = 1_000_000,
    source:        str   = None,
    burst_mode:    bool  = False,
):
    """
    Produce synthetic events to Kafka at a configurable rate.

    Args:
        broker:        Kafka bootstrap server
        topic:         Target Kafka topic
        rate:          Records per second (steady state)
        total_records: Total records to produce
        source:        Fixed source label (random if None)
        burst_mode:    If True, produce as fast as possible (no rate limiting)
    """
    producer    = create_producer(broker)
    produced    = 0
    failed      = 0
    start_time  = time.time()
    interval    = 1.0 / rate if not burst_mode else 0

    logger.info(f"Starting producer — Topic: {topic} | Rate: {rate} rec/s | Total: {total_records:,}")

    try:
        while produced < total_records:
            event   = generate_event(source)
            payload = json.dumps(event).encode("utf-8")

            try:
                producer.produce(
                    topic,
                    key=event["event_id"].encode("utf-8"),
                    value=payload,
                    callback=delivery_callback,
                )
                producer.poll(0)
                produced += 1

                if produced % 10_000 == 0:
                    elapsed  = time.time() - start_time
                    actual_rate = produced / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Produced: {produced:>10,} / {total_records:,} | "
                        f"Rate: {actual_rate:>8.0f} rec/s | "
                        f"Elapsed: {elapsed:.1f}s"
                    )

                if interval > 0:
                    time.sleep(interval)

            except KafkaException as e:
                logger.error(f"Kafka error at record {produced}: {e}")
                failed += 1
                if failed > 100:
                    logger.error("Too many failures — stopping producer.")
                    break

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")

    finally:
        logger.info("Flushing remaining messages...")
        producer.flush()

        elapsed    = time.time() - start_time
        actual_rate = produced / elapsed if elapsed > 0 else 0

        logger.info("=" * 50)
        logger.info(f"Producer finished.")
        logger.info(f"Total produced : {produced:,}")
        logger.info(f"Total failed   : {failed:,}")
        logger.info(f"Elapsed time   : {elapsed:.2f}s")
        logger.info(f"Effective rate : {actual_rate:.0f} records/sec")
        logger.info("=" * 50)


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Kafka Producer — Synthetic Event Generator for Fault Tolerance Benchmark"
    )
    parser.add_argument("--broker",   default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic",    default="fault-tolerance-test", help="Kafka topic")
    parser.add_argument("--rate",     type=int, default=1000, help="Records per second")
    parser.add_argument("--total",    type=int, default=1_000_000, help="Total records to produce")
    parser.add_argument("--source",   default=None, help="Fixed source label (random if not set)")
    parser.add_argument("--burst",    action="store_true", help="Burst mode — no rate limiting")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_producer(
        broker=args.broker,
        topic=args.topic,
        rate=args.rate,
        total_records=args.total,
        source=args.source,
        burst_mode=args.burst,
    )
