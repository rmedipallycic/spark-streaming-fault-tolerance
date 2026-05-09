"""
failure_simulator.py
--------------------
Controlled failure injection for fault tolerance benchmarking.
Simulates node failure, checkpoint corruption, and network partition
to test Spark Structured Streaming recovery behavior.

Author: Rajshekar Medipally
GitHub: https://github.com/rmedipallycic
Research: Distributed ML Systems | Fault-Tolerant Stream Processing
"""

import argparse
import logging
import os
import random
import shutil
import signal
import subprocess
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Failure Scenarios ────────────────────────────────────────────────────────

SCENARIOS = {
    "node_failure":             "Kill a Spark executor process mid-batch",
    "driver_failure":           "Kill the Spark driver during checkpoint write",
    "checkpoint_corruption":    "Corrupt checkpoint directory to simulate storage failure",
    "network_partition":        "Simulate network partition between executor and Kafka broker",
}


# ─── Metrics Tracking ─────────────────────────────────────────────────────────

class FailureMetrics:
    """Track failure injection and recovery timing."""

    def __init__(self):
        self.failure_time      = None
        self.recovery_time     = None
        self.duplicates_detected = 0
        self.data_loss_detected  = 0

    def record_failure(self):
        self.failure_time = time.time()
        logger.info(f"[METRICS] Failure injected at: {self.failure_time:.3f}")

    def record_recovery(self):
        self.recovery_time = time.time()
        latency = self.recovery_time - self.failure_time if self.failure_time else 0
        logger.info(f"[METRICS] Recovery detected at: {self.recovery_time:.3f}")
        logger.info(f"[METRICS] Recovery latency: {latency:.3f}s")
        return latency

    def summary(self) -> dict:
        latency = (
            self.recovery_time - self.failure_time
            if self.failure_time and self.recovery_time
            else None
        )
        return {
            "failure_time":         self.failure_time,
            "recovery_time":        self.recovery_time,
            "recovery_latency_sec": round(latency, 3) if latency else None,
            "duplicates_detected":  self.duplicates_detected,
            "data_loss_detected":   self.data_loss_detected,
        }


# ─── Scenario Implementations ─────────────────────────────────────────────────

def simulate_node_failure(metrics: FailureMetrics, delay_sec: int = 30):
    """
    Kill a Spark executor process after a delay.
    Tests executor recovery and task rescheduling.
    """
    logger.info(f"[NODE FAILURE] Waiting {delay_sec}s before injecting failure...")
    time.sleep(delay_sec)

    # Find Spark executor processes
    try:
        result = subprocess.run(
            ["pgrep", "-f", "SparkSubmit"],
            capture_output=True, text=True
        )
        pids = result.stdout.strip().split("\n")

        if not pids or pids == [""]:
            logger.warning("[NODE FAILURE] No Spark processes found. Is the pipeline running?")
            return

        # Kill a non-driver process (last in list is typically an executor)
        target_pid = int(pids[-1])
        logger.info(f"[NODE FAILURE] Killing Spark executor PID: {target_pid}")
        metrics.record_failure()
        os.kill(target_pid, signal.SIGKILL)
        logger.info(f"[NODE FAILURE] Process {target_pid} killed. Monitoring recovery...")

    except Exception as e:
        logger.error(f"[NODE FAILURE] Failed to inject failure: {e}")


def simulate_driver_failure(metrics: FailureMetrics, delay_sec: int = 45):
    """
    Kill the Spark driver process during an active checkpoint write.
    Tests driver recovery and checkpoint state consistency.
    """
    logger.info(f"[DRIVER FAILURE] Waiting {delay_sec}s before injecting failure...")
    time.sleep(delay_sec)

    try:
        result = subprocess.run(
            ["pgrep", "-f", "SparkSubmit"],
            capture_output=True, text=True
        )
        pids = result.stdout.strip().split("\n")

        if not pids or pids == [""]:
            logger.warning("[DRIVER FAILURE] No Spark driver found.")
            return

        driver_pid = int(pids[0])
        logger.info(f"[DRIVER FAILURE] Killing Spark driver PID: {driver_pid}")
        metrics.record_failure()
        os.kill(driver_pid, signal.SIGKILL)
        logger.info("[DRIVER FAILURE] Driver killed. Check checkpoint state for consistency.")

    except Exception as e:
        logger.error(f"[DRIVER FAILURE] Failed: {e}")


def simulate_checkpoint_corruption(
    checkpoint_dir: str,
    metrics: FailureMetrics,
    delay_sec: int = 20,
):
    """
    Corrupt checkpoint directory files to simulate storage failure.
    Tests Spark's ability to detect and handle corrupted checkpoint state.
    """
    logger.info(f"[CHECKPOINT CORRUPTION] Target: {checkpoint_dir}")
    logger.info(f"[CHECKPOINT CORRUPTION] Waiting {delay_sec}s before corruption...")
    time.sleep(delay_sec)

    if not os.path.exists(checkpoint_dir):
        logger.warning(f"[CHECKPOINT CORRUPTION] Directory not found: {checkpoint_dir}")
        return

    # Find checkpoint files and corrupt a random subset
    checkpoint_files = []
    for root, dirs, files in os.walk(checkpoint_dir):
        for f in files:
            if not f.startswith("."):
                checkpoint_files.append(os.path.join(root, f))

    if not checkpoint_files:
        logger.warning("[CHECKPOINT CORRUPTION] No checkpoint files found yet.")
        return

    # Corrupt 20% of checkpoint files
    num_to_corrupt = max(1, len(checkpoint_files) // 5)
    targets = random.sample(checkpoint_files, num_to_corrupt)

    logger.info(f"[CHECKPOINT CORRUPTION] Corrupting {num_to_corrupt}/{len(checkpoint_files)} files")
    metrics.record_failure()

    for filepath in targets:
        try:
            with open(filepath, "wb") as f:
                f.write(os.urandom(128))  # Write random bytes
            logger.info(f"[CHECKPOINT CORRUPTION] Corrupted: {filepath}")
        except Exception as e:
            logger.error(f"[CHECKPOINT CORRUPTION] Could not corrupt {filepath}: {e}")

    logger.info("[CHECKPOINT CORRUPTION] Corruption complete. Monitor for duplicate or lost records.")


def simulate_network_partition(
    kafka_host: str = "localhost",
    kafka_port: int = 9092,
    metrics: FailureMetrics = None,
    delay_sec: int = 30,
    partition_duration: int = 15,
):
    """
    Block network traffic to Kafka broker using iptables.
    Tests stream processing behavior under broker unavailability.
    Note: Requires sudo privileges.
    """
    logger.info(f"[NETWORK PARTITION] Target: {kafka_host}:{kafka_port}")
    logger.info(f"[NETWORK PARTITION] Waiting {delay_sec}s before partition...")
    time.sleep(delay_sec)

    try:
        # Block outbound traffic to Kafka port
        block_cmd = [
            "sudo", "iptables", "-A", "OUTPUT",
            "-p", "tcp", "--dport", str(kafka_port),
            "-j", "DROP"
        ]
        subprocess.run(block_cmd, check=True)
        logger.info(f"[NETWORK PARTITION] Blocked traffic to port {kafka_port}")

        if metrics:
            metrics.record_failure()

        logger.info(f"[NETWORK PARTITION] Partition active for {partition_duration}s...")
        time.sleep(partition_duration)

        # Restore network
        unblock_cmd = [
            "sudo", "iptables", "-D", "OUTPUT",
            "-p", "tcp", "--dport", str(kafka_port),
            "-j", "DROP"
        ]
        subprocess.run(unblock_cmd, check=True)
        logger.info("[NETWORK PARTITION] Network restored. Monitoring recovery...")

        if metrics:
            metrics.record_recovery()

    except subprocess.CalledProcessError as e:
        logger.error(f"[NETWORK PARTITION] iptables command failed: {e}")
        logger.error("Note: Network partition simulation requires sudo privileges.")


# ─── Main Runner ──────────────────────────────────────────────────────────────

def run_scenario(
    scenario:       str,
    checkpoint_dir: str = "/tmp/spark-checkpoints",
    kafka_host:     str = "localhost",
    kafka_port:     int = 9092,
    delay_sec:      int = 30,
):
    """Run the specified failure scenario and report metrics."""

    metrics = FailureMetrics()
    logger.info("=" * 60)
    logger.info(f"Failure Simulator — Scenario: {scenario}")
    logger.info(f"Description: {SCENARIOS.get(scenario, 'Unknown')}")
    logger.info("=" * 60)

    if scenario == "node_failure":
        simulate_node_failure(metrics, delay_sec)

    elif scenario == "driver_failure":
        simulate_driver_failure(metrics, delay_sec)

    elif scenario == "checkpoint_corruption":
        simulate_checkpoint_corruption(checkpoint_dir, metrics, delay_sec)

    elif scenario == "network_partition":
        simulate_network_partition(kafka_host, kafka_port, metrics, delay_sec)

    else:
        logger.error(f"Unknown scenario: {scenario}")
        return

    # Wait for recovery signal
    logger.info("Waiting 60s to measure recovery...")
    time.sleep(60)
    metrics.record_recovery()

    # Print summary
    summary = metrics.summary()
    logger.info("=" * 60)
    logger.info("FAILURE SIMULATION COMPLETE")
    for k, v in summary.items():
        logger.info(f"  {k}: {v}")
    logger.info("=" * 60)

    return summary


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Failure Simulator — Fault Tolerance Benchmark"
    )
    parser.add_argument(
        "--scenario",
        choices=list(SCENARIOS.keys()),
        required=True,
        help="Failure scenario to simulate"
    )
    parser.add_argument("--checkpoint-dir", default="/tmp/spark-checkpoints")
    parser.add_argument("--kafka-host",     default="localhost")
    parser.add_argument("--kafka-port",     type=int, default=9092)
    parser.add_argument("--delay",          type=int, default=30,
                        help="Seconds to wait before injecting failure")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_scenario(
        scenario=args.scenario,
        checkpoint_dir=args.checkpoint_dir,
        kafka_host=args.kafka_host,
        kafka_port=args.kafka_port,
        delay_sec=args.delay,
    )
