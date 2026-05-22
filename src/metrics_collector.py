"""
metrics_collector.py
--------------------
Collects and records performance metrics during fault tolerance benchmarking.
Tracks throughput, recovery latency, duplicate records, and checkpoint integrity
across all experimental runs.

Author: Rajshekar Medipally
GitHub: https://github.com/rmedipallycic
Research: Distributed ML Systems | Fault-Tolerant Stream Processing
"""

import csv
import json
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Optional, List, Dict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Metric Data Classes ──────────────────────────────────────────────────────

@dataclass
class ThroughputMetric:
    """Records throughput at a point in time."""
    timestamp:       float
    records_per_sec: float
    batch_id:        Optional[int] = None
    is_failure_active: bool = False


@dataclass
class CheckpointMetric:
    """Records checkpoint write events."""
    timestamp:      float
    strategy:       str
    storage_type:   str
    checkpoint_dir: str
    size_bytes:     int
    write_duration_ms: float
    success:        bool
    error_message:  Optional[str] = None


@dataclass
class RecoveryMetric:
    """Records failure injection and recovery events."""
    experiment_id:       str
    strategy:            str
    checkpoint_type:     str
    scenario:            str
    failure_time:        Optional[float] = None
    recovery_time:       Optional[float] = None
    recovery_latency_sec: Optional[float] = None
    duplicates_detected: int = 0
    data_loss_detected:  int = 0
    throughput_at_failure_rps: Optional[float] = None
    throughput_at_recovery_rps: Optional[float] = None
    throughput_degradation_pct: Optional[float] = None
    notes:               str = ""


@dataclass
class ExperimentSummary:
    """Full summary of a single benchmark experiment."""
    experiment_id:        str
    strategy:             str
    strategy_name:        str
    checkpoint_type:      str
    scenario:             str
    run_date:             str
    data_volume_records:  int
    normal_throughput_rps: float
    failure_throughput_rps: Optional[float]
    throughput_degradation_pct: Optional[float]
    recovery_latency_sec: Optional[float]
    duplicates_detected:  int
    data_loss_detected:   int
    checkpoint_size_mb:   Optional[float]
    notes:                str = ""


# ─── Metrics Collector ────────────────────────────────────────────────────────

class MetricsCollector:
    """
    Collects, buffers, and persists benchmark metrics for fault tolerance experiments.

    Usage:
        collector = MetricsCollector(
            experiment_id="EXP-025",
            strategy="A",
            checkpoint_type="local",
            scenario="node_failure",
            output_dir="experiments/"
        )
        collector.start()
        # ... run pipeline ...
        collector.record_failure()
        # ... pipeline recovers ...
        collector.record_recovery()
        collector.stop()
        collector.save_summary()
    """

    STRATEGY_NAMES = {
        "A": "High-Frequency Checkpointing",
        "B": "Interval-Based Checkpointing",
        "C": "Async WAL Checkpointing",
    }

    def __init__(
        self,
        experiment_id: str,
        strategy:       str,
        checkpoint_type: str,
        scenario:       str,
        output_dir:     str = "experiments/",
        sample_interval_sec: float = 5.0,
    ):
        self.experiment_id   = experiment_id
        self.strategy        = strategy
        self.checkpoint_type = checkpoint_type
        self.scenario        = scenario
        self.output_dir      = output_dir
        self.sample_interval = sample_interval_sec

        self.throughput_samples: List[ThroughputMetric] = []
        self.checkpoint_events:  List[CheckpointMetric] = []
        self.recovery_metric     = RecoveryMetric(
            experiment_id=experiment_id,
            strategy=strategy,
            checkpoint_type=checkpoint_type,
            scenario=scenario,
        )

        self._lock           = threading.Lock()
        self._running        = False
        self._start_time:    Optional[float] = None
        self._record_count   = 0
        self._baseline_rps:  Optional[float] = None

        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"MetricsCollector initialized — Experiment: {experiment_id}")

    # ─── Lifecycle ────────────────────────────────────────────────────────────

    def start(self):
        """Start metrics collection."""
        self._running    = True
        self._start_time = time.time()
        logger.info(f"[{self.experiment_id}] Metrics collection started.")

    def stop(self):
        """Stop metrics collection."""
        self._running = False
        elapsed = time.time() - self._start_time if self._start_time else 0
        logger.info(f"[{self.experiment_id}] Metrics collection stopped. Runtime: {elapsed:.2f}s")

    # ─── Record Events ────────────────────────────────────────────────────────

    def record_throughput(self, records_per_sec: float, batch_id: int = None):
        """Record a throughput sample."""
        with self._lock:
            metric = ThroughputMetric(
                timestamp=time.time(),
                records_per_sec=records_per_sec,
                batch_id=batch_id,
                is_failure_active=self.recovery_metric.failure_time is not None
                and self.recovery_metric.recovery_time is None,
            )
            self.throughput_samples.append(metric)

            # Track baseline (pre-failure average)
            if not metric.is_failure_active and self._baseline_rps is None:
                self._baseline_rps = records_per_sec

    def record_checkpoint(
        self,
        checkpoint_dir:    str,
        size_bytes:        int,
        write_duration_ms: float,
        success:           bool = True,
        error_message:     str = None,
    ):
        """Record a checkpoint write event."""
        with self._lock:
            metric = CheckpointMetric(
                timestamp=time.time(),
                strategy=self.strategy,
                storage_type=self.checkpoint_type,
                checkpoint_dir=checkpoint_dir,
                size_bytes=size_bytes,
                write_duration_ms=write_duration_ms,
                success=success,
                error_message=error_message,
            )
            self.checkpoint_events.append(metric)

    def record_failure(self, throughput_at_failure: float = None):
        """Record the moment a failure is injected."""
        with self._lock:
            self.recovery_metric.failure_time = time.time()
            self.recovery_metric.throughput_at_failure_rps = throughput_at_failure
            logger.info(f"[{self.experiment_id}] Failure recorded at {self.recovery_metric.failure_time:.3f}")

    def record_recovery(self, throughput_at_recovery: float = None):
        """Record the moment recovery is detected."""
        with self._lock:
            self.recovery_metric.recovery_time = time.time()
            self.recovery_metric.throughput_at_recovery_rps = throughput_at_recovery

            if self.recovery_metric.failure_time:
                latency = self.recovery_metric.recovery_time - self.recovery_metric.failure_time
                self.recovery_metric.recovery_latency_sec = round(latency, 3)
                logger.info(f"[{self.experiment_id}] Recovery detected. Latency: {latency:.3f}s")

            # Calculate throughput degradation
            if self._baseline_rps and throughput_at_recovery:
                degradation = ((self._baseline_rps - throughput_at_recovery) / self._baseline_rps) * 100
                self.recovery_metric.throughput_degradation_pct = round(degradation, 2)

    def record_duplicate(self, count: int = 1):
        """Record detected duplicate records."""
        with self._lock:
            self.recovery_metric.duplicates_detected += count
            logger.warning(f"[{self.experiment_id}] {count} duplicate(s) detected. Total: {self.recovery_metric.duplicates_detected}")

    def record_data_loss(self, count: int = 1):
        """Record detected data loss events."""
        with self._lock:
            self.recovery_metric.data_loss_detected += count
            logger.warning(f"[{self.experiment_id}] {count} data loss event(s). Total: {self.recovery_metric.data_loss_detected}")

    # ─── Aggregation ──────────────────────────────────────────────────────────

    def average_throughput(self, pre_failure_only: bool = False) -> float:
        """Calculate average throughput across samples."""
        samples = [
            s.records_per_sec for s in self.throughput_samples
            if not pre_failure_only or not s.is_failure_active
        ]
        return round(sum(samples) / len(samples), 2) if samples else 0.0

    def checkpoint_size_mb(self) -> Optional[float]:
        """Average checkpoint size in MB."""
        sizes = [c.size_bytes for c in self.checkpoint_events if c.success]
        if not sizes:
            return None
        return round(sum(sizes) / len(sizes) / (1024 * 1024), 2)

    # ─── Persistence ──────────────────────────────────────────────────────────

    def save_summary(self, data_volume_records: int = 0, notes: str = "") -> str:
        """Build and save ExperimentSummary to experiments/summary.csv."""
        normal_rps   = self.average_throughput(pre_failure_only=True) or self._baseline_rps or 0
        failure_rps  = self.recovery_metric.throughput_at_recovery_rps

        summary = ExperimentSummary(
            experiment_id=self.experiment_id,
            strategy=self.strategy,
            strategy_name=self.STRATEGY_NAMES.get(self.strategy, "Unknown"),
            checkpoint_type=self.checkpoint_type,
            scenario=self.scenario,
            run_date=datetime.utcnow().strftime("%Y-%m-%d"),
            data_volume_records=data_volume_records,
            normal_throughput_rps=normal_rps,
            failure_throughput_rps=failure_rps,
            throughput_degradation_pct=self.recovery_metric.throughput_degradation_pct,
            recovery_latency_sec=self.recovery_metric.recovery_latency_sec,
            duplicates_detected=self.recovery_metric.duplicates_detected,
            data_loss_detected=self.recovery_metric.data_loss_detected,
            checkpoint_size_mb=self.checkpoint_size_mb(),
            notes=notes,
        )

        output_path = os.path.join(self.output_dir, "summary.csv")
        file_exists = os.path.exists(output_path)

        with open(output_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(asdict(summary).keys()))
            if not file_exists:
                writer.writeheader()
            writer.writerow(asdict(summary))

        logger.info(f"[{self.experiment_id}] Summary saved to {output_path}")
        return output_path

    def save_throughput_log(self) -> str:
        """Save detailed throughput time series to experiments/."""
        output_path = os.path.join(
            self.output_dir,
            f"throughput_{self.experiment_id}_{self.scenario}.csv"
        )
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "records_per_sec", "batch_id", "is_failure_active"])
            writer.writeheader()
            for sample in self.throughput_samples:
                writer.writerow(asdict(sample))

        logger.info(f"[{self.experiment_id}] Throughput log saved to {output_path}")
        return output_path

    def print_summary(self):
        """Print experiment summary to console."""
        print("\n" + "=" * 60)
        print(f"EXPERIMENT SUMMARY — {self.experiment_id}")
        print("=" * 60)
        print(f"Strategy:             {self.strategy} — {self.STRATEGY_NAMES.get(self.strategy)}")
        print(f"Checkpoint Type:      {self.checkpoint_type}")
        print(f"Scenario:             {self.scenario}")
        print(f"Avg Throughput:       {self.average_throughput(pre_failure_only=True):.0f} rec/s")
        print(f"Recovery Latency:     {self.recovery_metric.recovery_latency_sec or 'N/A'} s")
        print(f"Duplicates Detected:  {self.recovery_metric.duplicates_detected}")
        print(f"Data Loss Events:     {self.recovery_metric.data_loss_detected}")
        print(f"Checkpoint Size:      {self.checkpoint_size_mb() or 'N/A'} MB")
        print(f"Throughput Drop:      {self.recovery_metric.throughput_degradation_pct or 'N/A'} %")
        print("=" * 60 + "\n")
