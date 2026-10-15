"""Record-level Spark Structured Streaming job for live fault-injection trials.

Unlike src/pipeline.py (windowed counts), this job keeps every record's
sequence number, so the trial runner can count duplicated and missing records
after a failure. It runs until the runner creates a stop file, then stops the
query gracefully.

Source:  kafka (measured runs) or files (offline tests of the runner only)
Sink:    file         Spark's built-in JSON file sink: exactly-once output via
                      the _spark_metadata commit log
         foreachbatch every micro-batch appended as a new plain file:
                      at-least-once (a replayed batch is written again)

Controller (how the trigger interval is chosen):
  fixed     one interval for the whole run (--trigger), as in the first grid
  acs       Adaptive Checkpoint Selection (live/acs.py) changes the interval
            every 5 s; a change stops the query and starts it again from the
            same checkpoint, inside the same Spark session (no JVM restart)
  schedule  a fixed list of interval:seconds steps (--schedule), used to
            measure what one interval switch costs

Every start, stop, switch and ACS evaluation is appended to --control-log
(JSON lines, flushed at once, so the log survives a SIGKILL). --state-file
keeps the current interval and whether the last stop was clean, so that after
a crash the restarted job resumes the interval and counts the crash as a
failure for ACS's recency signal.
"""
import argparse
import json
import os
import sys
import time
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import LongType, StringType, StructField, StructType

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import acs as acs_mod  # noqa: E402

SCHEMA = StructType([
    StructField("seq", LongType(), True),
    StructField("event_id", StringType(), True),
    StructField("pt", LongType(), True),       # producer send time, epoch ms
])


class ControlLog:
    def __init__(self, path):
        self.f = open(path, "a", encoding="utf-8") if path else None

    def __call__(self, kind, **kw):
        if self.f:
            self.f.write(json.dumps({"t": time.time(), "type": kind, **kw}) + "\n")
            self.f.flush()


def read_state(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def write_state(path, **kw):
    if path:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(kw, f)
        os.replace(tmp, path)


def ms(seconds):
    return f"{int(round(seconds * 1000))} milliseconds"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["kafka", "files"], default="kafka")
    p.add_argument("--broker", default="localhost:9092")
    p.add_argument("--topic")
    p.add_argument("--input-dir", help="files source only")
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--trigger", help='fixed controller, e.g. "1 second"')
    p.add_argument("--sink", choices=["file", "foreachbatch"], required=True)
    p.add_argument("--stop-file", required=True)
    p.add_argument("--controller", choices=["fixed", "acs", "schedule"], default="fixed")
    p.add_argument("--schedule", default="",
                   help='schedule controller: "interval_s:duration_s,..." e.g. "10:30,2:30"')
    p.add_argument("--acs-variant", choices=sorted(acs_mod.VARIANTS), default="v1")
    p.add_argument("--control-log")
    p.add_argument("--state-file")
    a = p.parse_args()
    if a.controller == "fixed" and not a.trigger:
        p.error("--trigger is required with the fixed controller")
    log = ControlLog(a.control_log)

    import pyspark
    builder = (SparkSession.builder.appName("live-ft-trial").master("local[2]")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false"))
    if a.source == "kafka":
        builder = builder.config(
            "spark.jars.packages",
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if a.source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", a.broker)
               .option("subscribe", a.topic)
               .option("startingOffsets", "earliest")
               .option("failOnDataLoss", "true")
               .load())
        rows = (raw.select(from_json(col("value").cast("string"), SCHEMA).alias("d"),
                           col("partition").alias("kpartition"), col("offset").alias("koffset"))
                .select("d.seq", "d.event_id", "d.pt", "kpartition", "koffset"))
    else:
        rows = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 50).json(a.input_dir)

    if a.sink == "foreachbatch":
        os.makedirs(a.output, exist_ok=True)

    def write_batch(df, batch_id):
        recs = [r.asDict() for r in df.collect()]
        path = os.path.join(a.output, f"batch-{batch_id:06d}-{uuid.uuid4().hex[:8]}.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            for r in recs:
                f.write(json.dumps(r) + "\n")

    def start(trigger):
        w = rows.writeStream.option("checkpointLocation", a.checkpoint).trigger(processingTime=trigger)
        if a.sink == "file":
            return w.format("json").option("path", a.output).outputMode("append").start()
        return w.foreachBatch(write_batch).start()

    # ---- initial interval and failure detection (from the state file)
    state = read_state(a.state_file) if a.state_file else None
    crashed = state is not None and not state.get("clean_stop", False)
    now = time.time()
    if a.controller == "fixed":
        interval, trigger = None, a.trigger
    elif a.controller == "acs":
        interval = float(state["interval_s"]) if state and state.get("interval_s") else acs_mod.START_INTERVAL_S
        trigger = ms(interval)
    else:
        steps = [tuple(float(x) for x in s.split(":")) for s in a.schedule.split(",") if s]
        if not steps:
            p.error("--schedule is empty")
        step_i, interval = 0, steps[0][0]
        trigger = ms(interval)
    ctrl = (acs_mod.ACS(interval, last_failure=now if crashed else None, variant=a.acs_variant)
            if a.controller == "acs" else None)
    write_state(a.state_file, interval_s=interval, clean_stop=False)

    q = start(trigger)
    log("query_start", trigger=trigger, interval_s=interval, controller=a.controller,
        acs_variant=a.acs_variant if ctrl else None,
        resumed_after_crash=crashed, run_id=str(q.runId))
    print(f"STREAM_STARTED {time.time():.6f}", flush=True)

    seen = set()             # (runId, batchId) of progress already handed to ACS
    pending_switch = None    # switch waiting for the new query's first batch
    next_eval = time.time() + acs_mod.OBS_WINDOW_S
    step_end = time.time() + (steps[0][1] if a.controller == "schedule" else 0)

    def new_batches():
        out = []
        for pr in q.recentProgress or []:
            key = (pr.get("runId"), pr.get("batchId"))
            if key not in seen:
                seen.add(key)
                out.append(pr)
        return out

    def switch(new_interval, reason):
        nonlocal q, interval, pending_switch
        # stop between batches, never inside one: an interrupted foreachBatch batch
        # may already have written its file and would be written again on restart
        t_req = time.time()
        while (q.status or {}).get("isTriggerActive") and time.time() - t_req < max(interval or 0, 5) + 30:
            time.sleep(0.05)
        t0 = time.time()
        q.stop()
        t1 = time.time()
        if q.exception() is not None:
            log("query_exception", error=str(q.exception()))
            return False
        q = start(ms(new_interval))
        t2 = time.time()
        log("switch", old_interval_s=interval, new_interval_s=new_interval, reason=reason,
            t_requested=t_req, wait_idle_s=t0 - t_req, t_stop_called=t0, t_stopped=t1, t_started=t2, stop_s=t1 - t0, start_s=t2 - t1,
            run_id=str(q.runId))
        pending_switch = {"t_stop_called": t0, "run_id": str(q.runId), "new_interval_s": new_interval}
        interval = new_interval
        write_state(a.state_file, interval_s=interval, clean_stop=False)
        return True

    while q.isActive and not os.path.exists(a.stop_file):
        time.sleep(0.5)
        if pending_switch and q.lastProgress and str(q.lastProgress.get("runId")) == pending_switch["run_id"]:
            t = time.time()
            log("first_batch_after_switch", new_interval_s=pending_switch["new_interval_s"],
                batch_id=q.lastProgress.get("batchId"), t_observed=t,
                downtime_s=t - pending_switch["t_stop_called"])
            pending_switch = None
        if a.controller == "acs" and time.time() >= next_eval:
            now = time.time()
            next_eval = now + acs_mod.OBS_WINDOW_S
            sig, proposed = ctrl.step(now, new_batches())
            log("acs_eval", interval_s=interval, proposed_s=proposed, **sig)
            if proposed is not None and not switch(proposed, "acs"):
                break
            ctrl.interval = interval
        elif a.controller == "schedule" and time.time() >= step_end and step_i + 1 < len(steps):
            step_i += 1
            step_end = time.time() + steps[step_i][1]
            if not switch(steps[step_i][0], f"schedule step {step_i}"):
                break

    if q.isActive:
        q.stop()
    write_state(a.state_file, interval_s=interval, clean_stop=True)
    log("query_stop", interval_s=interval)
    print(f"STREAM_STOPPED {time.time():.6f}", flush=True)
    exc = q.exception()
    spark.stop()
    if exc is not None:
        print(f"QUERY_EXCEPTION {exc}", flush=True)
        sys.exit(3)


if __name__ == "__main__":
    main()
