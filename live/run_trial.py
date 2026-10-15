"""Run ONE live fault-injection trial and write its raw record to JSON.

A trial: create a fresh Kafka topic, start the record-level Spark job
(live/stream_job.py), produce N records with sequence numbers 0..N-1 at a
fixed rate, inject a failure once the job has produced output, restart the
job from the same checkpoint, wait until output stops changing, stop the job
gracefully, and count duplicated and missing records.

Failures:
  none                  baseline, no failure
  driver_failure        SIGKILL the whole Spark process group (in local mode the
                        driver and executors share one JVM), then restart
  checkpoint_corruption SIGKILL as above, overwrite the middle of the latest
                        offsets/ file in the checkpoint with random bytes, restart

Strategies (how the trigger interval is set):
  A       fixed 1 s          B   fixed 30 s
  C       fixed --fixed-s (default 10 s; meant to be set to ACS's average interval)
  ACS     Adaptive Checkpoint Selection (live/acs.py), starting at 10 s
  SWITCH  a fixed schedule of interval changes (--schedule), no ACS: measures
          what one interval switch costs

Workloads: steady (constant --rate) or bursty (rate multiplier drawn every
--segment-s seconds from 0.5x / 1x / 3x with --workload-seed, so every strategy
gets the same input schedule for the same seed).

Nothing here computes summary statistics; live/analyze.py reads these files.

  python live/run_trial.py --strategy A --failure driver_failure --sink file --trial 0
  python live/run_trial.py --strategy ACS --workload bursty --failure none --sink file --trial 0
"""
import argparse
import glob
import json
import os
import platform
import random
import shutil
import signal
import subprocess
import sys
import tarfile
import threading
import time
import uuid
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TRIGGERS = {"A": "1 second", "B": "30 seconds"}
STRATEGIES = ["A", "B", "C", "ACS", "SWITCH"]
BURST_MULTIPLIERS = (0.5, 1.0, 3.0)


def sh(cmd):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        return (r.stdout + r.stderr).strip()
    except Exception:
        return None


def environment():
    try:
        import pyspark
        pyspark_v = pyspark.__version__
    except Exception:
        pyspark_v = None
    try:
        import confluent_kafka
        ck_v = confluent_kafka.version()[0]
    except Exception:
        ck_v = None
    return {
        "git_commit": sh(["git", "-C", REPO, "rev-parse", "HEAD"]),
        "git_dirty": bool(sh(["git", "-C", REPO, "status", "--porcelain", "--", "live", "src"])),
        "argv": sys.argv,
        "pyspark": pyspark_v,
        "confluent_kafka": ck_v,
        "java": (sh(["java", "-version"]) or "").splitlines()[:2],
        "python": sys.version,
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "free_disk_gb": {d: round(shutil.disk_usage(d).free / 1e9, 2)
                         for d in ("/tmp", os.path.expanduser("~")) if os.path.isdir(d)},
        "started_utc": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------- producers

class KafkaFeed:
    """Idempotent producer, so any duplicates measured come from Spark, not Kafka."""

    def __init__(self, broker, topic, partitions):
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({"bootstrap.servers": broker})
        fut = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
        for f in fut.values():
            f.result(30)
        self.p = Producer({"bootstrap.servers": broker, "acks": "all",
                           "enable.idempotence": True, "linger.ms": 5})
        self.topic, self.acked, self.errors = topic, set(), 0

    def _cb(self, err, msg):
        if err is None:
            self.acked.add(json.loads(msg.value())["seq"])
        else:
            self.errors += 1

    def send(self, seq):
        v = json.dumps({"seq": seq, "event_id": str(uuid.uuid4())}).encode()
        self.p.produce(self.topic, key=str(seq).encode(), value=v, callback=self._cb)
        self.p.poll(0)

    def close(self):
        self.p.flush(60)


class FileFeed:
    """Offline test source: one small JSON file per 50 records (not for measured runs)."""

    def __init__(self, input_dir):
        self.dir = input_dir
        os.makedirs(input_dir, exist_ok=True)
        self.buf, self.acked, self.errors, self.n = [], set(), 0, 0

    def _flush(self):
        if not self.buf:
            return
        tmp = os.path.join(self.dir, f".tmp-{self.n}")
        with open(tmp, "w") as f:
            for s in self.buf:
                f.write(json.dumps({"seq": s, "event_id": str(uuid.uuid4())}) + "\n")
        os.rename(tmp, os.path.join(self.dir, f"part-{self.n:06d}.json"))
        self.acked.update(self.buf)
        self.buf, self.n = [], self.n + 1

    def send(self, seq):
        self.buf.append(seq)
        if len(self.buf) >= 50:
            self._flush()

    def close(self):
        self._flush()


def rate_schedule(workload, rate, segment_s, seed, n_segments=200):
    """List of (segment start offset in s, records per second)."""
    if workload == "steady":
        return [(0.0, rate)]
    rng = random.Random(seed)
    return [(i * segment_s, rate * rng.choice(BURST_MULTIPLIERS)) for i in range(n_segments)]


def produce(feed, n, schedule, log):
    """Send records 0..n-1 following a piecewise-constant rate schedule."""
    t0 = time.time()
    seg, t_seg, sent_at_seg = 0, 0.0, 0
    for s in range(n):
        while seg + 1 < len(schedule) and time.time() - t0 >= schedule[seg + 1][0]:
            seg += 1
            t_seg, sent_at_seg = schedule[seg][0], s
        feed.send(s)
        lag = t0 + t_seg + (s - sent_at_seg + 1) / schedule[seg][1] - time.time()
        if lag > 0:
            time.sleep(lag)
    feed.close()
    log["producer_done_s"] = time.time()


# ---------------------------------------------------------------- output reading

def committed_files(out_dir, sink):
    """Data files a reader should see: for the file sink, only those in _spark_metadata."""
    if sink == "foreachbatch":
        return sorted(glob.glob(os.path.join(out_dir, "batch-*.jsonl")))
    meta = os.path.join(out_dir, "_spark_metadata")
    files = set()
    for m in sorted(glob.glob(os.path.join(meta, "*"))):
        if os.path.basename(m).startswith("."):
            continue
        with open(m, encoding="utf-8", errors="replace") as f:
            for line in f.read().splitlines()[1:]:
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if e.get("action", "add") == "add":
                    files.add(os.path.join(out_dir, os.path.basename(e["path"])))
                else:
                    files.discard(os.path.join(out_dir, os.path.basename(e["path"])))
    return sorted(files)


def raw_files(out_dir, sink):
    pat = "batch-*.jsonl" if sink == "foreachbatch" else "part-*"
    return sorted(f for f in glob.glob(os.path.join(out_dir, pat)) if not f.endswith(".crc"))


def read_seqs(files):
    counts = {}
    for fp in files:
        try:
            with open(fp, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        s = json.loads(line).get("seq")
                        counts[s] = counts.get(s, 0) + 1
        except (OSError, json.JSONDecodeError):
            pass
    return counts


def tally(counts, expected):
    dup_seqs = sorted(s for s, c in counts.items() if c > 1)
    missing = sorted(expected - set(counts))
    return {"rows": sum(counts.values()), "distinct": len(counts),
            "duplicate_rows": sum(c - 1 for c in counts.values()),
            "duplicated_records": len(dup_seqs), "missing_records": len(missing),
            "unexpected_records": len(set(counts) - expected),
            "duplicated_seqs": dup_seqs[:200], "missing_seqs": missing[:200]}


# ---------------------------------------------------------------- job control

class Job:
    def __init__(self, cmd, log_path, tmp_dir):
        self.cmd, self.log_path, self.proc = cmd, log_path, None
        # Spark's scratch files go to the trial's own work directory (on disk), because
        # a SIGKILLed JVM never deletes them; the runner removes the directory afterwards.
        os.makedirs(tmp_dir, exist_ok=True)
        self.env = dict(os.environ)
        self.env["SPARK_LOCAL_DIRS"] = tmp_dir
        self.env["JAVA_TOOL_OPTIONS"] = (self.env.get("JAVA_TOOL_OPTIONS", "")
                                         + f" -Djava.io.tmpdir={tmp_dir}").strip()

    def start(self):
        self.log = open(self.log_path, "a", encoding="utf-8")
        self.log.write(f"\n=== start {time.time():.6f}\n")
        self.log.flush()
        self.proc = subprocess.Popen(self.cmd, stdout=self.log, stderr=subprocess.STDOUT,
                                     start_new_session=True, env=self.env)
        return time.time()

    def kill(self):
        t = time.time()
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.proc.wait()
        self.log.write(f"\n=== SIGKILL {t:.6f}\n")
        self.log.flush()
        return t

    def alive(self):
        return self.proc is not None and self.proc.poll() is None


def batch_commits(ckpt):
    return [f for f in glob.glob(os.path.join(ckpt, "commits", "*")) if os.path.basename(f).isdigit()]


def snapshot(src, dst):
    if os.path.isdir(src):
        with tarfile.open(dst, "w:gz") as t:
            t.add(src, arcname="checkpoint")


def corrupt_latest_offset(ckpt):
    offs = [f for f in glob.glob(os.path.join(ckpt, "offsets", "*"))
            if os.path.basename(f).isdigit()]
    if not offs:
        return None
    target = max(offs, key=lambda f: int(os.path.basename(f)))
    size = os.path.getsize(target)
    with open(target, "r+b") as f:
        f.seek(size // 3)
        f.write(os.urandom(max(8, size // 3)))
    return {"file": os.path.relpath(target, ckpt), "size": size}


def control_summary(path):
    """Counts from the job's control log; the full log stays in control.jsonl."""
    evs = []
    try:
        with open(path, encoding="utf-8") as f:
            evs = [json.loads(line) for line in f if line.strip()]
    except (OSError, ValueError):
        pass
    downs = sorted(e["downtime_s"] for e in evs if e["type"] == "first_batch_after_switch")
    return {"switches": sum(e["type"] == "switch" for e in evs),
            "switch_downtimes_s": [round(d, 3) for d in downs],
            "median_switch_downtime_s": round(downs[len(downs) // 2], 3) if downs else None,
            "intervals_s": [e["new_interval_s"] for e in evs if e["type"] == "switch"],
            "query_starts": sum(e["type"] == "query_start" for e in evs)}


def wait_for(pred, timeout, step=0.5):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return True
        time.sleep(step)
    return False


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--strategy", choices=STRATEGIES, required=True)
    p.add_argument("--fixed-s", type=float, default=10.0, help="strategy C interval (s)")
    p.add_argument("--schedule", default="10:30,2:30,10:30,2:30,10:30",
                   help="strategy SWITCH: interval_s:duration_s,...")
    p.add_argument("--acs-variant", choices=["v1", "v2"], default="v1",
                   help="ACS rule: v1 as specified, v2 revised thresholds (see live/acs.py)")
    p.add_argument("--workload", choices=["steady", "bursty"], default="steady")
    p.add_argument("--segment-s", type=float, default=15.0, help="bursty: seconds per rate segment")
    p.add_argument("--workload-seed", type=int, default=None,
                   help="bursty: schedule seed (default: the trial number)")
    p.add_argument("--failure", choices=["none", "driver_failure", "checkpoint_corruption"], required=True)
    p.add_argument("--sink", choices=["file", "foreachbatch"], required=True)
    p.add_argument("--trial", type=int, required=True)
    p.add_argument("--run-type", choices=["smoke", "measured"], default="measured")
    p.add_argument("--source", choices=["kafka", "files"], default="kafka")
    p.add_argument("--broker", default="localhost:9092")
    p.add_argument("--partitions", type=int, default=4)
    # 36,000 records at 300/s = 120 s of input, so that data is still arriving after the
    # latest possible kill (first output + 15 s + up to one 30 s trigger) and the restart
    p.add_argument("--records", type=int, default=36000)
    p.add_argument("--rate", type=float, default=300.0, help="records per second")
    p.add_argument("--kill-after", type=float, default=15.0,
                   help="base seconds after the first committed output to inject the failure; "
                        "a per-trial offset spreads kills across the trigger interval")
    p.add_argument("--settle", type=float, default=None,
                   help="stop when output is unchanged this long after the producer finished "
                        "(default: 2 x trigger + 15 s)")
    p.add_argument("--timeout", type=float, default=600.0)
    p.add_argument("--out-root", default=None)
    a = p.parse_args()
    if a.source == "files" and a.run_type == "measured":
        p.error("the files source is only for smoke tests of the runner")

    if a.strategy in TRIGGERS:
        controller, trigger, trig_s = "fixed", TRIGGERS[a.strategy], (1.0 if a.strategy == "A" else 30.0)
    elif a.strategy == "C":
        controller, trigger, trig_s = "fixed", f"{int(round(a.fixed_s * 1000))} milliseconds", a.fixed_s
    elif a.strategy == "ACS":
        controller, trigger, trig_s = "acs", None, 10.0     # ACS starts at 10 s
    else:
        controller, trigger, trig_s = "schedule", None, 10.0
    # adaptive intervals can reach 45 s, so wait long enough for one more batch
    settle_trig = 45.0 if controller != "fixed" else trig_s
    settle = a.settle if a.settle is not None else 2 * settle_trig + 15.0
    seed = a.trial if a.workload_seed is None else a.workload_seed
    schedule = rate_schedule(a.workload, a.rate, a.segment_s, seed)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    label = f"ACS{a.acs_variant}" if a.strategy == "ACS" else a.strategy
    run_id = f"{stamp}_{label}_{a.workload}_{a.failure}_{a.sink}_t{a.trial:02d}"
    root = a.out_root or os.path.join(REPO, "experiments", "live" if a.run_type == "measured" else "live_smoke")
    out = os.path.join(root, run_id)
    # bulky data stays out of git, on disk rather than in /tmp (tmpfs on some WSL setups)
    work = os.path.join(os.path.expanduser("~"), "live-ft-work", run_id)
    os.makedirs(out, exist_ok=True)
    ckpt, sink_dir, inp = (os.path.join(work, d) for d in ("checkpoint", "output", "input"))
    stop_file = os.path.join(work, "STOP")
    os.makedirs(work, exist_ok=True)

    topic = f"ft-{run_id.lower().replace('_', '-')}"
    cmd = [sys.executable, os.path.join(HERE, "stream_job.py"), "--source", a.source,
           "--broker", a.broker, "--topic", topic, "--input-dir", inp, "--checkpoint", ckpt,
           "--output", sink_dir, "--sink", a.sink, "--stop-file", stop_file,
           "--controller", controller, "--control-log", os.path.join(out, "control.jsonl"),
           "--state-file", os.path.join(work, "controller_state.json")]
    if trigger:
        cmd += ["--trigger", trigger]
    if controller == "acs":
        cmd += ["--acs-variant", a.acs_variant]
    if controller == "schedule":
        cmd += ["--schedule", a.schedule]
    rec = {"run_id": run_id, "run_type": a.run_type, "config": vars(a), "controller": controller,
           "trigger": trigger, "settle_s": settle, "workload_schedule": schedule[:40],
           "environment": environment(), "events": {}}
    ev = rec["events"]

    feed = KafkaFeed(a.broker, topic, a.partitions) if a.source == "kafka" else FileFeed(inp)
    job = Job(cmd, os.path.join(out, "spark.log"), os.path.join(work, "tmp"))
    ev["job_start_1"] = job.start()
    t_prod = threading.Thread(target=produce, args=(feed, a.records, schedule, ev), daemon=True)
    ev["producer_start"] = time.time()
    t_prod.start()

    def n_committed():
        return len(read_seqs(committed_files(sink_dir, a.sink)))

    # wait for the first committed output before injecting anything
    got = wait_for(lambda: n_committed() > 0 or not job.alive(), 300)
    ev["first_output"] = time.time() if got and job.alive() else None
    rec["outcome"] = "ok"
    if ev["first_output"] is None:
        rec["outcome"] = "no_output_before_failure"
    elif a.failure != "none":
        # golden-ratio sequence: trials 0..9 sample different phases of the batch cycle
        phase = (a.trial * 0.6180339887) % 1.0
        ev["kill_delay_s"] = a.kill_after + phase * trig_s
        time.sleep(ev["kill_delay_s"])
        ev["committed_before_failure"] = n_committed()
        ev["kill"] = job.kill()
        snapshot(ckpt, os.path.join(out, "checkpoint_before_restart.tgz"))
        if a.failure == "checkpoint_corruption":
            rec["corruption"] = corrupt_latest_offset(ckpt)
            snapshot(ckpt, os.path.join(out, "checkpoint_after_corruption.tgz"))
        commits_before = set(batch_commits(ckpt))
        ev["job_start_2"] = job.start()
        # recovery = first micro-batch committed after the restart (checkpoint commits/)
        new = wait_for(lambda: set(batch_commits(ckpt)) - commits_before or not job.alive()
                       or (ev.get("producer_done_s") and time.time() - ev["job_start_2"] > 120), 300)
        if job.alive() and set(batch_commits(ckpt)) - commits_before:
            ev["first_commit_after_restart"] = time.time()
            ev["recovery_latency_s"] = ev["first_commit_after_restart"] - ev["kill"]
        elif not job.alive():
            rec["outcome"] = "restart_failed"
            ev["restart_exit_code"] = job.proc.returncode
        else:
            rec["outcome"] = "no_batch_after_restart"

    # wait for the producer, then until committed output stops changing
    t_prod.join(timeout=a.timeout)
    if rec["outcome"] in ("ok", "no_batch_after_restart"):
        last, last_t = -1, time.time()
        t_end = time.time() + a.timeout
        while time.time() < t_end and job.alive():
            n = n_committed()
            if n != last:
                last, last_t = n, time.time()
            elif time.time() - last_t >= settle:
                break
            time.sleep(1)
        ev["settled"] = time.time()
    open(stop_file, "w").close()
    if not wait_for(lambda: not job.alive(), 120):
        job.kill()
    ev["job_end"] = time.time()
    ev["final_exit_code"] = job.proc.returncode

    expected = set(feed.acked)
    rec["produced_acked"] = len(expected)
    rec["producer_errors"] = feed.errors
    rec["committed"] = tally(read_seqs(committed_files(sink_dir, a.sink)), expected)
    rec["raw_files"] = tally(read_seqs(raw_files(sink_dir, a.sink)), expected)
    rec["n_committed_files"] = len(committed_files(sink_dir, a.sink))
    rec["n_raw_files"] = len(raw_files(sink_dir, a.sink))
    rec["n_batch_commits"] = len(batch_commits(ckpt))
    rec["control"] = control_summary(os.path.join(out, "control.jsonl"))
    with open(os.path.join(out, "trial.json"), "w", encoding="utf-8") as f:
        json.dump(rec, f, indent=2)
    c = rec["committed"]
    print(f"{run_id}: outcome={rec['outcome']} expected={len(expected)} rows={c['rows']} "
          f"dup_rows={c['duplicate_rows']} missing={c['missing_records']} "
          f"recovery={ev.get('recovery_latency_s')} switches={rec['control']['switches']} "
          f"median_switch_downtime={rec['control']['median_switch_downtime_s']}")
    shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
