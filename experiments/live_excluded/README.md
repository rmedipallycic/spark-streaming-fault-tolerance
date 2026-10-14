# Excluded live trials (strategy B, first attempt, October 14, 2026)

These 42 trials ran on commit `4a2dfe3`. They are kept for the record but are **not results** and are not read by `live/analyze.py`.

**Why.** During this grid, Spark's scratch files went to `/tmp`, which on this WSL2 Ubuntu is an in-memory filesystem of 3.8 GB. Each Spark start copies its Kafka connector jars into a new `/tmp/spark-*` directory, and a SIGKILLed JVM never deletes it. After about 50 trials `/tmp` was full. From `..._B_driver_failure_file_t09` onward, every Spark start failed with `No space left on device` (visible in each `spark.log`), so 32 of these trials never produced output or never restarted; their outcomes describe the full disk, not Spark.

**What was checked.** All 84 logs of that grid were scanned: no strategy-A trial and none of the first 10 strategy-B trials contains the error. The 20 strategy-A `restart_failed` outcomes all show `MalformedInputException` from the corrupted offset file, not the disk error.

**What changed.** From the next commit, each trial's Spark scratch files go to that trial's own work directory under `~/live-ft-work/`, which the runner deletes after the trial, and the grid clears leftovers at start. All of strategy B was then re-run on the fixed code (the 10 unaffected strategy-B trials here were re-run too, so that every strategy-B result comes from one commit).
