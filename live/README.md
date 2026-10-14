# Live fault-injection runner

| File | Purpose |
| --- | --- |
| `stream_job.py` | Record-level Spark Structured Streaming job (Kafka source; file or foreachBatch sink) |
| `run_trial.py` | One trial: fresh topic, produce, inject failure, restart, count duplicates and missing records |
| `run_grid.sh` | Strategies A and B x failures x sinks x `TRIALS`, plus one baseline per strategy and sink |
| `analyze.py` | `experiments/live/summary.csv` and `summary.md` from the trial files |
| `setup_wsl.sh` | One-time setup in WSL2 Ubuntu |

Measured trials go to `experiments/live/<run_id>/`; smoke trials go to `experiments/live_smoke/`, which is never reported. Bulky data (the checkpoint, the output, and Spark's scratch files) lives under `~/live-ft-work/` during a trial and is deleted afterwards; the trial record keeps the counts, the duplicated and missing sequence numbers, the Spark log, and checkpoint snapshots.

`--source files` exists only to test the runner without Kafka and is refused for measured runs.
