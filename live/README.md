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

## Adaptive checkpointing (ACS), live

`live/acs.py` is the controller; `live/stream_job.py --controller acs` runs it inside the Spark job. In Structured Streaming the trigger interval also sets how often the offset and commit logs are written, so ACS adapts the trigger. A running query cannot change its trigger, so each change stops the query **between batches** and starts it again from the same checkpoint in the same Spark session (no JVM restart). Every evaluation, switch and restart is logged to `control.jsonl` in the trial directory.

Two ACS variants, both fixed before any measured run (see the docstring in `live/acs.py`):
- **v1**: the rule as specified in the simulation (tighten above risk 0.45, otherwise count a stable window). No single signal can cross 0.45, and the interval relaxes during bursts.
- **v2**: tighten above 0.30, stable only below 0.15, hold in between.

New strategies in `run_trial.py`: `C` (fixed `--fixed-s`), `ACS` (`--acs-variant v1|v2`), and `SWITCH` (a fixed `--schedule` of interval changes, to measure what one switch costs). `--workload bursty` draws a rate multiplier (0.5x, 1x, 3x) every `--segment-s` seconds from `--workload-seed` (default: the trial number), so every strategy gets the same input for the same trial.

```bash
# what one interval switch costs (Kafka source)
python live/run_trial.py --run-type smoke --strategy SWITCH --failure none --sink foreachbatch --trial 0
# ACS on a bursty workload with a driver kill
python live/run_trial.py --run-type smoke --strategy ACS --acs-variant v2 --workload bursty \
  --failure driver_failure --sink foreachbatch --trial 1
```
