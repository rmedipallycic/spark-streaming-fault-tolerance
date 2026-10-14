#!/bin/bash
# Live grid: strategies A (1 s) and B (30 s) x failures x sinks x TRIALS.
#   TRIALS=10 bash live/run_grid.sh        # measured runs -> experiments/live/ (about 4 hours)
#   STRATEGIES=B TRIALS=10 bash live/run_grid.sh   # only strategy B
#   bash live/run_grid.sh smoke            # 1 short trial per failure type, Kafka source
# Needs Kafka running: docker compose up -d zookeeper kafka
set -u
cd "$(dirname "$0")/.."
TRIALS="${TRIALS:-10}"
if [ -n "$(git status --porcelain -- live src)" ]; then
  echo "live/ or src/ has uncommitted changes; commit first so run records match the code." >&2
  [ "${1:-}" = smoke ] || exit 1
fi
if [ "${1:-}" = smoke ]; then
  for F in none driver_failure checkpoint_corruption; do
    python live/run_trial.py --run-type smoke --strategy A --failure $F --sink file --trial 0 \
      --records 12000 --rate 300 --kill-after 5
  done
  echo "smoke done."; exit 0
fi
mkdir -p experiments/live
# leftovers from SIGKILLed Spark JVMs of earlier runs (before scratch moved to the work dir)
rm -rf /tmp/spark-* /tmp/blockmgr-* /tmp/live-ft 2>/dev/null
for S in ${STRATEGIES:-A B}; do
  for K in file foreachbatch; do
    python live/run_trial.py --strategy $S --failure none --sink $K --trial 0 \
      || echo "TRIAL FAILED: $S none $K 0" | tee -a experiments/live/failed_trials.txt
    for F in driver_failure checkpoint_corruption; do
      for T in $(seq 0 $((TRIALS - 1))); do
        python live/run_trial.py --strategy $S --failure $F --sink $K --trial $T \
          || echo "TRIAL FAILED: $S $F $K $T" | tee -a experiments/live/failed_trials.txt
      done
    done
  done
done
python live/analyze.py
echo "done."
