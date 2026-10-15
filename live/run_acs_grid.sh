#!/bin/bash
# ACS comparison grid -> experiments/live_acs/
#   bash live/run_acs_grid.sh                       # defaults below
#   STRATEGIES="ACSv1 ACSv2 A B" WORKLOADS=bursty SINKS=foreachbatch TRIALS=10 bash live/run_acs_grid.sh
#   STRATEGIES=C FIXED_S=4.5 bash live/run_acs_grid.sh   # C at ACS's measured mean interval
#
# Trial t uses workload seed t and the same kill time for every strategy
# (KILL_SPAN_S), so strategies are compared on identical input and failures.
# Needs Kafka running: docker compose up -d zookeeper kafka
set -u
cd "$(dirname "$0")/.."
STRATEGIES="${STRATEGIES:-ACSv1 ACSv2 A B}"
WORKLOADS="${WORKLOADS:-bursty}"
SINKS="${SINKS:-foreachbatch}"
FAILURES="${FAILURES:-driver_failure}"
TRIALS="${TRIALS:-10}"
FIXED_S="${FIXED_S:-10}"
KILL_SPAN_S="${KILL_SPAN_S:-30}"
if [ -n "$(git status --porcelain -- live src)" ]; then
  echo "live/ or src/ has uncommitted changes; commit first so run records match the code." >&2
  exit 1
fi
mkdir -p experiments/live_acs
rm -rf /tmp/spark-* /tmp/blockmgr-* 2>/dev/null
for T in $(seq 0 $((TRIALS - 1))); do          # trial-major: strategies interleave over time
  for W in $WORKLOADS; do
    for K in $SINKS; do
      for F in $FAILURES; do
        for S in $STRATEGIES; do
          case $S in
            ACSv1) ARGS="--strategy ACS --acs-variant v1" ;;
            ACSv2) ARGS="--strategy ACS --acs-variant v2" ;;
            C)     ARGS="--strategy C --fixed-s $FIXED_S" ;;
            *)     ARGS="--strategy $S" ;;
          esac
          python live/run_trial.py $ARGS --workload $W --failure $F --sink $K --trial $T \
            --kill-span-s $KILL_SPAN_S --experiment live_acs \
            || echo "TRIAL FAILED: $S $W $F $K $T" | tee -a experiments/live_acs/failed_trials.txt
        done
      done
    done
  done
done
python live/analyze_acs.py
echo "done."
