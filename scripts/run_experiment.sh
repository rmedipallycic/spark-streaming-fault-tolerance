#!/bin/bash
# ============================================================
# run_experiment.sh — One-command fault tolerance experiment launcher
# Usage:
#   ./run_experiment.sh --framework spark --failure worker --checkpoint A
#   ./run_experiment.sh --framework flink --failure driver --checkpoint F1
#   ./run_experiment.sh --all   (runs all 24 configurations x 30 trials)
# ============================================================
set -e

FRAMEWORK="spark"
FAILURE="baseline"
CHECKPOINT="A"
TRIALS=30
RUN_ALL=false
OUTPUT_DIR="experiments/results/$(date +%Y%m%d_%H%M%S)"

while [[ $# -gt 0 ]]; do
  case $1 in
    --framework)  FRAMEWORK="$2";   shift 2 ;;
    --failure)    FAILURE="$2";     shift 2 ;;
    --checkpoint) CHECKPOINT="$2";  shift 2 ;;
    --trials)     TRIALS="$2";      shift 2 ;;
    --all)        RUN_ALL=true;     shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

mkdir -p "$OUTPUT_DIR"
log() { echo "[$(date +%H:%M:%S)] $*"; }

inject_failure() {
  local failure=$1
  log "Injecting failure: $failure"
  case $failure in
    worker)
      docker kill spark-worker-1 2>/dev/null || docker kill flink-taskmanager-1 2>/dev/null || true
      sleep 2 ;;
    driver)
      docker kill spark-driver 2>/dev/null || docker kill flink-jobmanager 2>/dev/null || true
      sleep 2 ;;
    checkpoint)
      local ckpt_dir="checkpoints/$(ls checkpoints/ 2>/dev/null | head -1)"
      if [ -d "$ckpt_dir" ]; then
        dd if=/dev/urandom of="$ckpt_dir/metadata" bs=1024 count=4 2>/dev/null || true
        log "Corrupted checkpoint metadata at $ckpt_dir"
      fi ;;
    network)
      docker exec spark-worker-1 tc qdisc add dev eth0 root netem loss 100% 2>/dev/null || true
      sleep 5
      docker exec spark-worker-1 tc qdisc del dev eth0 root 2>/dev/null || true ;;
    baseline) log "No failure (baseline run)" ;;
  esac
}

run_single() {
  local framework=$1 failure=$2 checkpoint=$3 trial=$4
  local result_file="$OUTPUT_DIR/${framework}_${failure}_${checkpoint}_trial${trial}.json"
  log "Trial $trial/$TRIALS | framework=$framework | failure=$failure | checkpoint=$checkpoint"
  if [ "$framework" = "spark" ]; then
    python src/pipeline.py --strategy "$checkpoint" --scenario "$failure" \
      --trial "$trial" --output "$result_file" &
  else
    python src/flink_pipeline.py --strategy "$checkpoint" --scenario "$failure" \
      --trial "$trial" --output "$result_file" &
  fi
  PIPELINE_PID=$!
  sleep 3
  if [ "$failure" != "baseline" ]; then inject_failure "$failure"; fi
  wait $PIPELINE_PID || true
  log "Trial $trial complete — $result_file"
}

run_all_configs() {
  local failures=("baseline" "worker" "driver" "checkpoint")
  local spark_ckpts=("A" "B" "C")
  local flink_ckpts=("F1" "F2" "F3")
  for fail in "${failures[@]}"; do
    for ckpt in "${spark_ckpts[@]}"; do
      for trial in $(seq 1 $TRIALS); do run_single "spark" "$fail" "$ckpt" "$trial"; done
    done
    for ckpt in "${flink_ckpts[@]}"; do
      for trial in $(seq 1 $TRIALS); do run_single "flink" "$fail" "$ckpt" "$trial"; done
    done
  done
}

log "=== Fault Tolerance Experiment Runner ==="
log "Output: $OUTPUT_DIR"

if ! docker info > /dev/null 2>&1; then
  log "ERROR: Docker not running. Start Docker and retry."; exit 1
fi
if ! docker compose ps 2>/dev/null | grep -q "kafka"; then
  log "Starting Kafka..."; docker compose up -d kafka zookeeper; sleep 10
fi

if $RUN_ALL; then
  log "Running ALL configurations x $TRIALS trials"
  run_all_configs
else
  for trial in $(seq 1 $TRIALS); do run_single "$FRAMEWORK" "$FAILURE" "$CHECKPOINT" "$trial"; done
fi

log "Aggregating results..."
python experiments/run_simulation.py --input "$OUTPUT_DIR" --output "$OUTPUT_DIR/summary.csv"
log "=== Done. Results at $OUTPUT_DIR/summary.csv ==="
