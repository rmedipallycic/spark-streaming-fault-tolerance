#!/bin/bash
# One-time setup inside WSL2 Ubuntu (Docker Desktop with the WSL 2 engine must be running).
#   bash live/setup_wsl.sh
set -euo pipefail
cd "$(dirname "$0")/.."
sudo apt-get update -qq
sudo apt-get install -y -qq openjdk-17-jre-headless curl
if ! command -v uv >/dev/null; then curl -LsSf https://astral.sh/uv/install.sh | sh; fi
export PATH="$HOME/.local/bin:$PATH"
uv venv --seed --python 3.11 "$HOME/venv-live"
"$HOME/venv-live/bin/pip" install -q pyspark==3.5.3 confluent-kafka==2.3.0
docker compose up -d zookeeper kafka
echo
echo "Setup done. In each new terminal:  source ~/venv-live/bin/activate"
echo "Check Kafka is healthy:            docker compose ps"
