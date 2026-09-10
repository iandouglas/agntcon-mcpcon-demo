#!/usr/bin/env bash
# Supervised agent worker.
#
# The audience gets to kill this process for real — the watchdog calls
# os._exit(1), no graceful drain. This loop is the supervisor that brings it
# back, standing in for a docker restart policy or a k8s deployment.
#
# Each restart is where workflow replay happens: the new process picks up the
# order mid-flight and reads every finished step out of history instead of
# re-running it. That is the whole demo.
#
#   ./scripts/run_worker.sh

set -uo pipefail
cd "$(dirname "$0")/.."

export CHAOS_URL="${CHAOS_URL:-http://localhost:8931/chaos}"
export CHAOS_COMPONENT="${CHAOS_COMPONENT:-agent_worker}"

if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
  echo "warning: ANTHROPIC_API_KEY is not set — llm_turn will fail with a 401" >&2
fi

echo "supervising agent worker · chaos feed ${CHAOS_URL}"
echo "kill it from the audience side:  uv run python scripts/audience.py kill agent_worker"
echo

generation=1
while true; do
  echo "--- worker generation ${generation} starting ---"
  uv run python -m richards_agent.worker
  code=$?

  if [ "$code" -eq 0 ]; then
    echo "--- worker exited cleanly, not restarting ---"
    exit 0
  fi

  echo "--- worker died (exit ${code}) · restarting in 2s ---"
  sleep 2
  generation=$((generation + 1))
done
