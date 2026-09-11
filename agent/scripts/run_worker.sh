#!/usr/bin/env bash
# Supervised agent worker, under the audience's control.
#
# The room can kill this process for real — the watchdog calls os._exit(1), no
# graceful drain. What this loop does NOT do is bring it back regardless: it
# waits until the chaos feed says the component is back above the threshold,
# which means a fixer has to actually do something.
#
# That is the point of the tug-of-war. An earlier version restarted after a
# flat 2 seconds, which made fixers decorative and the kill cosmetic.
#
# Each restart is where workflow replay happens: the new process picks up the
# order mid-flight and reads every finished step out of event history instead
# of re-running it.
#
#   ./scripts/run_worker.sh

set -uo pipefail
cd "$(dirname "$0")/.."

export CHAOS_URL="${CHAOS_URL:-http://localhost:8931/chaos}"
export CHAOS_COMPONENT="${CHAOS_COMPONENT:-agent_worker}"
THRESHOLD="${CHAOS_THRESHOLD:-50}"
export CHAOS_THRESHOLD="$THRESHOLD"

# Rehearsal escape hatch, off by default. Set to a number of seconds to revive
# the worker even if nobody fixes it — useful when practising alone, wrong on
# stage, because it takes the decision away from the room.
FORCE_REVIVE_AFTER="${WORKER_FORCE_REVIVE_SECONDS:-0}"

if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
  echo "warning: ANTHROPIC_API_KEY is not set — llm_turn will fail with a 401" >&2
fi

# Reads the component's health from either the flat local shape or the board's
# {"state": {...}} shape, with booleans counting as 0/100.
current_health() {
  curl -s --max-time 3 "$CHAOS_URL" 2>/dev/null | python3 -c '
import json,sys
try: p = json.load(sys.stdin)
except Exception: sys.exit(1)
src = p.get("state") if isinstance(p.get("state"), dict) else p
v = src.get("'"$CHAOS_COMPONENT"'")
if isinstance(v, bool): print(100 if v else 0)
elif isinstance(v, (int, float)): print(int(round(v)))
else: sys.exit(1)
' 2>/dev/null
}

echo "supervising $CHAOS_COMPONENT · feed $CHAOS_URL · threshold ${THRESHOLD}%"
echo "the room kills it:  uv run python scripts/audience.py kill $CHAOS_COMPONENT"
echo "the room revives it: uv run python scripts/audience.py fix $CHAOS_COMPONENT"
[ "$FORCE_REVIVE_AFTER" != "0" ] && \
  echo "NOTE: force-revive after ${FORCE_REVIVE_AFTER}s is ON — rehearsal only"
echo

generation=1
while true; do
  # Wait for the room's verdict. An unreachable feed counts as "go" so a
  # missing chaos server can never stop you from running the worker.
  waited=0
  while true; do
    health="$(current_health)" || break
    [ -z "$health" ] && break
    [ "$health" -ge "$THRESHOLD" ] && break

    if [ "$waited" -eq 0 ]; then
      echo "--- ${CHAOS_COMPONENT} at ${health}% · waiting for a fixer to cross ${THRESHOLD}% ---"
    fi
    if [ "$FORCE_REVIVE_AFTER" != "0" ] && [ "$waited" -ge "$FORCE_REVIVE_AFTER" ]; then
      echo "--- force-revive after ${waited}s (rehearsal mode) ---"
      break
    fi
    sleep 1
    waited=$((waited + 1))
  done

  [ "$waited" -gt 0 ] && echo "--- revived after ${waited}s down ---"

  echo "--- worker generation ${generation} starting ---"
  uv run python -m richards_agent.worker
  code=$?

  if [ "$code" -eq 0 ]; then
    echo "--- worker exited cleanly, not restarting ---"
    exit 0
  fi

  echo "--- worker died (exit ${code}) ---"
  generation=$((generation + 1))
done
