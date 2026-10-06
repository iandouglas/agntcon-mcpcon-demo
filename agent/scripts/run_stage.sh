#!/usr/bin/env bash
# The stage screen, as a long-running service.
#
#   ./scripts/run_stage.sh
#
# Two things here are not uvicorn's defaults and both matter on a platform:
#
#   --host 0.0.0.0  uvicorn binds 127.0.0.1 otherwise, which behind Railway's
#                   proxy produces a green deploy, healthy-looking logs, and a
#                   502 on every single request.
#   --port $PORT    Railway assigns the port; 8900 locally to match the README.
set -euo pipefail
cd "$(dirname "$0")/.."

exec uv run uvicorn stage.server:app --host 0.0.0.0 --port "${PORT:-8900}"
