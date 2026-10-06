# Single image, two Railway services. The only difference is the start command:
#   agent-worker  -> scripts/run_worker.sh   (no domain, outbound only)
#   stage-screen  -> scripts/run_stage.sh    (public domain, the projector view)
#
# Mirrors the MCP server's image on purpose, so there is one shape to debug at
# 9am rather than two.
FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv

# curl, because run_worker.sh polls the chaos feed with it. The supervisor
# treats an unreadable feed as "go", so a missing curl would not crash the
# worker -- it would quietly make the audience unable to keep it down, which
# is a far worse way to find out.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_FROZEN=1

WORKDIR /app

# Dependencies first, so a source-only change does not reinstall the world.
# --frozen against the committed lockfile: a conference demo should never
# resolve a different dependency tree than the one it was rehearsed with.
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --no-install-project

COPY src ./src
COPY scripts ./scripts
RUN uv sync --no-dev && chmod +x scripts/*.sh

# Overridden per service. Defaults to the worker because that is the one that
# has to be running for anything else to be worth looking at.
CMD ["./scripts/run_worker.sh"]
