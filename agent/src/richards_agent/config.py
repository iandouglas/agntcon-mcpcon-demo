"""Environment configuration. Nothing here is read from inside a workflow."""

from __future__ import annotations

import os

# --- Temporal -------------------------------------------------------------
TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")
TEMPORAL_API_KEY = os.getenv("TEMPORAL_API_KEY")  # set for Temporal Cloud

# --- Richard's MCP server -------------------------------------------------
# Ian's deployed server. Override to http://localhost:8931/mcp to use the
# local fake instead.
MCP_ENDPOINT = os.getenv(
    "MCP_ENDPOINT", "https://mcp-production-00c3d.up.railway.app/mcp"
)
# Required by the real server. Both tokens get rotated after the talk.
MCP_AUTH_TOKEN = os.getenv("MCP_AUTH_TOKEN", "")

# Above Railway's 15-minute hard request cap, which sits above the MCP server's
# own 840s wait cap. A broken service makes a call slow, not failed, so this
# has to be generous or we'd time out and re-issue work that is still running.
MCP_CALL_TIMEOUT_SECONDS = float(os.getenv("MCP_CALL_TIMEOUT_SECONDS", "960"))

# --- Model ----------------------------------------------------------------
# Opus 5 by default. For the stage run, latency matters more than depth —
# measure a full order end to end before deciding whether to switch this to
# claude-haiku-4-5. That's a call to make with a stopwatch, not up front.
MODEL = os.getenv("AGENT_MODEL", "claude-opus-5")

# Tool-calling turns are short, but thinking is on by default on Opus 5 and
# spends output tokens too — so this has to leave room for thinking AND the
# tool_use block, or a turn truncates mid-call and the agent looks like it
# finished when it didn't.
MAX_TOKENS = int(os.getenv("AGENT_MAX_TOKENS", "4096"))

# Simple, well-specified tool loop — low effort keeps turns fast.
# "off" omits the parameter entirely, which is also what happens automatically
# on a model that does not accept it (see EFFORT_UNSUPPORTED).
EFFORT = os.getenv("AGENT_EFFORT", "low")

# Models that reject `output_config.effort` with a 400 rather than ignoring it.
# Haiku 4.5 and Sonnet 4.5 predate the parameter; the Opus 5 and Sonnet 5
# generation all accept it.
#
# This is a list and not an AGENT_EFFORT=off instruction in DEPLOY.md because
# switching the model is one variable and nobody remembers that it is secretly
# two. Getting it wrong fails every single order with a 400, which is a bad
# thing to discover on stage.
EFFORT_UNSUPPORTED = ("claude-haiku-4-5", "claude-sonnet-4-5")


def effort_for(model: str) -> str | None:
    """The effort to send for `model`, or None to omit the parameter."""
    if EFFORT.lower() in ("off", "none", ""):
        return None
    if model.startswith(EFFORT_UNSUPPORTED):
        return None
    return EFFORT

# "off" disables thinking entirely. Worth A/B-ing with a stopwatch before the
# talk: thinking makes the out-of-stock re-plan more reliable, but it is also
# the single biggest contributor to dead air on stage.
THINKING = os.getenv("AGENT_THINKING", "on")

SYSTEM_PROMPT = """You are the ordering agent for Richard's Taquería.

You take a customer's order and see it all the way through using the tools \
available to you. Discover what you can do from the tool list — do not assume \
tools exist that aren't listed.

How to work:
- Call tools one step at a time and read each result before deciding the next step.
- A tool result may report a business problem (out of stock, payment declined, \
bad address). That is information, not a failure. Handle it: fix the cause if a \
tool lets you, then continue the order.
- Never call the same tool twice with identical arguments hoping for a different \
answer. If a result says something is wrong, change something.
- Payment and receipt steps are irreversible. Call each exactly once.

When the order is complete, reply with one short sentence for the customer \
confirming what was ordered and what it cost. No preamble."""
