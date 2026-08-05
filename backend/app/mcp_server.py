"""MCP server — durable execution as a consumable service (Phase 6).

Thin stdio adapter over the existing FastAPI REST surface
(``documents/MCP_SERVER_SPEC.md``). Six tools, no business logic: every
tool maps 1:1 to a REST call, so the branch lifecycle has exactly one
implementation and I5 cannot diverge between paths.

Run it::

    uv run meta-harness-mcp                     # stdio
    META_HARNESS_API_URL=http://localhost:8000  # target API (default)

Register with Claude Code::

    claude mcp add meta-harness -- uv --directory backend run meta-harness-mcp
"""

from __future__ import annotations

import os
from typing import Any

import httpx
from mcp.server.mcpserver import MCPServer

API_URL = os.environ.get("META_HARNESS_API_URL", "http://localhost:8000")

# Registered workloads: what `start_run` accepts. The reference workload
# is the meta-harness search; `mock-loop` is its deterministic fixture
# variant (scores follow a hardcoded curve — never measurements).
WORKLOADS: dict[str, dict[str, Any]] = {
    "mock-loop": {"proposer": "mock", "mock_bench": True},
    "harness-search": {"proposer": "claude", "mock_bench": False},
}

mcp = MCPServer(
    name="meta-harness-runtime",
    description=(
        "Durable execution runtime for long-horizon agent workflows: "
        "checkpointed, forkable, crash-recoverable runs that survive "
        "client disconnects and worker kills."
    ),
)


class RuntimeAPIError(RuntimeError):
    """REST call failed; message carries the API's detail verbatim."""


async def _request(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0) as client:
        response = await client.request(method, path, **kwargs)
    if response.status_code >= 400:
        try:
            detail = response.json().get("detail", response.text)
        except Exception:  # noqa: BLE001 — non-JSON error body
            detail = response.text
        raise RuntimeAPIError(f"{method} {path} → {response.status_code}: {detail}")
    return response.json()


@mcp.tool()
async def start_run(workload: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Start a durable, checkpointed run. Returns immediately with a
    run_id; the run continues on the server and SURVIVES CLIENT
    DISCONNECT — you can drop this connection and pick the run back up
    later by id. Poll get_branch_status / the run endpoints for
    progress.

    workload: one of "mock-loop" (deterministic fixture workload, no
    LLM calls, scores are hardcoded fixtures) or "harness-search" (the
    real proposer; requires credentials on the server).
    config: optional overrides — run_name, budget (iterations),
    trials, workers.
    """
    if workload not in WORKLOADS:
        raise RuntimeAPIError(
            f"unknown workload {workload!r}; registered: {sorted(WORKLOADS)}"
        )
    payload: dict[str, Any] = {**WORKLOADS[workload], **(config or {})}
    result = await _request("POST", "/runs", json=payload)
    return {
        "run_id": result.get("run_id"),
        "thread_id": result.get("thread_id"),
        "status": result.get("status"),
    }


@mcp.tool()
async def fork_from_checkpoint(
    run_id: str,
    checkpoint_id: str,
    mods: dict[str, Any] | None = None,
    name: str | None = None,
) -> dict[str, Any]:
    """Fork a new branch from ANY prior checkpoint of an existing run,
    optionally mutating state at the fork point (e.g.
    {"budget_remaining": 20}). The parent run is unaffected. Returns a
    branch_id immediately; execution happens on server-side workers and
    SURVIVES CLIENT DISCONNECT and even worker crashes — a killed
    worker's branch is reclaimed by another worker (watch
    lease_generation increment in get_branch_status) and resumes from
    its last checkpoint.

    List a run's checkpoints via GET {api}/runs/{run_id}/checkpoints or
    by polling run status while it executes.
    """
    body: dict[str, Any] = {"parent_checkpoint_id": checkpoint_id, "mods": mods or {}}
    if name is not None:
        body["name"] = name
    return await _request("POST", f"/runs/{run_id}/branches", json=body)


@mcp.tool()
async def get_branch_status(branch_id: str) -> dict[str, Any]:
    """Poll one branch. Everything long-running resolves through this.

    Key field: lease_generation — the fencing token. If you cached it
    and it has incremented, your branch's worker died (or stalled past
    its lease) and ANOTHER worker reclaimed the branch and resumed it
    from the last checkpoint. status transitions:
    created → running → completed | failed | cancelled.
    """
    return await _request("GET", f"/branches/{branch_id}")


@mcp.tool()
async def list_branches(run_id: str) -> dict[str, Any]:
    """List all branches of a run as a lineage tree: each node carries
    status, lease_generation, lease_owner, lease_expires_at,
    parent_thread_id, parent_checkpoint_id and created_at, so the tree
    reconstructs without further calls.
    """
    return await _request("GET", f"/runs/{run_id}/branches")


@mcp.tool()
async def cancel_branch(branch_id: str) -> dict[str, Any]:
    """Durably cancel a branch (invariant I6): the cancellation is
    written to the store BEFORE any running task is signalled, so a
    cancelled branch never resumes — not after worker crashes, not
    after server restarts. Cancelling bumps the fencing token, so a
    live worker's next write aborts.
    """
    return await _request("DELETE", f"/branches/{branch_id}")


@mcp.tool()
async def resume_run(run_id: str) -> dict[str, Any]:
    """Resume a run from its last durable checkpoint (e.g. after a
    server restart or client disconnect — runs are never lost with the
    client). Idempotent: calling this on an already-running run is a
    no-op that returns current status ("resumed": false), never a
    second execution.
    """
    return await _request("POST", f"/runs/{run_id}/resume")


def main() -> None:
    mcp.run("stdio")


if __name__ == "__main__":
    main()
