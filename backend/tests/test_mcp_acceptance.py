"""MCP acceptance scenario (MCP_SERVER_SPEC.md §6).

An MCP client with no knowledge of this repo:
1. calls `start_run`,
2. picks a checkpoint,
3. calls `fork_from_checkpoint` with state mods,
4. — we kill -9 the worker executing that branch —
5. its next `get_branch_status` poll shows the branch running again
   with an INCREMENTED lease_generation, resumed from checkpoint,
6. `evolution_summary.jsonl` has no duplicate iterations (I1).

Everything flows client → stdio MCP adapter → REST API → StateStore →
workers, across four real processes.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mcp import ClientSession, StdioServerParameters  # noqa: E402
from mcp.client.stdio import stdio_client  # noqa: E402

from app.meta_harness.persistence import get_dsn  # noqa: E402
from app.meta_harness.store import PostgresStateStore  # noqa: E402

pytestmark = pytest.mark.usefixtures("require_postgres")

BACKEND_DIR = Path(__file__).resolve().parents[1]
API_PORT = 8123
API_URL = f"http://127.0.0.1:{API_PORT}"


def _spawn(args: list[str], log: Path) -> subprocess.Popen:
    return subprocess.Popen(
        args,
        cwd=BACKEND_DIR,
        env={**os.environ},
        stdout=log.open("w"),
        stderr=subprocess.STDOUT,
    )


def _tool_payload(result) -> dict:
    assert not result.is_error, f"tool error: {result.content}"
    structured = getattr(result, "structured_content", None)
    if structured is not None:
        # The SDK wraps bare returns as {"result": ...}; a payload whose
        # own schema has a `result` field arrives unwrapped.
        if isinstance(structured, dict) and set(structured) == {"result"}:
            return structured["result"]
        return structured
    return json.loads(result.content[0].text)


async def test_mcp_acceptance_kill9_recovery_via_outside_client(tmp_path: Path):
    suffix = uuid.uuid4().hex[:8]
    run_name = f"mcp-accept-{suffix}"
    w1_id, w2_id = f"mcp-w1-{suffix}", f"mcp-w2-{suffix}"

    api = _spawn(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(API_PORT)],
        tmp_path / "api.log",
    )
    workers = {
        w1_id: _spawn(
            [sys.executable, "-m", "app.worker", "--worker-id", w1_id,
             "--lease-ttl", "2", "--poll-interval", "0.2",
             "--repo-root", str(REPO_ROOT)],
            tmp_path / "w1.log",
        ),
        w2_id: _spawn(
            [sys.executable, "-m", "app.worker", "--worker-id", w2_id,
             "--lease-ttl", "2", "--poll-interval", "0.2",
             "--repo-root", str(REPO_ROOT)],
            tmp_path / "w2.log",
        ),
    }
    store = await PostgresStateStore.connect(get_dsn())
    try:
        async with httpx.AsyncClient(base_url=API_URL, timeout=5.0) as http:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                try:
                    if (await http.get("/health")).status_code == 200:
                        break
                except httpx.TransportError:
                    await asyncio.sleep(0.3)
            else:
                raise AssertionError(f"API never came up:\n{(tmp_path / 'api.log').read_text()[-2000:]}")

        server = StdioServerParameters(
            command=sys.executable,
            args=["-m", "app.mcp_server"],
            env={**os.environ, "META_HARNESS_API_URL": API_URL},
            cwd=str(BACKEND_DIR),
        )
        async with stdio_client(server) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                # 1. start a durable run
                started = _tool_payload(
                    await session.call_tool(
                        "start_run",
                        {"workload": "mock-loop", "config": {"run_name": run_name, "budget": 2}},
                    )
                )
                assert started["run_id"] == run_name

                # 2. wait for checkpoints, pick a mid-point
                async with httpx.AsyncClient(base_url=API_URL, timeout=10.0) as http:
                    deadline = time.monotonic() + 60
                    checkpoint_id = None
                    while time.monotonic() < deadline:
                        detail = (await http.get(f"/runs/{run_name}")).json()
                        if detail.get("status") == "completed":
                            rows = (await http.get(f"/runs/{run_name}/checkpoints")).json()["checkpoints"]
                            checkpoint_id = rows[0]["checkpoint_id"]
                            break
                        await asyncio.sleep(0.3)
                    assert checkpoint_id, "parent run never completed"

                # 3. fork from it, mutating state so the branch lives ~10s
                forked = _tool_payload(
                    await session.call_tool(
                        "fork_from_checkpoint",
                        {
                            "run_id": run_name,
                            "checkpoint_id": checkpoint_id,
                            "mods": {"budget_remaining": 15, "demo_delay_s": 0.5},
                            "name": "acceptance branch",
                        },
                    )
                )
                branch_id = forked["branch_id"]

                # wait until a worker owns it, cache the fence
                deadline = time.monotonic() + 30
                status = {}
                while time.monotonic() < deadline:
                    status = _tool_payload(
                        await session.call_tool("get_branch_status", {"branch_id": branch_id})
                    )
                    if status["status"] == "running" and status["lease_owner"]:
                        break
                    await asyncio.sleep(0.3)
                assert status.get("status") == "running", status
                cached_fence = status["lease_generation"]
                assert cached_fence == 1
                owner = status["lease_owner"]
                assert owner in workers

                # 4. kill -9 the worker executing the branch
                os.kill(workers[owner].pid, signal.SIGKILL)
                workers[owner].wait(timeout=10)

                # 5. next polls: branch reclaimed with an incremented fence
                deadline = time.monotonic() + 60
                reclaimed = {}
                while time.monotonic() < deadline:
                    reclaimed = _tool_payload(
                        await session.call_tool("get_branch_status", {"branch_id": branch_id})
                    )
                    if reclaimed["lease_generation"] > cached_fence:
                        break
                    await asyncio.sleep(0.3)
                assert reclaimed["lease_generation"] == cached_fence + 1, reclaimed
                assert reclaimed["lease_owner"] != owner

                # ... and converges
                deadline = time.monotonic() + 120
                final = reclaimed
                while time.monotonic() < deadline:
                    if final["status"] in {"completed", "failed", "cancelled"}:
                        break
                    await asyncio.sleep(0.5)
                    final = _tool_payload(
                        await session.call_tool("get_branch_status", {"branch_id": branch_id})
                    )
                assert final["status"] == "completed", final
                assert final["result"]["budget_remaining"] == 0

                # list_branches reconstructs the lineage without more calls
                tree = _tool_payload(
                    await session.call_tool("list_branches", {"run_id": run_name})
                )
                node = next(b for b in tree["branches"] if b["branch_id"] == branch_id)
                assert node["parent_checkpoint_id"] == checkpoint_id
                assert node["lease_generation"] == 2

                # resume_run is idempotent on a finished run (I1)
                resumed = _tool_payload(
                    await session.call_tool("resume_run", {"run_id": run_name})
                )
                assert resumed["run_id"] == run_name

        # 6. I1 on the artifact
        summary = REPO_ROOT / "runs" / run_name / "evolution_summary.jsonl"
        rows = [json.loads(l) for l in summary.read_text().splitlines() if l.strip()]
        keys = [(r["iteration"], r["candidate"]) for r in rows]
        assert len(keys) == len(set(keys)), f"duplicate iterations: {keys}"
    finally:
        for proc in [api, *workers.values()]:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)
        for table, col in [
            ("branch_runs", "run_id"),
            ("run_events", "run_id"),
            ("run_event_seq", "run_id"),
            ("iteration_log", "run_id"),
        ]:
            await store._conn.execute(
                f"DELETE FROM {table} WHERE {col} = %s;", (run_name,)
            )
        await store._conn.execute(
            "DELETE FROM workers WHERE worker_id IN (%s, %s);", (w1_id, w2_id)
        )
        await store.close()
        shutil.rmtree(REPO_ROOT / "runs" / run_name, ignore_errors=True)
        for stub in (REPO_ROOT / "agents").glob("_mock_iter_*.py"):
            stub.unlink()
