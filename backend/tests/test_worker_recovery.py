"""Phase 2 exit criterion: kill -9 a worker mid-branch; another worker
picks the branch up from its last checkpoint; no duplicate iterations in
``evolution_summary.jsonl`` (I1), reclaim carries a new fence (I5), and
the branch converges (I3).

Postgres-gated end-to-end test: real worker subprocesses, real SIGKILL.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.meta_harness.outer import run_outer_loop  # noqa: E402
from app.meta_harness.persistence import get_dsn, persistence_layer  # noqa: E402
from app.meta_harness.runs import make_run_dir  # noqa: E402
from app.meta_harness.store import PostgresStateStore  # noqa: E402

pytestmark = pytest.mark.usefixtures("require_postgres")

BACKEND_DIR = Path(__file__).resolve().parents[1]
BRANCH_BUDGET = 30  # extra iterations the fork runs — enough runway to kill mid-flight


def _spawn_worker(runs_root: Path, worker_id: str, *extra: str) -> subprocess.Popen:
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "app.worker",
            "--worker-id",
            worker_id,
            "--runs-root",
            str(runs_root),
            "--repo-root",
            str(REPO_ROOT),
            "--lease-ttl",
            "1.0",
            "--poll-interval",
            "0.1",
            *extra,
        ],
        cwd=BACKEND_DIR,
        env={**os.environ},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def _summary_rows(run_dir: Path) -> list[dict]:
    path = run_dir / "evolution_summary.jsonl"
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


async def test_i1_i3_i5_kill9_worker_midbranch_another_worker_recovers(tmp_path: Path):
    run_name = f"wkrec-{uuid.uuid4().hex[:8]}"
    run_dir = make_run_dir(tmp_path, run_name, fresh=True)
    eval_tasks_dir = REPO_ROOT / "eval" / "tasks"
    store = await PostgresStateStore.connect(get_dsn())
    await store.setup()
    worker_a = worker_b = None
    try:
        # ── 1. Parent run: one mock iteration, checkpointed in Postgres.
        async with persistence_layer() as saver:
            from app.meta_harness.outer import build_runner_from_manifest
            from app.meta_harness.branches import get_state_history

            await run_outer_loop(
                run_dir=run_dir,
                repo_root=REPO_ROOT,
                eval_tasks_dir=eval_tasks_dir,
                mock_proposer=True,
                mock_bench=True,
                trials=5,
                bench_workers=1,
                budget=1,
                checkpointer=saver,
            )
            runner = build_runner_from_manifest(
                run_dir=run_dir,
                repo_root=REPO_ROOT,
                eval_tasks_dir=eval_tasks_dir,
                checkpointer=saver,
            )
            graph = runner.build()
            history = await get_state_history(graph, thread_id=run_name)
            parent_checkpoint_id = history[0].checkpoint_id  # latest

        parent_rows = _summary_rows(run_dir)
        assert len(parent_rows) == 1

        # ── 2. Durable fork: branch row with a fresh iteration budget.
        branch_id = uuid.uuid4().hex[:8]
        row = await store.create_branch(
            branch_id=branch_id,
            run_id=run_name,
            thread_id=f"{run_name}.fork.{branch_id}",
            parent_thread_id=run_name,
            parent_checkpoint_id=parent_checkpoint_id,
            mods={"budget_remaining": BRANCH_BUDGET},
            name="recovery-test branch",
        )

        # ── 3. Worker A starts executing; kill -9 mid-branch.
        worker_a = _spawn_worker(tmp_path / "runs", "worker-a")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            rows = _summary_rows(run_dir)
            if len(rows) >= len(parent_rows) + 3:
                break
            if worker_a.poll() is not None:
                out = worker_a.stdout.read().decode()
                raise AssertionError(f"worker A exited early:\n{out}")
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("worker A never made mid-branch progress")

        os.kill(worker_a.pid, signal.SIGKILL)
        worker_a.wait(timeout=10)

        mid_row = await store.get_branch(branch_id)
        assert mid_row.status == "running"  # orphaned, lease will expire
        assert mid_row.lease_generation == 1

        # ── 4. Worker B claims the orphan after lease expiry and finishes.
        worker_b = _spawn_worker(tmp_path / "runs", "worker-b", "--max-branches", "1")
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            final_row = await store.get_branch(branch_id)
            if final_row.status in {"completed", "failed", "cancelled"}:
                break
            await asyncio.sleep(0.2)
        else:
            raise AssertionError("worker B never finished the branch")

        # ── 5. Invariants.
        assert final_row.status == "completed", final_row.error
        # I5: the reclaim carried a NEW fence.
        assert final_row.lease_generation == 2
        # I3: the branch converged — full budget spent.
        assert final_row.result["budget_remaining"] == 0
        assert final_row.result["iteration"] == 1 + BRANCH_BUDGET
        # I1: no (iteration, candidate) row landed twice across the
        # crash/resume sequence.
        rows = _summary_rows(run_dir)
        keys = [(r["iteration"], r["candidate"]) for r in rows]
        assert len(keys) == len(set(keys)), f"duplicate iterations: {keys}"
        assert len(rows) == 1 + BRANCH_BUDGET
        # I7: worker-emitted durable events are gap-free and monotonic.
        events = await store.list_events(run_id=run_name)
        assert [e.seq for e in events] == list(range(1, len(events) + 1))
        assert len(events) > 0
    finally:
        for proc in (worker_a, worker_b):
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)
        await store._conn.execute(
            "DELETE FROM branch_runs WHERE run_id = %s;", (run_name,)
        )
        await store._conn.execute(
            "DELETE FROM run_events WHERE run_id = %s;", (run_name,)
        )
        await store._conn.execute(
            "DELETE FROM run_event_seq WHERE run_id = %s;", (run_name,)
        )
        await store._conn.execute(
            "DELETE FROM workers WHERE worker_id IN ('worker-a', 'worker-b');"
        )
        await store.close()
        for stub in (REPO_ROOT / "agents").glob("_mock_iter_*.py"):
            stub.unlink()
