"""Durable branch worker (REPOSITIONING_PLAN Phase 2).

A worker is a separate process from the API. It claims `branch_runs`
rows through the :class:`StateStore` claiming query (lease + fencing
token), executes each branch as a LangGraph fork/resume against the
shared ``AsyncPostgresSaver``, heartbeats its lease while work is in
flight, and finishes with fence-guarded writes.

Crash semantics (the whole point):

- kill -9 mid-branch → the row stays ``running`` with a lease that
  expires. Any worker — including a freshly booted one running
  ``reconcile_on_boot`` — reclaims it with an incremented fence and
  resumes from the branch thread's last checkpoint (I1, I2, I3).
- A stalled worker that wakes after reclaim gets ``StaleFenceError`` on
  its next heartbeat or finish write and aborts without retry (I5).
- A cancelled branch's fence is bumped by ``request_cancel``; the live
  worker aborts the same way, and reconciliation never revives a
  terminal row (I6).

Run one with::

    uv run meta-harness worker            # or:
    uv run python -m app.worker
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import copy
import os
import socket
from pathlib import Path
from typing import Any

from app import streaming
from app.meta_harness import branches as br
from app.meta_harness.outer import build_runner_from_manifest
from app.meta_harness.persistence import get_dsn, persistence_layer
from app.meta_harness.store import (
    BranchRow,
    PostgresStateStore,
    StaleFenceError,
    StateStore,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


async def prepare_branch_config(graph: Any, row: BranchRow) -> dict[str, Any]:
    """Return the config to ``ainvoke(None, …)`` for a claimed branch.

    First execution forks the parent checkpoint into the branch thread
    (``aupdate_state`` with the parent values + mods). A reclaimed branch
    already has checkpoints on its own thread — resume from the latest
    one instead of re-forking (re-forking would rewind completed work and
    violate I1).
    """
    thread_config = {"configurable": {"thread_id": row.thread_id}}
    existing = [
        s async for s in graph.aget_state_history(thread_config, limit=1)
    ]
    if existing:
        return thread_config

    parent_snapshot = await br._find_snapshot(
        graph,
        thread_id=row.parent_thread_id,
        checkpoint_id=row.parent_checkpoint_id,
    )
    as_node = await br._infer_as_node_for_fork(graph, parent_snapshot)
    fork_values = copy.deepcopy(br._snapshot_values(parent_snapshot))
    fork_values.update(row.mods)
    return await graph.aupdate_state(thread_config, fork_values, as_node=as_node)


async def execute_claimed_branch(
    store: StateStore,
    graph: Any,
    row: BranchRow,
    *,
    lease_ttl_s: float,
    recursion_limit: int = 200,
) -> None:
    """Execute one claimed branch under lease heartbeat + fence guards."""
    fence = row.lease_generation

    async def _heartbeat_forever() -> None:
        interval = max(lease_ttl_s / 3.0, 0.05)
        while True:
            await asyncio.sleep(interval)
            await store.heartbeat(
                branch_id=row.branch_id, fence=fence, lease_ttl_s=lease_ttl_s
            )

    async def _run() -> dict[str, Any]:
        config = await prepare_branch_config(graph, row)
        final = await graph.ainvoke(
            None, config={**config, "recursion_limit": recursion_limit}
        )
        return final if isinstance(final, dict) else {"result": final}

    exec_task = asyncio.create_task(_run(), name=f"branch:{row.thread_id}")
    hb_task = asyncio.create_task(
        _heartbeat_forever(), name=f"heartbeat:{row.branch_id}"
    )
    try:
        done, _pending = await asyncio.wait(
            {exec_task, hb_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if hb_task in done:
            # A heartbeat only *finishes* by raising — stale fence or a
            # store error. Either way this worker no longer provably owns
            # the branch: kill the work and abort without writing.
            exec_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await exec_task
            hb_task.result()  # re-raises
            return

        hb_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await hb_task
        try:
            result = exec_task.result()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — workload failure is data
            await store.finish_branch(
                branch_id=row.branch_id,
                fence=fence,
                status="failed",
                error=str(exc),
            )
            return
        summary = {
            "iteration": result.get("iteration"),
            "best_candidate": result.get("best_candidate"),
            "budget_remaining": result.get("budget_remaining"),
        }
        await store.finish_branch(
            branch_id=row.branch_id,
            fence=fence,
            status="completed",
            result=summary,
        )
    except StaleFenceError:
        # Reclaimed or cancelled out from under us. The new owner (if
        # any) resumes from the branch thread's checkpoints; our job is
        # only to stop.
        return
    finally:
        for task in (exec_task, hb_task):
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task


async def run_worker(
    *,
    worker_id: str | None = None,
    lease_ttl_s: float = 15.0,
    poll_interval_s: float = 0.5,
    repo_root: Path | None = None,
    runs_root: Path | None = None,
    max_branches: int | None = None,
    exit_when_idle: bool = False,
    store: StateStore | None = None,
) -> int:
    """Claim-and-execute loop. Returns the number of branches processed.

    ``max_branches`` / ``exit_when_idle`` exist for tests and batch use;
    the default loop runs forever.
    """
    worker_id = worker_id or default_worker_id()
    repo_root = (repo_root or REPO_ROOT).resolve()
    runs_root = (runs_root or repo_root / "runs").resolve()
    eval_tasks_dir = repo_root / "eval" / "tasks"

    owns_store = store is None
    if store is None:
        store = await PostgresStateStore.connect(get_dsn())
        await store.setup()

    writer = streaming.DurableEventWriter(store)
    writer.start()
    streaming.set_durable_sink(writer.emit)

    processed = 0
    try:
        async with persistence_layer() as saver:
            requeued = await store.reconcile_on_boot()
            for row in requeued:
                print(
                    f"[worker {worker_id}] reconciled orphan branch "
                    f"{row.branch_id} (gen {row.lease_generation}) → requeued",
                    flush=True,
                )

            graphs: dict[str, Any] = {}
            while max_branches is None or processed < max_branches:
                row = await store.claim_next_branch(
                    worker_id=worker_id, lease_ttl_s=lease_ttl_s
                )
                if row is None:
                    if exit_when_idle:
                        break
                    await asyncio.sleep(poll_interval_s)
                    continue

                print(
                    f"[worker {worker_id}] claimed {row.branch_id} "
                    f"(run {row.run_id}, fence {row.lease_generation})",
                    flush=True,
                )
                if row.run_id not in graphs:
                    runner = build_runner_from_manifest(
                        run_dir=runs_root / row.run_id,
                        repo_root=repo_root,
                        eval_tasks_dir=eval_tasks_dir,
                        checkpointer=saver,
                    )
                    graphs[row.run_id] = runner.build()
                await execute_claimed_branch(
                    store, graphs[row.run_id], row, lease_ttl_s=lease_ttl_s
                )
                processed += 1
                final = await store.get_branch(row.branch_id)
                print(
                    f"[worker {worker_id}] branch {row.branch_id} → "
                    f"{final.status if final else 'unknown'}",
                    flush=True,
                )
    finally:
        streaming.set_durable_sink(None)
        with contextlib.suppress(Exception):
            await writer.aclose()
        if owns_store:
            with contextlib.suppress(Exception):
                await store.close()
    return processed


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Meta-Harness branch worker")
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--lease-ttl", type=float, default=15.0)
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--repo-root", type=Path, default=None)
    parser.add_argument("--runs-root", type=Path, default=None)
    parser.add_argument("--max-branches", type=int, default=None)
    parser.add_argument(
        "--exit-when-idle",
        action="store_true",
        help="exit once no claimable branch remains (batch/test mode)",
    )
    args = parser.parse_args(argv)
    processed = asyncio.run(
        run_worker(
            worker_id=args.worker_id,
            lease_ttl_s=args.lease_ttl,
            poll_interval_s=args.poll_interval,
            repo_root=args.repo_root,
            runs_root=args.runs_root,
            max_branches=args.max_branches,
            exit_when_idle=args.exit_when_idle,
        )
    )
    print(f"[worker] processed {processed} branch(es)", flush=True)


if __name__ == "__main__":
    main()
