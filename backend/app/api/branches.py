"""Branch metadata and trajectory REST endpoints.

Two modes:
- durable (Postgres store active): branch rows come from ``branch_runs``,
  cancellation goes through fenced ``request_cancel`` (I6) and works even
  when the executing worker is another process.
- memory fallback: the original in-process registry.
"""

from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, HTTPException, Request, status

from app.api.runs import get_run_dir
from app.meta_harness.branches import (
    branch_registry,
    cancel_branch,
    get_branch,
    list_branches,
    reconstruct_trajectory,
)
from app.streaming import emit_run_event


router = APIRouter(tags=["branches"])


def _store(request: Request) -> Any | None:
    return getattr(request.app.state, "store", None)


def _trajectory_from_rows(run_id: str, rows: list[Any]) -> dict[str, Any]:
    threads: dict[str, dict[str, Any]] = {
        run_id: {
            "thread_id": run_id,
            "run_id": run_id,
            "parent_thread_id": None,
            "parent_checkpoint_id": None,
            "status": "root",
            "branch_id": None,
            "name": "root",
        }
    }
    edges: list[dict[str, Any]] = []
    for row in rows:
        threads[row.thread_id] = row.to_dict()
        edges.append(
            {
                "source": row.parent_thread_id,
                "target": row.thread_id,
                "parent_checkpoint_id": row.parent_checkpoint_id,
            }
        )
    return {"run_id": run_id, "threads": list(threads.values()), "edges": edges}


@router.get("/runs/{run_id}/branches")
async def list_run_branches(run_id: str, request: Request) -> dict[str, Any]:
    get_run_dir(request, run_id)
    store = _store(request)
    if store is not None:
        rows = await store.list_branches(run_id=run_id)
        return {"branches": [row.to_dict() for row in rows]}
    return {
        "branches": [branch.to_dict() for branch in list_branches(run_id=run_id)]
    }


@router.get("/runs/{run_id}/trajectory")
async def get_run_trajectory(run_id: str, request: Request) -> dict[str, Any]:
    get_run_dir(request, run_id)
    store = _store(request)
    if store is not None:
        rows = await store.list_branches(run_id=run_id)
        return {"trajectory": _trajectory_from_rows(run_id, rows)}
    return {"trajectory": reconstruct_trajectory(run_id)}


@router.get("/branches/{branch_id}")
async def get_branch_status(branch_id: str, request: Request) -> dict[str, Any]:
    """Branch status by id — the MCP poll endpoint (MCP_SERVER_SPEC §2).

    Exposes ``lease_generation`` deliberately: a client that cached the
    fence and sees it incremented knows its branch was reclaimed by
    another worker after a lease expiry.
    """
    store = _store(request)
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="branch status requires the Postgres-backed store",
        )
    row = await store.get_branch(branch_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="unknown branch_id"
        )
    info = row.to_dict()
    info["lease_valid"] = bool(
        row.status == "running"
        and row.lease_expires_at is not None
        and row.lease_expires_at > time.time()
    )
    # Best-effort checkpoint position from the shared checkpointer.
    info["last_checkpoint_id"] = None
    info["iteration"] = None
    checkpointer = getattr(request.app.state, "checkpointer", None)
    if checkpointer is not None:
        try:
            async for snapshot in checkpointer.alist(
                {"configurable": {"thread_id": row.thread_id}}, limit=1
            ):
                info["last_checkpoint_id"] = (
                    snapshot.config.get("configurable", {}).get("checkpoint_id")
                )
                values = (snapshot.checkpoint or {}).get("channel_values", {})
                info["iteration"] = values.get("iteration")
        except Exception:  # noqa: BLE001 — status must not fail on history reads
            pass
    return info


@router.delete("/branches/{branch_id}")
async def cancel_branch_by_id(branch_id: str, request: Request) -> dict[str, Any]:
    """Durable cancel by branch id (I6).

    The `branch_runs` write happens first — `request_cancel` bumps the
    fence in the store before any in-process task is touched, so a crash
    between the two can never leave a cancelled-but-resumable branch.
    """
    store = _store(request)
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="durable cancel requires the Postgres-backed store",
        )
    row = await store.get_branch(branch_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="unknown branch_id"
        )
    cancelled = await store.request_cancel(branch_id)
    task = branch_registry.get(row.thread_id)
    if task is not None and not task.done():
        try:
            await cancel_branch(row.thread_id)
        except KeyError:
            pass
    emit_run_event(
        row.run_id,
        "branch-cancelled",
        {"thread_id": row.thread_id, "reason": "requested"},
    )
    return {"branch_id": branch_id, "status": cancelled.status}


@router.post("/runs/{run_id}/branches/{thread_id}/cancel")
async def cancel_run_branch(
    run_id: str,
    thread_id: str,
    request: Request,
) -> dict[str, str]:
    get_run_dir(request, run_id)
    store = _store(request)

    if store is not None:
        row = await store.get_branch_by_thread(thread_id)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="branch not found",
            )
        cancelled = await store.request_cancel(row.branch_id)
        # If this process also happens to run the branch task (single
        # process dev mode), stop it promptly; the fence bump already
        # guarantees a remote worker aborts at its next guarded write.
        task = branch_registry.get(thread_id)
        if task is not None and not task.done():
            try:
                await cancel_branch(thread_id)
            except KeyError:
                pass
        emit_run_event(
            run_id,
            "branch-cancelled",
            {"thread_id": thread_id, "reason": "requested"},
        )
        return {"status": cancelled.status}

    if get_branch(thread_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="branch not found",
        )
    try:
        metadata = await cancel_branch(thread_id)
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from None

    emit_run_event(
        run_id,
        "branch-cancelled",
        {
            "thread_id": thread_id,
            "reason": "requested",
        },
    )
    return {"status": metadata.status}
