"""Branch metadata and trajectory REST endpoints.

Two modes:
- durable (Postgres store active): branch rows come from ``branch_runs``,
  cancellation goes through fenced ``request_cancel`` (I6) and works even
  when the executing worker is another process.
- memory fallback: the original in-process registry.
"""

from __future__ import annotations

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
