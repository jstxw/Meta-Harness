"""Chaos endpoints (REPOSITIONING_PLAN Phase 4.1).

Dev-only, double-gated: the routes 404 unless META_HARNESS_CHAOS is
truthy, and a kill only fires for a worker registered on THIS host.
The point is a ~15-second live demo of the recovery path — kill -9 a
worker mid-branch and watch another worker reclaim the lease with an
incremented fence over SSE.
"""

from __future__ import annotations

import os
import signal
import socket
from typing import Any

from fastapi import APIRouter, HTTPException, Request, status

router = APIRouter(tags=["debug"])


def chaos_enabled() -> bool:
    return os.environ.get("META_HARNESS_CHAOS", "").lower() in {"1", "true", "yes", "on"}


def _store_or_503(request: Request) -> Any:
    store = getattr(request.app.state, "store", None)
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="chaos endpoints require the Postgres-backed store",
        )
    return store


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


@router.get("/debug/workers")
async def list_workers(request: Request) -> dict[str, Any]:
    if not chaos_enabled():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    store = _store_or_503(request)
    hostname = socket.gethostname()
    workers = []
    for row in await store.list_workers():
        info = row.to_dict()
        info["local"] = row.hostname == hostname
        info["alive"] = _pid_alive(row.pid) if info["local"] else None
        workers.append(info)
    return {"chaos_enabled": True, "workers": workers}


@router.post("/debug/kill-worker/{worker_id}", status_code=status.HTTP_202_ACCEPTED)
async def kill_worker(worker_id: str, request: Request) -> dict[str, Any]:
    if not chaos_enabled():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    store = _store_or_503(request)
    row = await store.get_worker(worker_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="worker not registered"
        )
    if row.hostname != socket.gethostname():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"worker {worker_id} runs on {row.hostname}, not this host",
        )
    if row.pid <= 1 or row.pid == os.getpid():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="refusing to kill this pid",
        )
    try:
        os.kill(row.pid, signal.SIGKILL)
    except ProcessLookupError:
        # Already dead — clear the stale registration so the UI agrees.
        await store.remove_worker(worker_id)
        raise HTTPException(
            status_code=status.HTTP_410_GONE, detail="worker already dead"
        ) from None
    return {"killed": worker_id, "pid": row.pid, "signal": "SIGKILL"}
