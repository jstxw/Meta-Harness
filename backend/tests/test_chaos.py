"""Chaos endpoint tests (REPOSITIONING_PLAN Phase 4.1).

POST /debug/kill-worker/{worker_id} is double-gated (env flag + same
host) and actually SIGKILLs the registered pid.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.main import create_app  # noqa: E402
from app.meta_harness.persistence import get_dsn  # noqa: E402
from app.meta_harness.store import PostgresStateStore  # noqa: E402


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_chaos_routes_are_404_without_env_flag(monkeypatch):
    monkeypatch.delenv("META_HARNESS_CHAOS", raising=False)
    with TestClient(create_app(use_persistence=False)) as client:
        assert client.get("/debug/workers").status_code == 404
        assert client.post("/debug/kill-worker/whoever").status_code == 404


@pytest.mark.usefixtures("require_postgres")
def test_chaos_kill_worker_sigkills_registered_pid(monkeypatch):
    monkeypatch.setenv("META_HARNESS_CHAOS", "1")
    worker_id = f"chaos-test-{uuid.uuid4().hex[:8]}"
    victim = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    app = create_app(use_persistence=True)
    try:
        with TestClient(app) as client:
            assert app.state.store is not None

            import asyncio

            # Separate connection: the app store belongs to the
            # TestClient's event loop, not this thread's.
            asyncio.run(store_register(worker_id, victim.pid, socket.gethostname()))

            listed = client.get("/debug/workers")
            assert listed.status_code == 200
            entry = next(
                w
                for w in listed.json()["workers"]
                if w["worker_id"] == worker_id
            )
            assert entry["alive"] is True

            killed = client.post(f"/debug/kill-worker/{worker_id}")
            assert killed.status_code == 202
            assert killed.json() == {
                "killed": worker_id,
                "pid": victim.pid,
                "signal": "SIGKILL",
            }

            victim.wait(timeout=10)
            deadline = time.monotonic() + 5
            while _pid_alive(victim.pid) and time.monotonic() < deadline:
                time.sleep(0.05)
            assert not _pid_alive(victim.pid)

            # A second kill reports the corpse and clears the registration.
            second = client.post(f"/debug/kill-worker/{worker_id}")
            assert second.status_code == 410

            asyncio.run(store_cleanup(worker_id))
    finally:
        if victim.poll() is None:
            victim.kill()
            victim.wait(timeout=10)


async def store_register(worker_id: str, pid: int, hostname: str) -> None:
    store = await PostgresStateStore.connect(get_dsn())
    try:
        await store.setup()
        await store.register_worker(worker_id=worker_id, pid=pid, hostname=hostname)
    finally:
        await store.close()


async def store_cleanup(worker_id: str) -> None:
    store = await PostgresStateStore.connect(get_dsn())
    try:
        await store.remove_worker(worker_id)
    finally:
        await store.close()
