"""Durable branch state store (REPOSITIONING_PLAN Phase 2).

All branch-lifecycle state access goes through the :class:`StateStore`
protocol, with two implementations:

- :class:`PostgresStateStore` — the real thing: ``branch_runs`` table,
  ``FOR UPDATE SKIP LOCKED`` claiming, lease TTLs, fencing tokens, and a
  per-run event log with gap-free monotonic sequence numbers (I7) fanned
  out via ``NOTIFY``.
- :class:`InMemoryStateStore` — a deterministic fake with an injectable
  clock. The Phase 3 simulator drives the orchestrator against this
  implementation; a real database cannot be made deterministic.

Fencing (I5): every successful claim increments ``lease_generation`` and
the claimer holds that value as its fence. Every subsequent write —
heartbeat, finish — carries the fence and is rejected with
:class:`StaleFenceError` when it no longer matches. A worker that stalls
past lease expiry and gets reclaimed learns it lost ownership at its
next write instead of silently double-executing.

Branch state machine (see docs/INVARIANTS.md):

    created → running → completed | failed | cancelled
    created → cancelled
    running → running        (lease reclaimed after expiry — NEW fence)
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Protocol, runtime_checkable

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row
from psycopg.types.json import Json

TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})


class StaleFenceError(RuntimeError):
    """A write carried a fence that no longer owns the branch.

    The only correct reaction is to abort the local work — never retry:
    another worker owns the branch now (or the branch was cancelled).
    """


class UnknownBranchError(KeyError):
    """Referenced branch_id does not exist."""


def _iso(epoch: float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


@dataclass
class BranchRow:
    """One row of durable branch state."""

    branch_id: str
    run_id: str
    thread_id: str
    parent_thread_id: str | None
    parent_checkpoint_id: str | None
    status: str
    mods: dict[str, Any] = field(default_factory=dict)
    name: str | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    lease_owner: str | None = None
    lease_generation: int = 0
    lease_expires_at: float | None = None  # epoch seconds; None when unleased
    created_at: float = 0.0
    started_at: float | None = None
    finished_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "branch_id": self.branch_id,
            "run_id": self.run_id,
            "thread_id": self.thread_id,
            "parent_thread_id": self.parent_thread_id,
            "parent_checkpoint_id": self.parent_checkpoint_id,
            "status": self.status,
            "mods": self.mods,
            "name": self.name,
            "result": self.result,
            "error": self.error,
            "lease_owner": self.lease_owner,
            "lease_generation": self.lease_generation,
            "lease_expires_at": _iso(self.lease_expires_at),
            "created_at": _iso(self.created_at),
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
        }


@dataclass
class WorkerRow:
    """One registered worker process (chaos/observability surface)."""

    worker_id: str
    pid: int
    hostname: str
    started_at: float
    last_seen: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "pid": self.pid,
            "hostname": self.hostname,
            "started_at": _iso(self.started_at),
            "last_seen": _iso(self.last_seen),
        }


@dataclass(frozen=True)
class StoredEvent:
    """One durable per-run event with a gap-free monotonic sequence (I7)."""

    run_id: str
    seq: int
    event_type: str
    payload: dict[str, Any]
    ts: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "seq": self.seq,
            "event_type": self.event_type,
            "payload": self.payload,
            "ts": _iso(self.ts),
        }


@runtime_checkable
class StateStore(Protocol):
    """Everything the orchestrator may ask of durable state."""

    async def setup(self) -> None: ...

    # ── branch lifecycle ─────────────────────────────────────────────
    async def create_branch(
        self,
        *,
        branch_id: str,
        run_id: str,
        thread_id: str,
        parent_thread_id: str | None,
        parent_checkpoint_id: str | None,
        mods: dict[str, Any],
        name: str | None = None,
    ) -> BranchRow: ...

    async def claim_next_branch(
        self, *, worker_id: str, lease_ttl_s: float
    ) -> BranchRow | None: ...

    async def heartbeat(
        self, *, branch_id: str, fence: int, lease_ttl_s: float
    ) -> None: ...

    async def finish_branch(
        self,
        *,
        branch_id: str,
        fence: int,
        status: str,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None: ...

    async def request_cancel(self, branch_id: str) -> BranchRow: ...

    async def get_branch(self, branch_id: str) -> BranchRow | None: ...

    async def get_branch_by_thread(self, thread_id: str) -> BranchRow | None: ...

    async def list_branches(self, *, run_id: str | None = None) -> list[BranchRow]: ...

    async def reconcile_on_boot(self) -> list[BranchRow]: ...

    # ── worker registry (chaos/observability) ────────────────────────
    async def register_worker(
        self, *, worker_id: str, pid: int, hostname: str
    ) -> WorkerRow: ...

    async def touch_worker(self, worker_id: str) -> None: ...

    async def remove_worker(self, worker_id: str) -> None: ...

    async def list_workers(self) -> list[WorkerRow]: ...

    async def get_worker(self, worker_id: str) -> WorkerRow | None: ...

    # ── authoritative iteration log (I1) ─────────────────────────────
    async def record_iteration(
        self,
        *,
        run_id: str,
        iteration: int,
        candidate: str,
        row: dict[str, Any],
        branch_id: str | None = None,
        fence: int | None = None,
    ) -> bool: ...

    async def list_iterations(self, *, run_id: str) -> list[dict[str, Any]]: ...

    # ── per-run event log (I7) ───────────────────────────────────────
    async def append_event(
        self, *, run_id: str, event_type: str, payload: dict[str, Any]
    ) -> StoredEvent: ...

    async def list_events(
        self, *, run_id: str, after_seq: int = 0
    ) -> list[StoredEvent]: ...


# ─────────────────────────────────────────────────────────────────────
# In-memory deterministic implementation
# ─────────────────────────────────────────────────────────────────────


class InMemoryStateStore:
    """Deterministic in-memory :class:`StateStore`.

    ``clock`` is injectable so the Phase 3 simulator can drive lease
    expiry from a virtual clock. All mutations are synchronous between
    awaits, so a single-threaded event loop sees them atomically.
    """

    def __init__(self, *, clock: Callable[[], float] | None = None) -> None:
        self._clock = clock or time.time
        self._branches: dict[str, BranchRow] = {}
        self._iterations: dict[str, dict[tuple[int, str], dict[str, Any]]] = {}
        self._workers: dict[str, WorkerRow] = {}
        self._events: dict[str, list[StoredEvent]] = {}
        self._event_waiters: dict[str, list[asyncio.Event]] = {}
        self._create_counter = 0  # tiebreak FIFO when clock is frozen

    async def setup(self) -> None:
        return None

    def _now(self) -> float:
        return self._clock()

    async def create_branch(
        self,
        *,
        branch_id: str,
        run_id: str,
        thread_id: str,
        parent_thread_id: str | None,
        parent_checkpoint_id: str | None,
        mods: dict[str, Any],
        name: str | None = None,
    ) -> BranchRow:
        if branch_id in self._branches:
            raise ValueError(f"branch_id already exists: {branch_id}")
        self._create_counter += 1
        row = BranchRow(
            branch_id=branch_id,
            run_id=run_id,
            thread_id=thread_id,
            parent_thread_id=parent_thread_id,
            parent_checkpoint_id=parent_checkpoint_id,
            status="created",
            mods=dict(mods),
            name=name,
            created_at=self._now() + self._create_counter * 1e-9,
        )
        self._branches[branch_id] = row
        return _copy_row(row)

    async def claim_next_branch(
        self, *, worker_id: str, lease_ttl_s: float
    ) -> BranchRow | None:
        now = self._now()
        claimable = [
            r
            for r in self._branches.values()
            if r.status == "created"
            or (
                r.status == "running"
                and r.lease_expires_at is not None
                and r.lease_expires_at < now
            )
        ]
        if not claimable:
            return None
        row = min(claimable, key=lambda r: r.created_at)
        row.status = "running"
        row.lease_owner = worker_id
        row.lease_generation += 1
        row.lease_expires_at = now + lease_ttl_s
        row.started_at = row.started_at if row.started_at is not None else now
        return _copy_row(row)

    async def heartbeat(
        self, *, branch_id: str, fence: int, lease_ttl_s: float
    ) -> None:
        row = self._branches.get(branch_id)
        if row is None:
            raise UnknownBranchError(branch_id)
        if row.status != "running" or row.lease_generation != fence:
            raise StaleFenceError(
                f"branch {branch_id}: fence {fence} is stale "
                f"(status={row.status}, generation={row.lease_generation})"
            )
        row.lease_expires_at = self._now() + lease_ttl_s

    async def finish_branch(
        self,
        *,
        branch_id: str,
        fence: int,
        status: str,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        if status not in TERMINAL_STATUSES:
            raise ValueError(f"finish_branch status must be terminal, got {status!r}")
        row = self._branches.get(branch_id)
        if row is None:
            raise UnknownBranchError(branch_id)
        if row.status != "running" or row.lease_generation != fence:
            raise StaleFenceError(
                f"branch {branch_id}: fence {fence} is stale "
                f"(status={row.status}, generation={row.lease_generation})"
            )
        row.status = status
        row.result = result
        row.error = error
        row.finished_at = self._now()
        row.lease_owner = None
        row.lease_expires_at = None

    async def request_cancel(self, branch_id: str) -> BranchRow:
        row = self._branches.get(branch_id)
        if row is None:
            raise UnknownBranchError(branch_id)
        if row.status in TERMINAL_STATUSES:
            return _copy_row(row)
        # Bump the fence so any live worker's next guarded write aborts
        # (I5), and land in a terminal state a restart can never revive
        # (I6).
        row.lease_generation += 1
        row.status = "cancelled"
        row.finished_at = self._now()
        row.lease_owner = None
        row.lease_expires_at = None
        return _copy_row(row)

    async def get_branch(self, branch_id: str) -> BranchRow | None:
        row = self._branches.get(branch_id)
        return _copy_row(row) if row else None

    async def get_branch_by_thread(self, thread_id: str) -> BranchRow | None:
        for row in self._branches.values():
            if row.thread_id == thread_id:
                return _copy_row(row)
        return None

    async def list_branches(self, *, run_id: str | None = None) -> list[BranchRow]:
        rows = [
            _copy_row(r)
            for r in self._branches.values()
            if run_id is None or r.run_id == run_id
        ]
        return sorted(rows, key=lambda r: r.created_at)

    async def reconcile_on_boot(self) -> list[BranchRow]:
        now = self._now()
        affected: list[BranchRow] = []
        for row in self._branches.values():
            if row.status != "running":
                continue
            if row.lease_expires_at is None or row.lease_expires_at < now:
                # Requeue from last checkpoint: claimable again, fence
                # preserved (monotonic forever — never reset).
                row.status = "created"
                row.lease_owner = None
                row.lease_expires_at = None
                affected.append(_copy_row(row))
        return affected

    async def register_worker(
        self, *, worker_id: str, pid: int, hostname: str
    ) -> WorkerRow:
        now = self._now()
        existing = self._workers.get(worker_id)
        row = WorkerRow(
            worker_id=worker_id,
            pid=pid,
            hostname=hostname,
            started_at=existing.started_at if existing else now,
            last_seen=now,
        )
        self._workers[worker_id] = row
        return row

    async def touch_worker(self, worker_id: str) -> None:
        row = self._workers.get(worker_id)
        if row is not None:
            row.last_seen = self._now()

    async def remove_worker(self, worker_id: str) -> None:
        self._workers.pop(worker_id, None)

    async def list_workers(self) -> list[WorkerRow]:
        return sorted(self._workers.values(), key=lambda w: w.started_at)

    async def get_worker(self, worker_id: str) -> WorkerRow | None:
        return self._workers.get(worker_id)

    async def record_iteration(
        self,
        *,
        run_id: str,
        iteration: int,
        candidate: str,
        row: dict[str, Any],
        branch_id: str | None = None,
        fence: int | None = None,
    ) -> bool:
        """Atomically record one iteration exactly once (I1).

        With ``branch_id``/``fence``, the write is fence-guarded: a
        stalled worker whose lease was reclaimed gets
        :class:`StaleFenceError` *from the data layer itself* — the file
        append it would otherwise race on can't protect itself.
        Returns ``True`` if newly recorded, ``False`` if the
        (iteration, candidate) key already exists.
        """
        if branch_id is not None:
            branch = self._branches.get(branch_id)
            if branch is None:
                raise UnknownBranchError(branch_id)
            if branch.status != "running" or branch.lease_generation != fence:
                raise StaleFenceError(
                    f"branch {branch_id}: record_iteration fence {fence} is "
                    f"stale (status={branch.status}, "
                    f"generation={branch.lease_generation})"
                )
        log = self._iterations.setdefault(run_id, {})
        key = (iteration, candidate)
        if key in log:
            return False
        log[key] = dict(row)
        return True

    async def list_iterations(self, *, run_id: str) -> list[dict[str, Any]]:
        log = self._iterations.get(run_id, {})
        return [dict(log[k]) for k in sorted(log)]

    async def append_event(
        self, *, run_id: str, event_type: str, payload: dict[str, Any]
    ) -> StoredEvent:
        events = self._events.setdefault(run_id, [])
        event = StoredEvent(
            run_id=run_id,
            seq=len(events) + 1,
            event_type=event_type,
            payload=dict(payload),
            ts=self._now(),
        )
        events.append(event)
        for waiter in self._event_waiters.get(run_id, []):
            waiter.set()
        return event

    async def list_events(
        self, *, run_id: str, after_seq: int = 0
    ) -> list[StoredEvent]:
        return [e for e in self._events.get(run_id, []) if e.seq > after_seq]

    async def stream_events(
        self, *, run_id: str, after_seq: int = 0
    ) -> AsyncIterator[StoredEvent]:
        """Yield events in seq order forever (test/simulator helper)."""
        seq = after_seq
        waiter = asyncio.Event()
        self._event_waiters.setdefault(run_id, []).append(waiter)
        try:
            while True:
                fresh = await self.list_events(run_id=run_id, after_seq=seq)
                if not fresh:
                    waiter.clear()
                    await waiter.wait()
                    continue
                for event in fresh:
                    seq = event.seq
                    yield event
        finally:
            self._event_waiters.get(run_id, []).remove(waiter)


def _copy_row(row: BranchRow) -> BranchRow:
    return BranchRow(
        branch_id=row.branch_id,
        run_id=row.run_id,
        thread_id=row.thread_id,
        parent_thread_id=row.parent_thread_id,
        parent_checkpoint_id=row.parent_checkpoint_id,
        status=row.status,
        mods=dict(row.mods),
        name=row.name,
        result=dict(row.result) if row.result is not None else None,
        error=row.error,
        lease_owner=row.lease_owner,
        lease_generation=row.lease_generation,
        lease_expires_at=row.lease_expires_at,
        created_at=row.created_at,
        started_at=row.started_at,
        finished_at=row.finished_at,
    )


# ─────────────────────────────────────────────────────────────────────
# Postgres implementation
# ─────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS branch_runs (
    branch_id            TEXT PRIMARY KEY,
    run_id               TEXT NOT NULL,
    thread_id            TEXT NOT NULL UNIQUE,
    parent_thread_id     TEXT,
    parent_checkpoint_id TEXT,
    status               TEXT NOT NULL,   -- created|running|completed|failed|cancelled
    mods                 JSONB NOT NULL DEFAULT '{}',
    name                 TEXT,
    result               JSONB,
    error                TEXT,
    lease_owner          TEXT,            -- worker id
    lease_generation     BIGINT NOT NULL DEFAULT 0,   -- fencing token
    lease_expires_at     TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at           TIMESTAMPTZ,
    finished_at          TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS branch_runs_status_lease_idx
    ON branch_runs (status, lease_expires_at);
CREATE INDEX IF NOT EXISTS branch_runs_run_id_idx
    ON branch_runs (run_id);

CREATE TABLE IF NOT EXISTS run_event_seq (
    run_id   TEXT PRIMARY KEY,
    last_seq BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id  TEXT PRIMARY KEY,
    pid        BIGINT NOT NULL,
    hostname   TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS iteration_log (
    run_id    TEXT NOT NULL,
    iteration BIGINT NOT NULL,
    candidate TEXT NOT NULL,
    row       JSONB NOT NULL,
    ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, iteration, candidate)
);

CREATE TABLE IF NOT EXISTS run_events (
    run_id     TEXT NOT NULL,
    seq        BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    payload    JSONB NOT NULL,
    ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, seq)
);
"""

_CLAIM_SQL = """
UPDATE branch_runs SET
    status           = 'running',
    lease_owner      = %(worker_id)s,
    lease_generation = lease_generation + 1,
    lease_expires_at = now() + %(lease_ttl)s * interval '1 second',
    started_at       = COALESCE(started_at, now())
WHERE branch_id = (
    SELECT branch_id FROM branch_runs
    WHERE status = 'created'
       OR (status = 'running' AND lease_expires_at < now())
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;
"""


def notify_channel_for_run(run_id: str) -> str:
    """Postgres NOTIFY channel for one run's events."""
    return f"run_events_{run_id}"


class PostgresStateStore:
    """Postgres-backed :class:`StateStore`.

    Uses short-lived operations on a dedicated autocommit connection.
    The claiming query is a single atomic UPDATE (subquery with
    ``FOR UPDATE SKIP LOCKED``), so concurrent workers never double-claim
    (I5's easy half; fencing covers the hard half).
    """

    def __init__(self, conn: AsyncConnection) -> None:
        self._conn = conn

    @classmethod
    async def connect(cls, dsn: str) -> "PostgresStateStore":
        conn = await AsyncConnection.connect(
            conninfo=dsn, row_factory=dict_row, autocommit=True
        )
        return cls(conn)

    async def close(self) -> None:
        await self._conn.close()

    async def setup(self) -> None:
        await self._conn.execute(_SCHEMA)

    async def create_branch(
        self,
        *,
        branch_id: str,
        run_id: str,
        thread_id: str,
        parent_thread_id: str | None,
        parent_checkpoint_id: str | None,
        mods: dict[str, Any],
        name: str | None = None,
    ) -> BranchRow:
        cur = await self._conn.execute(
            """
            INSERT INTO branch_runs (
                branch_id, run_id, thread_id, parent_thread_id,
                parent_checkpoint_id, status, mods, name
            )
            VALUES (%s, %s, %s, %s, %s, 'created', %s, %s)
            RETURNING *;
            """,
            (
                branch_id,
                run_id,
                thread_id,
                parent_thread_id,
                parent_checkpoint_id,
                Json(mods),
                name,
            ),
        )
        return self._row(await cur.fetchone())

    async def claim_next_branch(
        self, *, worker_id: str, lease_ttl_s: float
    ) -> BranchRow | None:
        cur = await self._conn.execute(
            _CLAIM_SQL, {"worker_id": worker_id, "lease_ttl": lease_ttl_s}
        )
        record = await cur.fetchone()
        return self._row(record) if record else None

    async def heartbeat(
        self, *, branch_id: str, fence: int, lease_ttl_s: float
    ) -> None:
        cur = await self._conn.execute(
            """
            UPDATE branch_runs
            SET lease_expires_at = now() + %(lease_ttl)s * interval '1 second'
            WHERE branch_id = %(branch_id)s
              AND status = 'running'
              AND lease_generation = %(fence)s;
            """,
            {"branch_id": branch_id, "fence": fence, "lease_ttl": lease_ttl_s},
        )
        if cur.rowcount == 0:
            raise StaleFenceError(
                f"branch {branch_id}: heartbeat fence {fence} is stale — "
                "lease was reclaimed or branch cancelled; abort"
            )

    async def finish_branch(
        self,
        *,
        branch_id: str,
        fence: int,
        status: str,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        if status not in TERMINAL_STATUSES:
            raise ValueError(f"finish_branch status must be terminal, got {status!r}")
        cur = await self._conn.execute(
            """
            UPDATE branch_runs SET
                status           = %(status)s,
                result           = %(result)s,
                error            = %(error)s,
                finished_at      = now(),
                lease_owner      = NULL,
                lease_expires_at = NULL
            WHERE branch_id = %(branch_id)s
              AND status = 'running'
              AND lease_generation = %(fence)s;
            """,
            {
                "branch_id": branch_id,
                "fence": fence,
                "status": status,
                "result": Json(result) if result is not None else None,
                "error": error,
            },
        )
        if cur.rowcount == 0:
            raise StaleFenceError(
                f"branch {branch_id}: finish fence {fence} is stale — "
                "another worker owns this branch; abort without retry"
            )

    async def request_cancel(self, branch_id: str) -> BranchRow:
        cur = await self._conn.execute(
            """
            UPDATE branch_runs SET
                status           = 'cancelled',
                lease_generation = lease_generation + 1,
                finished_at      = now(),
                lease_owner      = NULL,
                lease_expires_at = NULL
            WHERE branch_id = %(branch_id)s
              AND status NOT IN ('completed', 'failed', 'cancelled')
            RETURNING *;
            """,
            {"branch_id": branch_id},
        )
        record = await cur.fetchone()
        if record is not None:
            return self._row(record)
        existing = await self.get_branch(branch_id)
        if existing is None:
            raise UnknownBranchError(branch_id)
        return existing

    async def get_branch(self, branch_id: str) -> BranchRow | None:
        cur = await self._conn.execute(
            "SELECT * FROM branch_runs WHERE branch_id = %s;", (branch_id,)
        )
        record = await cur.fetchone()
        return self._row(record) if record else None

    async def get_branch_by_thread(self, thread_id: str) -> BranchRow | None:
        cur = await self._conn.execute(
            "SELECT * FROM branch_runs WHERE thread_id = %s;", (thread_id,)
        )
        record = await cur.fetchone()
        return self._row(record) if record else None

    async def list_branches(self, *, run_id: str | None = None) -> list[BranchRow]:
        if run_id is None:
            cur = await self._conn.execute(
                "SELECT * FROM branch_runs ORDER BY created_at;"
            )
        else:
            cur = await self._conn.execute(
                "SELECT * FROM branch_runs WHERE run_id = %s ORDER BY created_at;",
                (run_id,),
            )
        return [self._row(r) for r in await cur.fetchall()]

    async def reconcile_on_boot(self) -> list[BranchRow]:
        cur = await self._conn.execute(
            """
            UPDATE branch_runs SET
                status           = 'created',
                lease_owner      = NULL,
                lease_expires_at = NULL
            WHERE status = 'running'
              AND (lease_expires_at IS NULL OR lease_expires_at < now())
            RETURNING *;
            """
        )
        return [self._row(r) for r in await cur.fetchall()]

    async def register_worker(
        self, *, worker_id: str, pid: int, hostname: str
    ) -> WorkerRow:
        cur = await self._conn.execute(
            """
            INSERT INTO workers (worker_id, pid, hostname)
            VALUES (%s, %s, %s)
            ON CONFLICT (worker_id) DO UPDATE
                SET pid = EXCLUDED.pid,
                    hostname = EXCLUDED.hostname,
                    last_seen = now()
            RETURNING *;
            """,
            (worker_id, pid, hostname),
        )
        return self._worker_row(await cur.fetchone())

    async def touch_worker(self, worker_id: str) -> None:
        await self._conn.execute(
            "UPDATE workers SET last_seen = now() WHERE worker_id = %s;",
            (worker_id,),
        )

    async def remove_worker(self, worker_id: str) -> None:
        await self._conn.execute(
            "DELETE FROM workers WHERE worker_id = %s;", (worker_id,)
        )

    async def list_workers(self) -> list[WorkerRow]:
        cur = await self._conn.execute("SELECT * FROM workers ORDER BY started_at;")
        return [self._worker_row(r) for r in await cur.fetchall()]

    async def get_worker(self, worker_id: str) -> WorkerRow | None:
        cur = await self._conn.execute(
            "SELECT * FROM workers WHERE worker_id = %s;", (worker_id,)
        )
        record = await cur.fetchone()
        return self._worker_row(record) if record else None

    @staticmethod
    def _worker_row(record: dict[str, Any]) -> WorkerRow:
        return WorkerRow(
            worker_id=record["worker_id"],
            pid=record["pid"],
            hostname=record["hostname"],
            started_at=record["started_at"].timestamp(),
            last_seen=record["last_seen"].timestamp(),
        )

    async def record_iteration(
        self,
        *,
        run_id: str,
        iteration: int,
        candidate: str,
        row: dict[str, Any],
        branch_id: str | None = None,
        fence: int | None = None,
    ) -> bool:
        if branch_id is not None:
            # Fence check and insert in ONE statement: the insert only
            # happens while the fence is provably current, and the
            # primary key makes the record exactly-once (I1).
            cur = await self._conn.execute(
                """
                INSERT INTO iteration_log (run_id, iteration, candidate, row)
                SELECT %(run_id)s, %(iteration)s, %(candidate)s, %(row)s
                WHERE EXISTS (
                    SELECT 1 FROM branch_runs
                    WHERE branch_id = %(branch_id)s
                      AND status = 'running'
                      AND lease_generation = %(fence)s
                )
                ON CONFLICT (run_id, iteration, candidate) DO NOTHING;
                """,
                {
                    "run_id": run_id,
                    "iteration": iteration,
                    "candidate": candidate,
                    "row": Json(row),
                    "branch_id": branch_id,
                    "fence": fence,
                },
            )
            if cur.rowcount == 1:
                return True
            # Nothing inserted: either duplicate key or stale fence —
            # disambiguate so a reclaimed worker aborts loudly.
            check = await self._conn.execute(
                """
                SELECT 1 FROM branch_runs
                WHERE branch_id = %s AND status = 'running'
                  AND lease_generation = %s;
                """,
                (branch_id, fence),
            )
            if await check.fetchone() is None:
                raise StaleFenceError(
                    f"branch {branch_id}: record_iteration fence {fence} is "
                    "stale — lease reclaimed or branch cancelled; abort"
                )
            return False

        cur = await self._conn.execute(
            """
            INSERT INTO iteration_log (run_id, iteration, candidate, row)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (run_id, iteration, candidate) DO NOTHING;
            """,
            (run_id, iteration, candidate, Json(row)),
        )
        return cur.rowcount == 1

    async def list_iterations(self, *, run_id: str) -> list[dict[str, Any]]:
        cur = await self._conn.execute(
            """
            SELECT row FROM iteration_log
            WHERE run_id = %s
            ORDER BY iteration, candidate;
            """,
            (run_id,),
        )
        return [r["row"] for r in await cur.fetchall()]

    async def append_event(
        self, *, run_id: str, event_type: str, payload: dict[str, Any]
    ) -> StoredEvent:
        # Gap-free per-run sequence via an atomic upsert counter + event
        # insert + NOTIFY in ONE statement (implicit transaction), so a
        # crash never leaks a seq without its event (I7) and concurrent
        # appends on a shared connection cannot interleave transactions.
        cur = await self._conn.execute(
            """
            WITH bump AS (
                INSERT INTO run_event_seq (run_id, last_seq)
                VALUES (%(run_id)s, 1)
                ON CONFLICT (run_id)
                DO UPDATE SET last_seq = run_event_seq.last_seq + 1
                RETURNING last_seq
            ), ins AS (
                INSERT INTO run_events (run_id, seq, event_type, payload)
                SELECT %(run_id)s, last_seq, %(event_type)s, %(payload)s
                FROM bump
                RETURNING seq, ts
            )
            SELECT
                ins.seq,
                ins.ts,
                pg_notify(
                    %(channel)s,
                    json_build_object(
                        'seq', ins.seq, 'event_type', %(event_type)s
                    )::text
                )
            FROM ins;
            """,
            {
                "run_id": run_id,
                "event_type": event_type,
                "payload": Json(payload),
                "channel": notify_channel_for_run(run_id),
            },
        )
        record = await cur.fetchone()
        seq, ts = record["seq"], record["ts"]
        return StoredEvent(
            run_id=run_id,
            seq=seq,
            event_type=event_type,
            payload=dict(payload),
            ts=ts.timestamp(),
        )

    async def list_events(
        self, *, run_id: str, after_seq: int = 0
    ) -> list[StoredEvent]:
        cur = await self._conn.execute(
            """
            SELECT run_id, seq, event_type, payload, ts
            FROM run_events
            WHERE run_id = %s AND seq > %s
            ORDER BY seq;
            """,
            (run_id, after_seq),
        )
        return [
            StoredEvent(
                run_id=r["run_id"],
                seq=r["seq"],
                event_type=r["event_type"],
                payload=r["payload"],
                ts=r["ts"].timestamp(),
            )
            for r in await cur.fetchall()
        ]

    @staticmethod
    def _row(record: dict[str, Any]) -> BranchRow:
        def _epoch(value: Any) -> float | None:
            return value.timestamp() if value is not None else None

        return BranchRow(
            branch_id=record["branch_id"],
            run_id=record["run_id"],
            thread_id=record["thread_id"],
            parent_thread_id=record["parent_thread_id"],
            parent_checkpoint_id=record["parent_checkpoint_id"],
            status=record["status"],
            mods=record["mods"] or {},
            name=record["name"],
            result=record["result"],
            error=record["error"],
            lease_owner=record["lease_owner"],
            lease_generation=record["lease_generation"],
            lease_expires_at=_epoch(record["lease_expires_at"]),
            created_at=record["created_at"].timestamp(),
            started_at=_epoch(record["started_at"]),
            finished_at=_epoch(record["finished_at"]),
        )


async def listen_run_events(
    dsn: str,
    run_id: str,
    *,
    stop: asyncio.Event | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """LISTEN on a run's NOTIFY channel; yield decoded notification payloads.

    Uses its own connection: LISTEN must not share a connection with
    regular queries.
    """
    conn = await AsyncConnection.connect(conninfo=dsn, autocommit=True)
    try:
        await conn.execute(
            sql.SQL("LISTEN {};").format(
                sql.Identifier(notify_channel_for_run(run_id))
            )
        )
        gen = conn.notifies()
        async for notification in gen:
            if stop is not None and stop.is_set():
                break
            try:
                yield json.loads(notification.payload)
            except json.JSONDecodeError:
                continue
    finally:
        await conn.close()
