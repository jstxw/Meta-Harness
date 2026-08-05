"""StateStore contract tests (REPOSITIONING_PLAN Phase 2).

Every test runs against both implementations — the deterministic
in-memory fake and real Postgres (gated on reachability) — so the fake
the Phase 3 simulator relies on cannot drift from the real thing.

Tests are named after the invariants in docs/INVARIANTS.md.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.meta_harness.persistence import get_dsn  # noqa: E402
from app.meta_harness.store import (  # noqa: E402
    InMemoryStateStore,
    PostgresStateStore,
    StaleFenceError,
    StateStore,
)


class FakeClock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture(params=["memory", "postgres"])
async def store_ctx(request, postgres_available):
    """Yield ``(store, clock_or_None, expire_lease)`` per implementation.

    ``expire_lease`` is an async callable that makes any lease with the
    given TTL expire: virtual-clock advance for memory, real sleep for
    Postgres.
    """
    if request.param == "memory":
        clock = FakeClock()
        store = InMemoryStateStore(clock=clock)
        await store.setup()

        async def expire(ttl: float) -> None:
            clock.advance(ttl + 1.0)

        yield store, clock, expire
        return

    if not postgres_available:
        pytest.skip("Postgres not reachable at configured DSN")
    store = await PostgresStateStore.connect(get_dsn())
    await store.setup()

    async def expire(ttl: float) -> None:
        await asyncio.sleep(ttl + 0.15)

    run_prefix = f"storetest-{uuid.uuid4().hex[:8]}"
    request.node._run_prefix = run_prefix
    yield store, None, expire
    await store._conn.execute(
        "DELETE FROM branch_runs WHERE run_id LIKE %s;", (f"{run_prefix}%",)
    )
    await store._conn.execute(
        "DELETE FROM run_events WHERE run_id LIKE %s;", (f"{run_prefix}%",)
    )
    await store._conn.execute(
        "DELETE FROM run_event_seq WHERE run_id LIKE %s;", (f"{run_prefix}%",)
    )
    await store._conn.execute(
        "DELETE FROM workers WHERE worker_id LIKE %s;", (f"{run_prefix}%",)
    )
    await store.close()


def _run_id(request) -> str:
    prefix = getattr(request.node, "_run_prefix", None)
    return prefix or f"storetest-{uuid.uuid4().hex[:8]}"


async def _mk_branch(store: StateStore, run_id: str, n: int = 0):
    branch_id = f"{run_id}-b{n}-{uuid.uuid4().hex[:6]}"
    return await store.create_branch(
        branch_id=branch_id,
        run_id=run_id,
        thread_id=f"{run_id}.fork.{branch_id}",
        parent_thread_id=run_id,
        parent_checkpoint_id="ckpt-1",
        mods={"proposer_prior": "x"},
        name=f"branch {n}",
    )


# ── lifecycle basics ─────────────────────────────────────────────────


async def test_create_claim_finish_lifecycle(store_ctx, request):
    store, _, _ = store_ctx
    run_id = _run_id(request)
    created = await _mk_branch(store, run_id)
    assert created.status == "created"
    assert created.lease_generation == 0

    claimed = await store.claim_next_branch(worker_id="w1", lease_ttl_s=30)
    assert claimed is not None
    assert claimed.branch_id == created.branch_id
    assert claimed.status == "running"
    assert claimed.lease_generation == 1
    assert claimed.lease_owner == "w1"

    await store.finish_branch(
        branch_id=claimed.branch_id,
        fence=claimed.lease_generation,
        status="completed",
        result={"ok": True},
    )
    final = await store.get_branch(claimed.branch_id)
    assert final.status == "completed"
    assert final.result == {"ok": True}
    assert final.lease_owner is None


async def test_created_to_cancelled_direct(store_ctx, request):
    store, _, _ = store_ctx
    run_id = _run_id(request)
    created = await _mk_branch(store, run_id)
    cancelled = await store.request_cancel(created.branch_id)
    assert cancelled.status == "cancelled"
    # A cancelled branch is never claimable again.
    assert await store.claim_next_branch(worker_id="w1", lease_ttl_s=30) is None


# ── I5: lease safety via fencing ─────────────────────────────────────


async def test_i5_second_claim_blocked_while_lease_valid(store_ctx, request):
    store, _, _ = store_ctx
    run_id = _run_id(request)
    await _mk_branch(store, run_id)
    first = await store.claim_next_branch(worker_id="w1", lease_ttl_s=60)
    assert first is not None
    assert await store.claim_next_branch(worker_id="w2", lease_ttl_s=60) is None


async def test_i5_stale_fence_rejected_after_reclaim(store_ctx, request):
    """The fencing-token scenario from INVARIANTS.md: worker stalls past
    lease expiry, branch is reclaimed with a new fence, the stalled
    worker's writes must be rejected — abort, not retry."""
    store, _, expire = store_ctx
    run_id = _run_id(request)
    await _mk_branch(store, run_id)

    ttl = 0.2
    stalled = await store.claim_next_branch(worker_id="w1", lease_ttl_s=ttl)
    await expire(ttl)

    reclaimed = await store.claim_next_branch(worker_id="w2", lease_ttl_s=60)
    assert reclaimed is not None
    assert reclaimed.branch_id == stalled.branch_id
    assert reclaimed.lease_generation == stalled.lease_generation + 1

    # The stalled worker wakes up and tries to keep working:
    with pytest.raises(StaleFenceError):
        await store.heartbeat(
            branch_id=stalled.branch_id,
            fence=stalled.lease_generation,
            lease_ttl_s=ttl,
        )
    with pytest.raises(StaleFenceError):
        await store.finish_branch(
            branch_id=stalled.branch_id,
            fence=stalled.lease_generation,
            status="completed",
            result={"from": "stalled worker"},
        )

    # The rightful owner's writes still land.
    await store.finish_branch(
        branch_id=reclaimed.branch_id,
        fence=reclaimed.lease_generation,
        status="completed",
        result={"from": "w2"},
    )
    final = await store.get_branch(reclaimed.branch_id)
    assert final.result == {"from": "w2"}


async def test_i5_heartbeat_extends_lease(store_ctx, request):
    store, _, expire = store_ctx
    run_id = _run_id(request)
    await _mk_branch(store, run_id)
    ttl = 0.4
    claimed = await store.claim_next_branch(worker_id="w1", lease_ttl_s=ttl)
    # Heartbeat with a long TTL, then let the *original* TTL pass: the
    # branch must not be claimable because the lease was extended.
    await store.heartbeat(
        branch_id=claimed.branch_id, fence=claimed.lease_generation, lease_ttl_s=120
    )
    await expire(ttl)
    assert await store.claim_next_branch(worker_id="w2", lease_ttl_s=60) is None


# ── I6: durable cancel ───────────────────────────────────────────────


async def test_i6_cancelled_running_branch_never_resumes(store_ctx, request):
    store, _, expire = store_ctx
    run_id = _run_id(request)
    await _mk_branch(store, run_id)
    ttl = 0.2
    claimed = await store.claim_next_branch(worker_id="w1", lease_ttl_s=ttl)

    cancelled = await store.request_cancel(claimed.branch_id)
    assert cancelled.status == "cancelled"

    # The live worker's fence is now stale — its writes abort (I5+I6).
    with pytest.raises(StaleFenceError):
        await store.heartbeat(
            branch_id=claimed.branch_id,
            fence=claimed.lease_generation,
            lease_ttl_s=ttl,
        )

    # Simulated restart: reconciliation must not revive it, and it must
    # never be claimable again — even after every lease has expired.
    await expire(ttl)
    revived = await store.reconcile_on_boot()
    assert cancelled.branch_id not in {r.branch_id for r in revived}
    assert await store.claim_next_branch(worker_id="w2", lease_ttl_s=30) is None
    final = await store.get_branch(claimed.branch_id)
    assert final.status == "cancelled"


# ── I2: boot reconciliation ──────────────────────────────────────────


async def test_i2_reconcile_requeues_expired_running_rows(store_ctx, request):
    store, _, expire = store_ctx
    run_id = _run_id(request)
    await _mk_branch(store, run_id, 0)
    await _mk_branch(store, run_id, 1)
    ttl = 0.2
    dead = await store.claim_next_branch(worker_id="crashed", lease_ttl_s=ttl)
    live = await store.claim_next_branch(worker_id="alive", lease_ttl_s=120)
    await expire(ttl)

    requeued = await store.reconcile_on_boot()
    requeued_ids = {r.branch_id for r in requeued}
    assert dead.branch_id in requeued_ids
    assert live.branch_id not in requeued_ids  # live lease untouched

    # I2: nothing is left `running` without a live lease.
    for row in await store.list_branches(run_id=run_id):
        if row.status == "running":
            assert row.lease_expires_at is not None
            assert row.branch_id == live.branch_id

    # The requeued branch is claimable with a strictly greater fence.
    reclaimed = await store.claim_next_branch(worker_id="w2", lease_ttl_s=60)
    assert reclaimed is not None
    assert reclaimed.branch_id == dead.branch_id
    assert reclaimed.lease_generation == dead.lease_generation + 1


# ── I7: event log ────────────────────────────────────────────────────


async def test_i7_event_seq_monotonic_and_gapless(store_ctx, request):
    store, _, _ = store_ctx
    run_id = _run_id(request)
    n = 25
    for i in range(n):
        event = await store.append_event(
            run_id=run_id,
            event_type="state-update",
            payload={"thread_id": run_id, "i": i},
        )
        assert event.seq == i + 1

    events = await store.list_events(run_id=run_id)
    seqs = [e.seq for e in events]
    assert seqs == list(range(1, n + 1))  # monotonic, no gaps, no dups

    tail = await store.list_events(run_id=run_id, after_seq=20)
    assert [e.seq for e in tail] == [21, 22, 23, 24, 25]


async def test_worker_registry_lifecycle(store_ctx, request):
    store, clock, expire = store_ctx
    run_id = _run_id(request)
    worker_id = f"{run_id}-worker"

    registered = await store.register_worker(
        worker_id=worker_id, pid=4242, hostname="host-a"
    )
    assert registered.pid == 4242

    await expire(0.0)  # let time pass so last_seen visibly moves
    await store.touch_worker(worker_id)
    touched = await store.get_worker(worker_id)
    assert touched.last_seen >= registered.last_seen

    # Re-register (restart with same identity) updates pid, keeps row.
    rereg = await store.register_worker(
        worker_id=worker_id, pid=4243, hostname="host-a"
    )
    assert rereg.pid == 4243
    assert any(w.worker_id == worker_id for w in await store.list_workers())

    await store.remove_worker(worker_id)
    assert await store.get_worker(worker_id) is None


async def test_i7_event_seq_gapless_under_concurrent_appends(store_ctx, request):
    store, _, _ = store_ctx
    run_id = _run_id(request)
    n = 20
    await asyncio.gather(
        *[
            store.append_event(
                run_id=run_id,
                event_type="state-update",
                payload={"thread_id": run_id, "i": i},
            )
            for i in range(n)
        ]
    )
    events = await store.list_events(run_id=run_id)
    assert [e.seq for e in events] == list(range(1, n + 1))
