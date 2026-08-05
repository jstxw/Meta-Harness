"""Hypothesis stateful testing over the branch lifecycle (Phase 3).

Complementary to the simulator: Hypothesis searches for bad *sequences*
of lifecycle operations (fork/claim/heartbeat/cancel/expire/reconcile)
with automatic shrinking to a minimal reproducer; the simulator searches
bad *interleavings* under a fault schedule.

Runs entirely against the deterministic in-memory store — no Postgres.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    invariant,
    precondition,
    rule,
)

from app.meta_harness.store import (
    InMemoryStateStore,
    StaleFenceError,
    TERMINAL_STATUSES,
)
from sim.harness import LEGAL_TRANSITIONS, VirtualClock, _sync

LEASE_TTL = 10.0


class BranchLifecycleMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.clock = VirtualClock()
        self.store = InMemoryStateStore(clock=self.clock)
        self.branch_ids: list[str] = []
        self.claims: list[dict] = []  # live fences we've been handed
        self.statuses: dict[str, str] = {}
        self.counter = 0

    # ── rules ────────────────────────────────────────────────────────

    @rule()
    def create_branch(self) -> None:
        self.counter += 1
        branch_id = f"b{self.counter}"
        _sync(
            self.store.create_branch(
                branch_id=branch_id,
                run_id="hyp-run",
                thread_id=f"hyp-run.fork.{branch_id}",
                parent_thread_id="hyp-run",
                parent_checkpoint_id="ckpt-0",
                mods={},
            )
        )
        self.branch_ids.append(branch_id)

    @rule(worker=st.sampled_from(["w1", "w2", "w3"]))
    def claim(self, worker: str) -> None:
        row = _sync(
            self.store.claim_next_branch(worker_id=worker, lease_ttl_s=LEASE_TTL)
        )
        if row is not None:
            self.claims.append(
                {
                    "branch_id": row.branch_id,
                    "fence": row.lease_generation,
                    "worker": worker,
                }
            )

    @precondition(lambda self: self.claims)
    @rule(data=st.data())
    def heartbeat_with_some_fence(self, data) -> None:
        claim = data.draw(st.sampled_from(self.claims))
        try:
            _sync(
                self.store.heartbeat(
                    branch_id=claim["branch_id"],
                    fence=claim["fence"],
                    lease_ttl_s=LEASE_TTL,
                )
            )
        except StaleFenceError:
            self.claims.remove(claim)  # stale fence learned; abort

    @precondition(lambda self: self.claims)
    @rule(data=st.data(), status=st.sampled_from(["completed", "failed"]))
    def finish_with_some_fence(self, data, status: str) -> None:
        claim = data.draw(st.sampled_from(self.claims))
        try:
            _sync(
                self.store.finish_branch(
                    branch_id=claim["branch_id"],
                    fence=claim["fence"],
                    status=status,
                )
            )
            self.claims.remove(claim)
        except StaleFenceError:
            self.claims.remove(claim)

    @precondition(lambda self: self.branch_ids)
    @rule(data=st.data())
    def cancel(self, data) -> None:
        branch_id = data.draw(st.sampled_from(self.branch_ids))
        _sync(self.store.request_cancel(branch_id))

    @rule(seconds=st.floats(min_value=0.1, max_value=3 * LEASE_TTL))
    def advance_clock(self, seconds: float) -> None:
        self.clock.advance(seconds)

    @rule()
    def reconcile(self) -> None:
        _sync(self.store.reconcile_on_boot())
        now = self.clock.now()
        for row in _sync(self.store.list_branches()):
            if row.status == "running":
                assert row.lease_expires_at is not None
                assert row.lease_expires_at >= now, (
                    f"I2: {row.branch_id} running with expired lease "
                    "after reconcile"
                )

    # ── invariants ───────────────────────────────────────────────────

    @invariant()
    def transitions_are_legal_and_terminal_is_final(self) -> None:
        for row in _sync(self.store.list_branches()):
            prev = self.statuses.get(row.branch_id)
            if prev is not None and prev != row.status:
                assert (prev, row.status) in LEGAL_TRANSITIONS, (
                    f"illegal transition {prev}→{row.status} on {row.branch_id}"
                )
                assert prev not in TERMINAL_STATUSES, (
                    f"terminal branch {row.branch_id} transitioned "
                    f"{prev}→{row.status}"
                )
            self.statuses[row.branch_id] = row.status

    @invariant()
    def i5_at_most_one_current_fence(self) -> None:
        rows = {r.branch_id: r for r in _sync(self.store.list_branches())}
        for branch_id, row in rows.items():
            holders = [
                c
                for c in self.claims
                if c["branch_id"] == branch_id
                and c["fence"] == row.lease_generation
                and row.status == "running"
            ]
            assert len(holders) <= 1, (
                f"I5: multiple current-fence holders on {branch_id}: {holders}"
            )

    @invariant()
    def fences_never_regress(self) -> None:
        for row in _sync(self.store.list_branches()):
            for claim in self.claims:
                if claim["branch_id"] == row.branch_id:
                    assert row.lease_generation >= claim["fence"]


BranchLifecycleMachine.TestCase.settings = settings(
    max_examples=200, stateful_step_count=50, deadline=None
)
TestBranchLifecycle = BranchLifecycleMachine.TestCase
