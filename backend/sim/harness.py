"""Seeded, fault-injecting simulator for the durable branch protocol.

Drives simulated workers against the REAL :class:`InMemoryStateStore`
(production code — the same protocol surface Postgres implements) under
a virtual clock. No wall-clock time, no real DB, no LLM calls: a seed
fully determines the execution, so every failure replays exactly.

Faults injected at scheduling points:

- worker crash (process vanishes mid-branch, lease left dangling)
- worker stall past lease expiry (GC pause / suspended laptop), waking
  later still believing it owns its branch
- durable cancel racing live execution
- worker-local clock skew (heartbeat scheduling runs off a skewed clock)
- boot of a fresh worker running ``reconcile_on_boot``
- duplicate + delayed (out-of-order) NOTIFY delivery to the event
  subscriber

Two protocol modes:

- ``unfenced_file`` — models the historical workload append: check-then-
  append with no fence between the check and the write. The simulator
  finds I1 violations here (see ``docs/INVARIANTS.md`` §found-bugs).
- ``fenced_store`` — the shipped fix: iteration records go through the
  store's atomic, fence-guarded ``record_iteration``.

Invariants I1–I7 are asserted after (and during) every run.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.meta_harness.store import (  # noqa: E402
    InMemoryStateStore,
    StaleFenceError,
    TERMINAL_STATUSES,
)

LEGAL_TRANSITIONS = {
    ("created", "running"),
    ("created", "cancelled"),
    ("running", "completed"),
    ("running", "failed"),
    ("running", "cancelled"),
    ("running", "created"),  # boot-reconciliation requeue
    ("running", "running"),  # lease reclaim (fence must increase)
}


class VirtualClock:
    def __init__(self, start: float = 0.0) -> None:
        self._now = start

    def __call__(self) -> float:
        return self._now

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


def _sync(coro: Any) -> Any:
    """Drive an InMemoryStateStore coroutine to completion synchronously.

    The in-memory store never truly awaits, so one ``send`` finishes it.
    Anything that tries a real await would break determinism — fail loud.
    """
    try:
        coro.send(None)
    except StopIteration as stop:
        return stop.value
    raise RuntimeError("store operation attempted a real await inside the simulator")


@dataclass
class SimParams:
    n_workers: int = 2
    n_branches: int = 2
    branch_iters: int = 5
    lease_ttl: float = 10.0
    protocol: str = "fenced_store"  # or "unfenced_file"
    p_crash: float = 0.02
    p_stall: float = 0.04
    p_cancel: float = 0.02
    p_boot: float = 0.05
    p_dup_notify: float = 0.10
    p_delay_notify: float = 0.10
    max_clock_step: float = 4.0
    step_limit: int = 4000


@dataclass
class SimResult:
    seed: int
    violations: list[str] = field(default_factory=list)
    steps: int = 0
    trace: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations


class SimWorker:
    """One simulated worker process following the real worker protocol."""

    def __init__(self, worker_id: str, sim: "Simulator", skew: float) -> None:
        self.worker_id = worker_id
        self.sim = sim
        self.skew = skew  # worker-local clock offset (clock-skew fault)
        self.crashed = False
        self.stalled_until: float | None = None
        # execution state for the currently claimed branch
        self.row = None
        self.fence: int | None = None
        self.position = 0  # iterations completed (from branch checkpoint)
        self.micro: list[str] = []
        self.pending_iter: int | None = None
        self.pending_append: bool = False
        self.next_hb_due: float = 0.0

    # ── helpers ──────────────────────────────────────────────────────

    def local_now(self) -> float:
        return self.sim.clock.now() + self.skew

    def _abort(self) -> None:
        self.row = None
        self.fence = None
        self.micro = []
        self.pending_iter = None
        self.pending_append = False

    def executing(self, branch_id: str) -> bool:
        return (
            not self.crashed
            and self.row is not None
            and self.row.branch_id == branch_id
        )

    # ── one scheduling step ──────────────────────────────────────────

    def step(self) -> None:
        sim = self.sim
        if self.crashed:
            return
        if self.stalled_until is not None:
            if sim.clock.now() < self.stalled_until:
                return  # still stalled; only the clock moves
            self.stalled_until = None

        if self.row is None:
            row = _sync(
                sim.store.claim_next_branch(
                    worker_id=self.worker_id, lease_ttl_s=sim.params.lease_ttl
                )
            )
            if row is None:
                return
            sim.observe(f"{self.worker_id} claimed {row.branch_id} fence={row.lease_generation}")
            self.row = row
            self.fence = row.lease_generation
            self.position = sim.checkpoints.get(row.thread_id)
            if self.position is None:
                # First execution: fork from the parent checkpoint (I4:
                # read parent state, never write it).
                self.position = sim.checkpoints[row.parent_thread_id]
                sim.checkpoints[row.thread_id] = self.position
            self.next_hb_due = self.local_now() + sim.params.lease_ttl / 3.0
            self._plan_next_iteration()
            return

        # Heartbeat when due by the worker's LOCAL clock (skew-sensitive).
        # The real heartbeat is a CONCURRENT task racing the execution
        # task — a woken stalled worker may run its next work step before
        # the heartbeat that would tell it the fence is stale. The
        # scheduler decides which task wins.
        if self.local_now() >= self.next_hb_due and sim.rng.random() < 0.5:
            try:
                _sync(
                    sim.store.heartbeat(
                        branch_id=self.row.branch_id,
                        fence=self.fence,
                        lease_ttl_s=sim.params.lease_ttl,
                    )
                )
                self.next_hb_due = self.local_now() + sim.params.lease_ttl / 3.0
            except StaleFenceError:
                sim.observe(f"{self.worker_id} heartbeat fence stale → abort")
                self._abort()
            return

        if not self.micro:
            self._plan_next_iteration()
            if not self.micro:
                return
        op = self.micro.pop(0)
        getattr(self, f"_op_{op}")()

    def _plan_next_iteration(self) -> None:
        assert self.row is not None
        if self.position >= self.sim.params.branch_iters:
            self.micro = ["finish"]
        elif self.sim.params.protocol == "fenced_store":
            self.micro = ["record", "guarded_append", "emit", "checkpoint"]
        else:  # unfenced_file: check and append are SEPARATE steps
            self.micro = ["read_log", "append_log", "emit", "checkpoint"]

    # ── micro-ops ────────────────────────────────────────────────────

    def _op_record(self) -> None:
        sim = self.sim
        iteration = self.position + 1
        try:
            _sync(
                sim.store.record_iteration(
                    run_id=self.row.run_id,
                    iteration=iteration,
                    candidate=f"cand-{self.row.branch_id}-{iteration}",
                    row={
                        "iteration": iteration,
                        "worker": self.worker_id,
                        "branch": self.row.branch_id,
                    },
                    branch_id=self.row.branch_id,
                    fence=self.fence,
                )
            )
        except StaleFenceError:
            sim.observe(f"{self.worker_id} record fence stale → abort")
            self._abort()

    def _op_guarded_append(self) -> None:
        # The real file append: dedupe-check + write inside ONE sync call
        # with no await between — a single scheduling point here, unlike
        # the historical protocol's separate read/append steps.
        key = (self.row.branch_id, self.position + 1)
        if key not in self.sim.file_log:
            self.sim.file_log.append(key)

    def _op_read_log(self) -> None:
        iteration = self.position + 1
        key = (self.row.branch_id, iteration)
        self.pending_iter = iteration
        self.pending_append = key not in self.sim.file_log

    def _op_append_log(self) -> None:
        # The historical protocol: nothing revalidates ownership between
        # the read-check and this append.
        if self.pending_append:
            self.sim.file_log.append((self.row.branch_id, self.pending_iter))
        self.pending_iter = None
        self.pending_append = False

    def _op_emit(self) -> None:
        sim = self.sim
        event = _sync(
            sim.store.append_event(
                run_id=self.row.run_id,
                event_type="state-update",
                payload={
                    "thread_id": self.row.thread_id,
                    "iteration": self.position + 1,
                },
            )
        )
        sim.notify_queue.append(event.seq)
        if sim.rng.random() < sim.params.p_dup_notify:
            sim.notify_queue.append(event.seq)  # duplicate delivery
        if len(sim.notify_queue) >= 2 and sim.rng.random() < sim.params.p_delay_notify:
            sim.notify_queue[-1], sim.notify_queue[-2] = (
                sim.notify_queue[-2],
                sim.notify_queue[-1],
            )  # out-of-order delivery

    def _op_checkpoint(self) -> None:
        self.position += 1
        current = self.sim.checkpoints.get(self.row.thread_id, 0) or 0
        if self.position < current:
            # DST finding (seed 9270): LangGraph checkpoint writes are
            # NOT fence-guarded, so a woken zombie worker can append a
            # stale checkpoint after the rightful owner moved on. Benign:
            # terminal branches are never resumed (I6), and a requeued
            # branch re-executes idempotently (fenced record dedupes).
            self.sim.observe(
                f"{self.worker_id} ZOMBIE checkpoint write "
                f"{current}→{self.position} on {self.row.thread_id}"
            )
        self.sim.checkpoints[self.row.thread_id] = self.position

    def _op_finish(self) -> None:
        sim = self.sim
        try:
            _sync(
                sim.store.finish_branch(
                    branch_id=self.row.branch_id,
                    fence=self.fence,
                    status="completed",
                    result={"iterations": self.position},
                )
            )
            sim.observe(f"{self.worker_id} completed {self.row.branch_id}")
        except StaleFenceError:
            sim.observe(f"{self.worker_id} finish fence stale → abort")
        self._abort()


class Simulator:
    def __init__(self, seed: int, params: SimParams | None = None) -> None:
        self.seed = seed
        self.params = params or SimParams()
        self.rng = random.Random(seed)
        self.clock = VirtualClock()
        self.store = InMemoryStateStore(clock=self.clock)
        self.checkpoints: dict[str, int] = {}  # thread_id → iterations done
        self.file_log: list[tuple[str, int]] = []  # the modeled jsonl file
        self.notify_queue: list[int] = []  # NOTIFY seqs in delivery order
        self.subscriber_seen: list[int] = []  # seqs the SSE client saw
        self.subscriber_pos = 0  # last seq the client consumed
        self.result = SimResult(seed=seed)
        self.workers: list[SimWorker] = []
        self._worker_counter = 0
        self._prev_rows: dict[str, Any] = {}
        self.cancelled_freeze: dict[str, tuple[int, int]] = {}
        self.run_id = "sim-run"

    # ── bookkeeping ──────────────────────────────────────────────────

    def observe(self, message: str) -> None:
        self.result.trace.append(f"[{self.clock.now():8.2f}] {message}")

    def violation(self, invariant: str, message: str) -> None:
        self.result.violations.append(f"{invariant}: {message}")

    def _spawn_worker(self, *, skew: float = 0.0) -> SimWorker:
        self._worker_counter += 1
        worker = SimWorker(f"w{self._worker_counter}", self, skew)
        self.workers.append(worker)
        return worker

    # ── setup ────────────────────────────────────────────────────────

    def _setup(self) -> None:
        self.checkpoints[self.run_id] = 0  # parent thread checkpoint
        self.parent_snapshot = dict(self.checkpoints)
        for i in range(self.params.n_branches):
            _sync(
                self.store.create_branch(
                    branch_id=f"b{i}",
                    run_id=self.run_id,
                    thread_id=f"{self.run_id}.fork.b{i}",
                    parent_thread_id=self.run_id,
                    parent_checkpoint_id="ckpt-0",
                    mods={},
                )
            )
        for _ in range(self.params.n_workers):
            skew = self.rng.uniform(-1.0, 1.0) if self.rng.random() < 0.3 else 0.0
            self._spawn_worker(skew=skew)
        self._prev_rows = {r.branch_id: r for r in self._rows()}

    def _rows(self):
        return _sync(self.store.list_branches(run_id=self.run_id))

    # ── faults ───────────────────────────────────────────────────────

    def _inject_fault(self) -> None:
        roll = self.rng.random()
        live = [w for w in self.workers if not w.crashed]
        if roll < self.params.p_crash and live:
            worker = self.rng.choice(live)
            worker.crashed = True
            self.observe(f"FAULT crash {worker.worker_id}")
        elif roll < self.params.p_crash + self.params.p_stall and live:
            worker = self.rng.choice(live)
            duration = self.rng.uniform(
                self.params.lease_ttl, self.params.lease_ttl * 3
            )
            worker.stalled_until = self.clock.now() + duration
            self.observe(f"FAULT stall {worker.worker_id} for {duration:.1f}s")
        elif roll < self.params.p_crash + self.params.p_stall + self.params.p_cancel:
            candidates = [r for r in self._rows() if r.status not in TERMINAL_STATUSES]
            if candidates:
                row = self.rng.choice(candidates)
                store_count = len(
                    [
                        r
                        for r in _sync(
                            self.store.list_iterations(run_id=self.run_id)
                        )
                        if r.get("branch") == row.branch_id
                    ]
                )
                self.cancelled_freeze[row.branch_id] = (
                    self.checkpoints.get(row.thread_id, 0) or 0,
                    store_count,
                )
                _sync(self.store.request_cancel(row.branch_id))
                self.observe(f"FAULT cancel {row.branch_id}")
        elif (
            roll
            < self.params.p_crash
            + self.params.p_stall
            + self.params.p_cancel
            + self.params.p_boot
        ):
            requeued = _sync(self.store.reconcile_on_boot())
            worker = self._spawn_worker()
            self.observe(
                f"FAULT boot {worker.worker_id}; reconciled "
                f"{[r.branch_id for r in requeued]}"
            )
            self._check_i2_after_reconcile()

    # ── invariant monitors ───────────────────────────────────────────

    def _check_i2_after_reconcile(self) -> None:
        now = self.clock.now()
        for row in self._rows():
            if row.status == "running" and (
                row.lease_expires_at is None or row.lease_expires_at < now
            ):
                self.violation(
                    "I2",
                    f"after reconcile, {row.branch_id} is running without "
                    f"a live lease (expires={row.lease_expires_at}, now={now})",
                )

    def _check_step_invariants(self) -> None:
        rows = {r.branch_id: r for r in self._rows()}
        # legal transitions + fence monotonicity + cancelled frozen (I6)
        for branch_id, row in rows.items():
            prev = self._prev_rows.get(branch_id)
            if prev is not None:
                if (
                    prev.status != row.status
                    and (prev.status, row.status) not in LEGAL_TRANSITIONS
                ):
                    self.violation(
                        "state-machine",
                        f"illegal transition {prev.status}→{row.status} "
                        f"on {branch_id}",
                    )
                if row.lease_generation < prev.lease_generation:
                    self.violation(
                        "I5", f"fence went backwards on {branch_id}"
                    )
                if prev.status == "cancelled" and row.status != "cancelled":
                    self.violation(
                        "I6", f"cancelled branch {branch_id} left terminal state"
                    )
        # I5: at most one live worker holds the CURRENT fence per branch
        for branch_id, row in rows.items():
            owners = [
                w.worker_id
                for w in self.workers
                if w.executing(branch_id)
                and w.stalled_until is None
                and w.fence == row.lease_generation
                and row.status == "running"
            ]
            if len(owners) > 1:
                self.violation(
                    "I5", f"{len(owners)} workers hold the current fence "
                    f"on {branch_id}: {owners}"
                )
        # I6: no post-cancel progress in the AUTHORITATIVE store log.
        # (The modeled file may still receive the trailing append of an
        # iteration whose fenced record landed pre-cancel — benign, the
        # record was authorized while the fence was valid.)
        if self.params.protocol == "fenced_store" and self.cancelled_freeze:
            per_branch: dict[str, int] = {}
            for r in _sync(self.store.list_iterations(run_id=self.run_id)):
                branch = r.get("branch")
                per_branch[branch] = per_branch.get(branch, 0) + 1
            for branch_id, (_ckpt, store_frozen) in self.cancelled_freeze.items():
                count = per_branch.get(branch_id, 0)
                if count > store_frozen:
                    self.violation(
                        "I6",
                        f"store log grew after cancel on {branch_id} "
                        f"({store_frozen}→{count})",
                    )
        self._prev_rows = rows

    def _drain_subscriber(self) -> None:
        """Model the SSE client: NOTIFY (dup/reordered) only *wakes* it;
        reads always go through ``list_events(after_seq)``."""
        while self.notify_queue:
            self.notify_queue.pop(0)
            fresh = _sync(
                self.store.list_events(
                    run_id=self.run_id, after_seq=self.subscriber_pos
                )
            )
            for event in fresh:
                self.subscriber_seen.append(event.seq)
                self.subscriber_pos = event.seq

    # ── final checks ─────────────────────────────────────────────────

    def _final_checks(self) -> None:
        rows = self._rows()
        # I1 on the modeled file/log: exactly-once per (branch, iteration)
        if len(self.file_log) != len(set(self.file_log)):
            dupes = sorted(
                {k for k in self.file_log if self.file_log.count(k) > 1}
            )
            self.violation("I1", f"duplicate iteration rows: {dupes}")
        # I3: every non-cancelled branch converged to the fault-free result
        for row in rows:
            if row.status == "cancelled":
                continue
            if row.status != "completed":
                self.violation(
                    "I3", f"{row.branch_id} did not converge: {row.status}"
                )
                continue
            expected = set(range(1, self.params.branch_iters + 1))
            got = {it for (b, it) in set(self.file_log) if b == row.branch_id}
            if got != expected:
                self.violation(
                    "I3",
                    f"{row.branch_id} log mismatch: missing "
                    f"{sorted(expected - got)}, extra {sorted(got - expected)}",
                )
            if self.params.protocol == "fenced_store":
                store_got = {
                    r["iteration"]
                    for r in _sync(
                        self.store.list_iterations(run_id=self.run_id)
                    )
                    if r.get("branch") == row.branch_id
                }
                if store_got != expected:
                    self.violation(
                        "I3",
                        f"{row.branch_id} store log mismatch: missing "
                        f"{sorted(expected - store_got)}",
                    )
            # NOTE deliberately NOT asserted: latest-checkpoint equality.
            # A zombie's trailing checkpoint write may land after the
            # branch completes (unfenced by design in LangGraph — DST
            # finding, seed 9270). The authoritative outcome is the store
            # log + terminal status; a requeued branch re-executing from
            # a stale checkpoint converges because records are idempotent.
        # I4: branch execution never mutated the parent thread state
        if self.checkpoints[self.run_id] != self.parent_snapshot[self.run_id]:
            self.violation("I4", "parent thread checkpoint was mutated by a fork")
        # I7: the subscriber saw a gap-free monotonic sequence
        self._drain_subscriber()
        expected_seqs = [
            e.seq for e in _sync(self.store.list_events(run_id=self.run_id))
        ]
        if self.subscriber_seen != expected_seqs:
            self.violation(
                "I7",
                f"subscriber saw {self.subscriber_seen[-5:]}, "
                f"log has {expected_seqs[-5:]}",
            )
        for prev, cur in zip(self.subscriber_seen, self.subscriber_seen[1:]):
            if cur != prev + 1:
                self.violation("I7", f"gap in subscriber sequence: {prev}→{cur}")

    # ── main loop ────────────────────────────────────────────────────

    def run(self) -> SimResult:
        self._setup()
        params = self.params
        steps = 0
        fault_phase = True
        while steps < params.step_limit:
            steps += 1
            rows = self._rows()
            if all(r.status in TERMINAL_STATUSES for r in rows):
                break

            if fault_phase and steps > params.step_limit // 2:
                # Drain phase: stop injecting faults, revive the fleet,
                # let the run converge so I3 is checkable.
                fault_phase = False
                self.clock.advance(params.lease_ttl * 3 + 1)
                _sync(self.store.reconcile_on_boot())
                self._check_i2_after_reconcile()
                if not any(not w.crashed for w in self.workers):
                    self._spawn_worker()
                self.observe("drain phase begins")

            action = self.rng.random()
            if fault_phase and action < 0.15:
                self._inject_fault()
            elif action < 0.35:
                self.clock.advance(self.rng.uniform(0.1, params.max_clock_step))
            else:
                live = [
                    w
                    for w in self.workers
                    if not w.crashed
                ]
                if not live:
                    # everyone is dead: boot a replacement
                    self.clock.advance(params.lease_ttl + 1)
                    _sync(self.store.reconcile_on_boot())
                    self._check_i2_after_reconcile()
                    self._spawn_worker()
                    continue
                worker = self.rng.choice(live)
                worker.step()
            if self.rng.random() < 0.5:
                self._drain_subscriber()
            self._check_step_invariants()

        rows = self._rows()
        if not all(r.status in TERMINAL_STATUSES for r in rows):
            self.violation(
                "liveness",
                f"run did not converge within {params.step_limit} steps: "
                f"{[(r.branch_id, r.status) for r in rows]}",
            )
        self.result.steps = steps
        self._final_checks()
        return self.result


def run_seed(seed: int, params: SimParams | None = None) -> SimResult:
    return Simulator(seed, params).run()
