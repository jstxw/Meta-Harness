# INVARIANTS.md — the runtime spec

This is the contract the durable execution runtime is verified against.
Every test that asserts one of these carries the invariant ID in its
name (`test_i1_…`), and the Phase 3 simulator asserts all of them after
every simulated run. If a behavior isn't captured here, it isn't
guaranteed.

---

## Invariants

| ID | Name | Statement |
|---|---|---|
| **I1** | No double execution | No iteration number appears twice in `evolution_summary.jsonl` across any crash/resume sequence. |
| **I2** | No orphans | After boot reconciliation, no branch row remains `running` without a live lease. |
| **I3** | Resume convergence | Crash at any point + resume yields the same final state as an uninterrupted run with the same seed. |
| **I4** | Fork isolation | A branch's writes never mutate parent thread state. |
| **I5** | Lease safety | At most one worker executes a given branch at a time. |
| **I6** | Durable cancel | A cancelled branch never resumes after restart. |
| **I7** | Event integrity | Per-run SSE sequence numbers are monotonic with no gaps for a continuously connected client. |

---

## Branch state machine

Legal transitions — anything not listed is a bug:

```
created → running → completed
                  → failed
                  → cancelled
created → cancelled
running → running        (lease reclaimed after expiry — only with a NEW fence)
running → created        (boot reconciliation requeues an expired-lease orphan)
```

Terminal states: `completed`, `failed`, `cancelled`. A terminal row never
transitions again, regardless of fence.

---

## The fencing-token problem (why I5 is the hard one)

`SELECT … FOR UPDATE SKIP LOCKED` plus a lease TTL gives at-most-one
execution **only while the lease is valid**. The failure mode leases
cannot close on their own:

1. Worker A claims branch B, lease expires at T.
2. A stalls past T — GC pause, swap, suspended laptop. A does not know
   it stalled.
3. The reaper (or another claimer) sees the expired lease and hands B to
   worker W2.
4. A wakes up, still believing it owns B, and writes.

Two workers are now executing the same branch; I5 is false. TTLs bound
how long a lease is *held*, not how long a stalled process *believes* it
holds one.

**The fix is a fencing token.** `branch_runs.lease_generation` is a
monotonic counter:

- Every successful claim increments `lease_generation`; the claimer
  receives the new value as its fence.
- Every write a worker makes carries its fence:
  `… AND lease_generation = %(fence)s`. Zero rows updated ⇒ the fence is
  stale ⇒ the worker was reclaimed. It must abort — not retry.
- Heartbeats extend `lease_expires_at` under the same guard and must
  **fail loudly** on fence mismatch, so a stalled-then-woken worker
  learns it lost ownership at the next heartbeat, before it can do
  further damage.

With fencing, the stalled worker's late writes are rejected by the
store rather than trusted, and the `running → running` transition is
safe because the new claimant always holds a strictly greater fence.

---

## Test coverage map

Existing tests that already assert an invariant are named for it:

| Invariant | Test |
|---|---|
| I1 | `backend/tests/test_persistence.py::test_i1_resume_completes_remaining_iterations_no_duplicates` (cancel mid-run + resume → no duplicate iterations) |
| I3 | `backend/tests/test_persistence.py::test_i3_checkpoints_persist_in_postgres` (checkpoint history is complete enough to replay) |
| I4 | `backend/tests/test_branches.py::test_i4_worktree_add_creates_branch_and_applies_mods` (parent checkpoint state unchanged after fork runs) |
| I6 | `backend/tests/test_branches.py::test_cancel_branch_marks_running_task_cancelled` (in-process half of I6; the durable half lands with Phase 2) |
| I7 | `backend/tests/test_streaming.py` (closed event set; sequence numbers land with Phase 2's LISTEN/NOTIFY fanout) |

I2 and I5 are covered by the `StateStore` contract tests in
`backend/tests/test_store.py` (both implementations) and continuously by
the simulator. The kill-9 end-to-end test in
`backend/tests/test_worker_recovery.py` covers I1/I3/I5 against real
worker processes. The Phase 3 simulator (`backend/sim/`) asserts I1–I7
after every seeded run:

```bash
cd backend && uv run python -m sim.run --seeds 10000        # 0 failures
cd backend && uv run python -m sim.run --seed 7 --mode unfenced_file -v   # replay a found bug
```

---

## Bugs found by deterministic simulation

Both are reproducible from their seed; the seed fully determines the
schedule, faults, and interleaving.

### DST-1 — double append past a stale fence (`unfenced_file`, seed 7)

The historical protocol appended iteration rows to
`evolution_summary.jsonl` with a read-check but **no fence at the data
layer**. Interleaving found at seed 7 (also 22, 46, 69 …):

1. Worker A passes the dedupe read-check for iteration *k*.
2. A stalls past lease expiry (GC pause / suspend). Worker B reclaims
   the branch with a new fence and re-executes iteration *k* — check,
   append.
3. A wakes and its next scheduled step is the append — its heartbeat
   task, which would have detected the stale fence, hasn't fired yet.
   Duplicate row: **I1 violated.**

**Fix (shipped):** `StateStore.record_iteration` — an atomic,
fence-guarded, exactly-once record (`INSERT … WHERE fence valid … ON
CONFLICT DO NOTHING` in Postgres). The worker's iteration recorder runs
before the file append; a reclaimed worker gets `StaleFenceError` from
the data layer itself. 10,000 seeds pass with this protocol; the file is
a best-effort projection.

### DST-2 — zombie checkpoint write after completion (seed 9270)

LangGraph checkpoint writes are **not** fence-guarded. A stalled
worker that wakes after the rightful owner completed the branch can
append one stale trailing checkpoint to the branch thread's history.

Impact assessment (why this is documented rather than "fixed"): a
terminal branch is never resumed (I6), so nothing reads the stale
checkpoint; a *requeued* branch resuming from a stale checkpoint
re-executes iterations idempotently — the fenced record dedupes — and
converges (I3). The residual effect is thread-history pollution bounded
to one in-flight write. A full fix would require fencing inside the
checkpointer itself.
