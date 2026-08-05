# Meta-Harness → Durable Execution Runtime

**Working document for Claude Code / agent sessions. Read this fully before writing code.**

---

## 0. What this project is now

**Old framing:** a self-improving agent harness that searches over harness source code and improves benchmark accuracy.

**New framing:** *a durable execution runtime for long-horizon agent workflows — checkpointed, forkable, crash-recoverable — verified by deterministic simulation testing.*

The harness search is no longer the thesis. It is now the **reference workload** that stresses the runtime. It has to *run*, not *win*.

### What changes as a consequence

| Concern | Old status | New status |
|---|---|---|
| Benchmark accuracy improvement | The headline claim | Not a claim at all |
| Harbor / better eval substrate | Blocking priority | **Out of scope** |
| Mock-bench synthetic score arc | Dishonest liability | **Asset** — fast deterministic fixture |
| Real token/cost accounting | Needed for Pareto story | Optional, low priority |
| Crash recovery / branch durability | Nice-to-have | **The product** |
| Simulation testing | Didn't exist | **The differentiator** |

### Component triage

- **Promote to core:** `AsyncPostgresSaver` checkpointing, `branches.py` fork semantics, `resume_outer_loop`, SSE streaming, D3 trajectory tree (now an observability UI, not a demo toy).
- **Demote to workload:** `proposer.py`, the 11 override points, `SKILL.md`, Pareto frontier, the 5 search + 2 holdout eval tasks. All still work. None are the claim.
- **Delete:** the "62% → 85%" figure everywhere it appears (README, resume, docs). It is a hardcoded mock constant, not a measurement.

---

## 1. Ground truth about the current repo

Verify each of these before acting on them — `docs/PROJECT_STATUS.md` is stamped 2026-04-26 and is known to be partly stale.

**Confirmed in source:**
- `backend/app/meta_harness/branches.py` holds `branch_registry: dict[str, asyncio.Task]` and `branch_metadata: dict[str, BranchMetadata]` — **in-process only, lost on restart.**
- Fork thread naming: `f"{parent_thread_id}.fork.{branch_id}"` where `branch_id = uuid.uuid4().hex[:8]`.
- Forks use `asyncio.create_task`, not `gather`, so each is independently cancellable. Keep this property.
- Resume works via `graph.ainvoke(None, config={"configurable": {"thread_id": run_dir.name}})` — `None` input + existing thread = resume from last checkpoint.
- `backend/tests/test_memory.py` calls `healthcheck()` at **module import time** through a hand-constructed event loop. Fragile; misreports Postgres as unreachable under some loop policies.
- `backend/tests/test_streaming.py` hand-rolls an `async_test` decorator to avoid depending on pytest-asyncio.
- `backend/app/streaming.py` exposes a closed set of 11 SSE event types. `EventRegistry` is **in-process**.
- `backend/tests/test_api.py` asserts `GET /runs/{id}/candidates/{name}/diff` and `.../test-output` return 200 with real content.

**Known-stale doc claims — re-verify, do not trust:**
- "No diff/test-output endpoint exists on the backend" — contradicted by `test_api.py` above. The gap is probably only that `frontend/dashboard/src/lib/api.ts` `getDiff()`/`getTestOutput()` still return `null`. Check before building anything.
- Test counts (31 passed / 21 skipped / 3 failed / 31 errors) — re-run before believing.

**Known synthetic values:**
- Mock-bench accuracy is `min(0.95, 0.60 + 0.20 * (iteration - 1))`. It produces the same curve regardless of what the proposer emits.
- Real-bench path writes literal zeros for `tokens` and `cost_usd`.
- `MemoryPanel.tsx` has ~3 hardcoded fixture patterns.

---

## 2. Invariants (the spec)

Everything in Phase 3 asserts these. Write them into `docs/INVARIANTS.md` and name each test after its invariant ID.

| ID | Invariant |
|---|---|
| **I1** | **No double execution.** No iteration number appears twice in `evolution_summary.jsonl` across any crash/resume sequence. |
| **I2** | **No orphans.** After boot reconciliation, no branch row remains `running` without a live lease. |
| **I3** | **Resume convergence.** Crash at any point + resume yields the same final state as an uninterrupted run with the same seed. |
| **I4** | **Fork isolation.** A branch's writes never mutate parent thread state. |
| **I5** | **Lease safety.** At most one worker executes a given branch at a time. |
| **I6** | **Durable cancel.** A cancelled branch never resumes after restart. |
| **I7** | **Event integrity.** Per-run SSE sequence numbers are monotonic with no gaps for a continuously connected client. |

Legal branch state transitions:

```
created → running → completed
                  → failed
                  → cancelled
created → cancelled
running → running        (lease reclaimed after expiry — only with a NEW fence)
```

### I5 is the hard one — read this before implementing

`SELECT … FOR UPDATE SKIP LOCKED` plus a lease gives at-most-one **only while the lease is valid**. A worker that stalls past expiry (GC pause, swap, suspended laptop) wakes up believing it still owns the branch while a second worker has already claimed it.

**You must implement fencing tokens.** Every claim increments a monotonic `lease_generation`. Every write carries the fence the worker holds. Writes with a stale fence are rejected. Without this, I5 is false and the simulator will find it.

---

## 3. Phases

Phases 0 → 1 → 2 → 3 are **strictly ordered**. Phase 4 (frontend) depends only on Phase 2 for its core items, so 4.0 and 4.1 may run as soon as durable branches exist. Phase 5 (sandbox) is independent, gated, and may slip entirely.

### Phase 0 — Unblock (target: 1 weekend)

Nothing else is possible until the thing runs.

- [ ] Move the backend to WSL2 or a devcontainer. This kills the Unix `/tmp` assumptions, the temp/cache permission failures, and the sandbox portability problem in one move. **Do not fix these individually on Windows.**
- [ ] Bring up Postgres: `docker compose -f infra/docker-compose.yml up -d postgres`. The ~21 skipped tests are the checkpointing/memory/fork tests — the parts this project is now about.
- [ ] Fix `backend/tests/test_memory.py`: replace the module-level `healthcheck()` call with a session-scoped fixture.
- [ ] Set `asyncio_mode = "auto"` under `[tool.pytest.ini_options]` in `backend/pyproject.toml`; delete the hand-rolled `async_test` decorator in `test_streaming.py`.
- [ ] Run one full mock-bench loop end to end and watch it complete.
- [ ] Rewrite `docs/PROJECT_STATUS.md` with real current numbers.

**Exit criteria:** suite green *with Postgres-backed tests actually executing* (not skipping), and one complete mock-bench loop.

**Do not start Phase 2 until this passes.** The entire risk of this plan is building verification machinery around a workload nobody has watched run.

---

### Phase 1 — Write the spec (target: half a day)

- [ ] `docs/INVARIANTS.md` — the table in §2, plus the state machine, plus a prose note on the fencing-token problem.
- [ ] Name existing tests after the invariants they already cover (the SIGINT/resume test in `test_persistence.py` is an I1 test).

TLA+/Alloy are optional flex. A precise markdown spec captures most of the value.

---

### Phase 2 — Durable branches (target: ~1 week)

**Architectural constraint that must land in this phase, not later:** put all state access behind a `StateStore` protocol with two implementations — real Postgres, and an in-memory deterministic fake. If Phase 2 is written directly against `psycopg`, **Phase 3 is impossible without a rewrite**, because a real database cannot be made deterministic. This is the single most important sequencing decision in the plan.

- [ ] Define `StateStore` protocol in `backend/app/meta_harness/store.py`. Two impls: `PostgresStateStore`, `InMemoryStateStore`.
- [ ] Add `branch_runs` table:

```sql
CREATE TABLE branch_runs (
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
CREATE INDEX ON branch_runs (status, lease_expires_at);
CREATE INDEX ON branch_runs (run_id);
```

- [ ] Claiming query:

```sql
UPDATE branch_runs SET
    status           = 'running',
    lease_owner      = %(worker_id)s,
    lease_generation = lease_generation + 1,
    lease_expires_at = now() + %(lease_ttl)s,
    started_at       = COALESCE(started_at, now())
WHERE branch_id = (
    SELECT branch_id FROM branch_runs
    WHERE status = 'created'
       OR (status = 'running' AND lease_expires_at < now())
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING branch_id, thread_id, parent_checkpoint_id, mods, lease_generation;
```

- [ ] Heartbeat loop extends `lease_expires_at` while work is in flight. Heartbeat must **fail loudly** if the row's `lease_generation` no longer matches the fence this worker holds — that means it was reclaimed and this worker must abort.
- [ ] Every status write carries `AND lease_generation = %(fence)s`. Zero rows updated ⇒ stale fence ⇒ abort, do not retry.
- [ ] Boot reconciliation: on startup, sweep `running` rows with expired leases → requeue from last checkpoint, or mark `failed`.
- [ ] Split the worker process from the API process. Keep `asyncio.create_task` semantics *inside* a worker for independent cancellation.
- [ ] Move SSE fanout to Postgres `LISTEN/NOTIFY` on a per-run channel. **The moment there are two processes, the in-process `EventRegistry` stops delivering worker events to browsers connected to the API process.** Add a monotonic per-run sequence number to satisfy I7.
- [ ] Keep the existing 11-event closed set. Do not add event types in this phase.

**Exit criteria:** kill `-9` a worker mid-branch; another worker picks it up from its last checkpoint; no duplicate iterations in `evolution_summary.jsonl`.

---

### Phase 3 — Deterministic simulation testing (target: ~1–2 weeks)

The differentiator. Nobody else's portfolio project has this.

- [ ] `backend/sim/` — seeded scheduler driving the orchestrator on `InMemoryStateStore`, with stubbed trial execution returning seeded results. No real LLM calls, no real DB, no wall-clock time.
- [ ] Virtual clock. All timeouts and lease expiries read from it, never from `time.monotonic()` directly. Audit for stray real-clock reads — one is enough to destroy determinism.
- [ ] Fault injection at every scheduling point: process crash, lease expiry mid-work, worker stall past lease, clock skew, duplicate delivery, out-of-order `NOTIFY`.
- [ ] Assert I1–I7 after every simulated run.
- [ ] **Print the seed on failure.** A DST suite that can't replay its failures is just a flaky test suite.
- [ ] Layer `hypothesis.stateful.RuleBasedStateMachine` over the branch lifecycle: random sequences of fork/cancel/crash/resume with automatic shrinking to a minimal reproducer.

The two techniques are complementary: Hypothesis finds bad **sequences**, the simulator finds bad **interleavings**.

**Exit criteria:** 10k seeds pass, **and at least one real bug found and documented with its seed.** The second half matters more than the first. "DST found a fencing-token race at seed 4471" is the story worth telling.

---

### Phase 4 — Observability frontend (target: ~1 week; 4.0 and 4.1 may run right after Phase 2)

**The UI's job is no longer to show accuracy going up. It is to show state surviving failure.** A durable execution runtime nobody can watch recover is a hard sell, so this phase is what makes Phases 2–3 legible.

Ordered by value. 4.0 is a prerequisite for honesty; 4.1 and 4.2 are the two that carry the project.

#### 4.0 — Remove the lying fallback (do this first, ~1 hour)

- [ ] **Delete the dashboard's fallback to a mock demo run when the backend is unreachable.** For a project whose entire claim is "watch state survive failure," a UI that fabricates a healthy run when nothing is running behind it makes the demo unfalsifiable — the opposite of what is being sold. Replace with an explicit disconnected state.
- [ ] Wire `getDiff()` / `getTestOutput()` in `frontend/dashboard/src/lib/api.ts`. The backend endpoints exist and are asserted in `backend/tests/test_api.py`; the client functions return `null`, so the UI silently renders nothing where diffs belong. Verify first — this is likely a one-line fix, not a feature.
- [ ] Label all mock-bench-derived values in the UI as fixtures.

#### 4.1 — Chaos button (the demo)

A control that kills a worker mid-branch, so the recovery path is visible live over SSE:

- [ ] `POST /debug/kill-worker/{worker_id}` (dev-only, gated behind an env flag).
- [ ] Render the sequence in the trajectory tree as it happens: lease goes stale → reaper marks expired → another worker claims with incremented fence → execution resumes from last checkpoint.
- [ ] Surface the fence generation increment explicitly — that's the mechanism, and it's the part worth pointing at.

This is the entire thesis as a ~15-second visual, with no accuracy number and nothing that can't be backed. **If only one item in Phase 4 gets built, build this one.** It depends only on Phase 2 — do not hold it behind the sandbox.

#### 4.2 — Seed replay viewer (the rare artifact)

DST runs are deterministic, so a seed plus the invariant spec fully determines the timeline.

- [ ] Input a seed → scrubber over the simulated event sequence.
- [ ] Mark injected faults (crash, lease expiry, clock skew, duplicate delivery) on the timeline.
- [ ] Highlight the invariant violation at the exact step it occurs, labeled with its ID (I1–I7).
- [ ] Pure function of the seed: no backend, no DB, no LLM calls. Works in a static deploy and cannot break during a demo.

#### 4.3 — Lease/lifecycle view (cheap, layout already exists)

- [ ] Extend the existing D3 branch-lineage tree: node color by status, live vs. expired lease indicator, `lease_generation` on hover, orphaned-on-boot marked distinctly.

#### 4.4 — Fork-from-checkpoint as an interaction

- [ ] Click a checkpoint in the trajectory → edit `mods` in a small JSON editor → spawn a branch. `worktree_add` already exists on the backend; this exposes it.

#### 4.5 — Sandbox visibility (optional; gated on Phase 5 landing)

Not required — Phase 4's exit criteria does not involve the sandbox. Build only if Phase 5 succeeds.

- [ ] **Isolation badge per trial** — wasm, Docker, or bare subprocess. Since Phase 5 is gated and may land as Docker, showing the boundary makes the UI tell the truth about what actually shipped.
- [ ] Sandbox lifecycle as timed events in the trajectory (instantiation → execution → teardown). Startup latency is the whole argument for wasm over Docker, so the milliseconds are the evidence.
- [ ] **Denied capability attempts** — a candidate harness trying to open a file it wasn't granted, surfaced live. This is the one thing Docker can't demonstrate as cleanly, so it only exists if the wasm path works.

#### Demote

- [ ] Pareto chart: real token accounting is optional in this plan, so the x-axis stays fake. Either wire real `response.usage` aggregation or cut the chart. Do not ship it with a constant-zero axis.
- [ ] `MemoryPanel.tsx` hardcoded fixture patterns: wire to the real store or remove.

**Exit criteria:** a cold observer can watch a worker die and the branch recover, without narration.

---

### Phase 5 — wasmtime sandbox (target: ~1 week, gated, may slip)

Capability-based isolation, millisecond startup, deterministic execution. **Moved last deliberately:** this is the phase most likely to slip, and nothing else depends on it.

- [ ] **Timebox the pytest-under-WASM spike to two days.** Python-in-WASM via Pyodide or `componentize-py` is genuinely fiddly and your eval tasks run pytest.
- [ ] If the spike fails: **take Docker-per-trial instead** (`docker` Python SDK) and keep the WASM reasoning as an interview answer. You lose little — Phase 3 already delivered determinism where it matters.
- [ ] Either way, this replaces subprocess isolation, which is not isolation — it's the same trust boundary with extra steps, and it's the root cause of the `/tmp` failures.
- [ ] If this lands as wasm rather than Docker, 4.5 becomes worth building.

---

## 4. Explicit non-goals

Do not do these, even if they seem adjacent:

- Harbor / Terminal-Bench migration
- Partial-credit scoring or any eval-signal work
- Chasing an accuracy number, calibrating the score arc, or hitting ±5%
- Temporal, Celery, RQ, Kafka, Kubernetes, gRPC, or a second database — each adds a name and no new idea, and the whole point is that *you* implemented the durable-execution mechanics
- Rewriting anything in Rust
- New SSE event types
- Real token accounting (optional, low priority, do it last if at all)

---

## 5. Honesty rules

This project previously shipped a synthetic number as a result. Do not repeat that.

- Any number in the README, resume, or docs must be reproducible by a command written next to it.
- Mock-bench output must be labeled as a fixture everywhere it surfaces in the UI.
- Do not claim "I built durable execution" without qualification. LangGraph provides single-thread checkpoint persistence and resume. **The honest, stronger claim:** LangGraph gives single-thread checkpointing; this project adds branch lifecycle as durable state, lease-based claiming with fencing tokens across processes, crash reconciliation on boot, and fork-from-arbitrary-checkpoint with state mutation. That distinction survives questioning; the vague version does not.
- If a phase's exit criteria isn't met, say so in `PROJECT_STATUS.md` rather than marking it complete.

---

## 6. Verification commands

```bash
# environment + suite
cd backend && python -m pytest tests -q
docker compose -f infra/docker-compose.yml up -d postgres

# confirm Postgres tests actually run rather than skip
cd backend && python -m pytest tests -q -rs | grep -i skip

# stale-doc checks from §1
grep -rn "getDiff\|getTestOutput" frontend/dashboard/src/lib/api.ts
grep -n "cost_usd\|total_tokens" backend/app/meta_harness/outer.py
grep -rn "0.60\|0.20\|62\|85" docs/ README.md

# crash-recovery smoke (Phase 2 exit)
# start a run, kill -9 the worker mid-branch, confirm pickup + no duplicate iterations
cat runs/<run>/evolution_summary.jsonl | jq -r .iteration | sort | uniq -d   # must be empty

# DST (Phase 3)
cd backend && python -m pytest sim -q
cd backend && python -m sim.run --seeds 10000
```

---

## 7. Naming

`Meta-Harness` advertises the demoted part. Consider renaming once Phase 2 lands — the repo name is currently doing positioning work against the project.
