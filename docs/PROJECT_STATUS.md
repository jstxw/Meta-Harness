# Project Status — Meta-Harness

> Last updated: 2026-08-05 (macOS workspace, `durable-runtime` branch)

**Framing:** this project is a *durable execution runtime for long-horizon
agent workflows* — checkpointed, forkable, crash-recoverable — verified by
deterministic simulation testing. The self-improving harness search is the
reference workload that stresses the runtime, not the thesis. See
`documents/REPOSITIONING_PLAN.md` for the full plan and
`docs/INVARIANTS.md` for the spec the runtime is verified against.

Every number in this file is reproducible by the command printed next to it.

---

## Verified snapshot (2026-08-05)

Environment: macOS (Darwin), Python via `uv`, Postgres 16 in Docker.

```bash
docker compose -f infra/docker-compose.yml up -d postgres
cd backend && uv run pytest tests -q
```

Result: **92 passed, 1 skipped in ~6s.**

The single skip is `tests/test_inner.py` (live-LLM smoke test) when
`ANTHROPIC_API_KEY` is not set. All Postgres-backed tests (checkpointing,
memory, forks) **execute** — they no longer skip, and no test performs a
module-import-time healthcheck (replaced by the session-scoped
`postgres_available` fixture in `backend/tests/conftest.py`).

```bash
cd backend && uv run meta-harness loop --proposer mock --mock-bench --budget 2 --fresh
```

Result: completes in seconds with `"iterations_completed": 2`,
`"persistent": true`, and an `evolution_summary.jsonl` with no duplicate
iteration numbers:

```bash
cat runs/<run>/evolution_summary.jsonl | jq -r .iteration | sort | uniq -d   # empty
```

**Phase 0 exit criteria met:** suite green with Postgres tests executing,
one complete mock-bench loop observed end to end.

---

## Known synthetic values (do not present as results)

- Mock-bench accuracy is `min(0.95, 0.60 + 0.20 * (iteration - 1))` — a
  deterministic fixture, useful for testing, meaningless as a measurement.
  The old "62% → 85%" demo arc derives from this constant and must not be
  quoted as a result anywhere.
- Real-bench path writes literal zeros for `tokens` and `cost_usd`.
- `MemoryPanel.tsx` contains hardcoded fixture patterns.

---

## Phase status (per `documents/REPOSITIONING_PLAN.md`)

| Phase | What | Status |
|---|---|---|
| 0 | Unblock: Postgres up, suite green, mock-bench loop runs | ✅ Complete (evidence above) |
| 1 | `docs/INVARIANTS.md` spec, tests named after invariants | ✅ Complete |
| 2 | Durable branches: `StateStore`, `branch_runs`, leases + fencing tokens, boot reconciliation, worker/API split, LISTEN/NOTIFY SSE | ✅ Complete — exit criterion verified by `tests/test_worker_recovery.py`: worker SIGKILLed mid-branch, second worker reclaims with fence 2, no duplicate iterations |
| 3 | Deterministic simulation testing (`backend/sim/`) + Hypothesis stateful | ✅ Complete — `uv run python -m sim.run --seeds 10000` → 0 failures; two real bugs found and documented with seeds (DST-1 seed 7, DST-2 seed 9270 — see `docs/INVARIANTS.md`) |
| 4 | Observability frontend (4.0 honesty fixes → 4.1 chaos button → 4.2 seed replay) | 4.0 ✅ Complete (demo-run fallback deleted, explicit disconnected state, fixtures labeled). 4.1 ✅ Complete: `POST /debug/kill-worker/{id}` gated by `META_HARNESS_CHAOS=1`, worker registry in the store, "chaos" dashboard tab with per-worker kill -9 buttons, live branch-lease view (status, owner, lease countdown, **fence generation badge**) and a fence-increment log — kill a worker, watch gen N → N+1 as another claims. 4.3 ✅ (folded into the same panel). 4.2 (seed replay viewer), 4.4 (fork UI existed pre-plan), 4.5 (gated on Phase 5) remain |
| 5 | wasmtime sandbox (gated; Docker-per-trial fallback) | Not started |

Notes vs. the plan:

- The plan's Phase 0 "move to WSL2" item is moot — this workspace is macOS,
  so the Unix `/tmp` assumptions hold natively. The 2026-04-26 Windows
  failure report (31 passed / 21 skipped / 3 failed / 31 errors) does not
  reproduce here and is superseded by the snapshot above.
- `asyncio_mode = "auto"` was already set in `backend/pyproject.toml`; the
  hand-rolled `async_test` decorators in `test_streaming.py` and
  `test_branches.py` have been deleted.

---

## Component triage (what is core vs. workload)

- **Core (the product):** `AsyncPostgresSaver` checkpointing,
  `branches.py` fork semantics, `resume_outer_loop`, SSE streaming, the D3
  trajectory tree (observability UI).
- **Workload (still works, not the claim):** `proposer.py`, the 11
  override points, `SKILL.md`, the Pareto frontier, the 5 search + 2
  holdout eval tasks.
- **Known durability gaps (Phase 2 targets):** branch registry is
  in-process `dict`s in `branches.py` — lost on restart; `EventRegistry`
  is in-process — breaks the moment worker and API are separate processes;
  no leases, no fencing, no boot reconciliation.

---

## Known issues

| Issue | Severity | Notes |
|-------|----------|-------|
| ~~Branch registry/metadata in-process only~~ | Fixed (Phase 2) | `branch_runs` table + leases + fencing tokens; in-process registry remains only as the memory-mode fallback |
| ~~Dashboard falls back to a mock demo run when backend is unreachable~~ | Fixed (Phase 4.0) | Explicit disconnected state; fabricated demo fixture deleted |
| ~~Frontend `getDiff()` / `getTestOutput()` return `null`~~ | Was already fixed | Verified wired to the real endpoints; the doc claim was stale |
| `tokens` / `cost_usd` are zero in real-bench results | Low (demoted) | Real token accounting is explicitly optional in the repositioning plan; no token axis ships in the UI |
| Zombie trailing checkpoint write (DST-2, seed 9270) | Low (documented) | LangGraph checkpoint writes are unfenced; benign — see `docs/INVARIANTS.md` |

---

## Architecture reference

```
meta_harness/
├── agents/               # Candidate harness modules (baseline + generated)
├── backend/
│   ├── app/
│   │   ├── api/          # FastAPI REST routers
│   │   ├── meta_harness/ # Core engine (outer/inner loops, branches, persistence)
│   │   ├── cli.py        # Typer CLI
│   │   ├── main.py       # FastAPI app factory
│   │   └── streaming.py  # SSE event registry (closed set of 11 event types)
│   └── tests/            # pytest suite (asyncio_mode = "auto")
├── eval/                 # 5 search tasks + 2 holdout tasks + scorer
├── frontend/             # Next.js dashboard
├── infra/                # docker-compose.yml (Postgres 16)
├── documents/            # REPOSITIONING_PLAN.md — the active plan
└── docs/                 # INVARIANTS.md (spec), historical design docs
```

---

## Key documents

- [`documents/REPOSITIONING_PLAN.md`](../documents/REPOSITIONING_PLAN.md) — the active plan; read first
- [`docs/INVARIANTS.md`](INVARIANTS.md) — invariants I1–I7 the runtime is tested against
- [`docs/INTERFACES.md`](INTERFACES.md) — cross-component contracts
- Historical (pre-repositioning, contain synthetic demo-arc numbers):
  `BUILD_ORDER.md`, `DEFINITION_OF_DONE.md`, `PROJECT_KNOWLEDGE_BASE.md`,
  `TEAM_HANDOFF.md`
