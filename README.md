# Meta-Harness

> *A durable execution runtime for long-horizon agent workflows — checkpointed,
> forkable, crash-recoverable — verified by deterministic simulation testing.*

LangGraph gives a single thread checkpoint persistence and resume. This
project adds what it doesn't have: **branch lifecycle as durable state,
lease-based claiming with fencing tokens across processes, crash
reconciliation on boot, and fork-from-arbitrary-checkpoint with state
mutation** — and verifies those mechanics with a seeded, fault-injecting
simulator instead of hope.

The reference workload is a self-improving coding-agent harness search
(after the Stanford Meta-Harness paper,
[arXiv:2603.28052](https://arxiv.org/abs/2603.28052)): an outer LangGraph
state machine proposes candidate harnesses, benchmarks them in an inner
state machine, and forks alternative branches from any checkpoint. The
workload has to *run*; it is not the claim. No benchmark-accuracy numbers
are claimed anywhere in this repo — the mock benchmark is a deterministic
fixture and is labeled as such.

---

## What the runtime guarantees

The spec lives in [`docs/INVARIANTS.md`](docs/INVARIANTS.md); the
simulator and test suite assert it. In short:

| ID | Invariant |
|---|---|
| I1 | No double execution — no iteration lands twice across any crash/resume sequence |
| I2 | No orphans — after boot reconciliation, no branch stays `running` without a live lease |
| I3 | Resume convergence — crash + resume reaches the same final state as an uninterrupted run |
| I4 | Fork isolation — a branch's writes never mutate parent thread state |
| I5 | Lease safety — at most one worker executes a branch at a time (fencing tokens, not just leases) |
| I6 | Durable cancel — a cancelled branch never resumes after restart |
| I7 | Event integrity — per-run SSE sequence numbers are monotonic with no gaps |

---

## Architecture

```
   OUTER STATE MACHINE  (4 nodes, checkpointed via AsyncPostgresSaver)
   ──────────────────────────────────────────────────────────────────
   propose ──► validate ──► benchmark ──► update_frontier
      │                          │                │
      │                          │                └─ loop while budget > 0
      ▼                          ▼
   spawns proposer            spawns inner
   subprocess + SKILL.md      subgraph per candidate
                                  │
                                  ▼
   INNER STATE MACHINE  (5 nodes, sandboxed subgraph per candidate)
   ────────────────────────────────────────────────────────────────
   orient ─► plan ─► act ─► verify ─► submit
                                  │
                                  ▼  events streamed via SSE (closed set of 11 types)
   DASHBOARD  (Next.js)
   ────────────────────
   ▸ outer state graph (ReactFlow) — nodes light up per iteration
   ▸ trajectory tree (D3) — branch lineage, fork from any checkpoint
   ▸ code diff viewer (Monaco) — candidate vs parent
   ▸ cross-run memory panel — patterns learned by prior runs
```

Durability mechanics (branch lifecycle table, lease claiming with
fencing tokens, boot reconciliation, worker/API process split, SSE over
Postgres LISTEN/NOTIFY) are being landed per
[`documents/REPOSITIONING_PLAN.md`](documents/REPOSITIONING_PLAN.md);
current phase status is tracked honestly in
[`docs/PROJECT_STATUS.md`](docs/PROJECT_STATUS.md).

---

## Quickstart

**Prerequisites**

- Python 3.11+ and [uv](https://github.com/astral-sh/uv)
- Docker (for local Postgres)
- Node.js 20+ + npm (for the dashboard, optional)
- An Anthropic API key only for live-LLM paths (everything below runs without one)

**Get running**

```bash
git clone https://github.com/jstxw/Meta-Harness.git
cd Meta-Harness
cp .env.example .env
uv sync
docker compose -f infra/docker-compose.yml up -d postgres

# Backend test suite (live LLM test skips without ANTHROPIC_API_KEY)
cd backend && uv run pytest tests -q
# → 92 passed, 1 skipped

# Full outer loop with the deterministic mock proposer + mock benchmark
uv run meta-harness loop --proposer mock --mock-bench --budget 2 --fresh

# Kill it mid-run, then resume from the last Postgres checkpoint
uv run meta-harness resume <run-name>

# No duplicate iterations across the crash/resume sequence (I1):
cat ../runs/<run-name>/evolution_summary.jsonl | jq -r .iteration | sort | uniq -d   # empty
```

Every number quoted in this repo is reproducible by a command printed
next to it. Mock-bench scores follow a hardcoded fixture curve and are
never presented as measurements.

---

## What's distinctive about this implementation

1. **Durability is the product, not a feature flag.** Branch lifecycle,
   leases, fencing tokens, and boot reconciliation are first-class
   durable state — not in-process dicts that die with the process.
2. **Deterministic simulation testing.** A seeded scheduler drives the
   orchestrator against an in-memory state store with a virtual clock
   and fault injection (crash, lease expiry, stall, clock skew,
   duplicate delivery). Failures replay from their seed.
3. **Two LangGraph state machines.** The outer machine evolves the inner
   machine's source code; both are checkpointed; forks are concurrent
   (`asyncio.create_task`, independently cancellable) and grow on the
   dashboard at once.
4. **A closed SSE contract.** Eleven event types, registry-enforced;
   unknown types are a 500, not a silent new feature.
5. **The inner loop has a fixed contract and an evolvable shape.** Six
   tools are the contract with the evaluator and cannot be modified by
   candidates; eleven override points define the search space.
6. **Cross-run memory persists across runs.** A pattern learned in run A
   flows into run B's proposer system prompt.

---

## Repository layout

```
meta-harness/
├── backend/                                   # FastAPI + LangGraph
│   ├── app/
│   │   ├── cli.py                             # `meta-harness` CLI (typer)
│   │   ├── main.py                            # FastAPI app entry
│   │   ├── streaming.py                       # closed-set SSE event registry
│   │   ├── api/                               # REST routers
│   │   └── meta_harness/                      # internal namespace
│   │       ├── outer.py                       # outer 4-node StateGraph
│   │       ├── inner.py                       # inner 5-phase StateGraph
│   │       ├── state.py                       # MetaHarnessState + CodingAgentState
│   │       ├── harness.py                     # CodingAgentHarness (11 override points)
│   │       ├── proposer.py                    # claude_propose + mock_propose
│   │       ├── tools.py                       # 6 fixed inner-loop tools
│   │       ├── sandbox.py                     # per-task sandbox dirs
│   │       ├── frontier.py                    # Pareto on (accuracy × tokens)
│   │       ├── persistence.py                 # AsyncPostgresSaver
│   │       ├── runs.py                        # filesystem lifecycle
│   │       ├── memory.py                      # cross-run patterns
│   │       └── branches.py                    # forks + trajectory
│   └── tests/                                 # backend pytest suite
├── frontend/                                  # Next.js dashboard
├── sdk/meta_harness/                          # public Python library
├── skills/meta-harness-coding-agent/SKILL.md  # the proposer's workflow
├── eval/                                      # 5 search tasks + 2 holdout + scorer
├── agents/                                    # baseline + generated candidates
├── infra/docker-compose.yml                   # postgres:16 service
├── documents/REPOSITIONING_PLAN.md            # the active plan — read first
└── docs/                                      # INVARIANTS.md, PROJECT_STATUS.md, contracts
```

---

## Documentation

| Doc | When to read |
|---|---|
| [`documents/REPOSITIONING_PLAN.md`](documents/REPOSITIONING_PLAN.md) | First — the active plan and framing |
| [`docs/INVARIANTS.md`](docs/INVARIANTS.md) | The spec: invariants I1–I7 + branch state machine |
| [`docs/PROJECT_STATUS.md`](docs/PROJECT_STATUS.md) | Current verified state, with reproduction commands |
| [`docs/INTERFACES.md`](docs/INTERFACES.md) | Every cross-component contract |
| `docs/BUILD_ORDER.md`, `docs/DEFINITION_OF_DONE.md`, `docs/PROJECT_KNOWLEDGE_BASE.md`, `docs/TEAM_HANDOFF.md` | Historical (pre-repositioning); contain synthetic demo-arc numbers — do not quote them as results |
| [`relay_metaharness_v7.md`](relay_metaharness_v7.md) + appendices | The original design docs for the workload |

---

## Acknowledgments

Built on, and grateful for, the work of:

- Yoonho Lee, Roshen Nair, Qizheng Zhang, Kangwook Lee, Omar Khattab,
  and Chelsea Finn — *Meta-Harness: End-to-End Optimization of Model
  Harnesses*, [arXiv:2603.28052](https://arxiv.org/abs/2603.28052),
  [project page](https://yoonholee.com/meta-harness/).
- The LangChain team for LangGraph's checkpointing and time-travel
  primitives.
- Anthropic for the Claude Code CLI's `--append-system-prompt` and
  stream-json output, used by the real proposer path.

---

## License

MIT — see [LICENSE](LICENSE).
