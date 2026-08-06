# Technical Overview — Meta-Harness

> Everything technical in one place: the stack, the data model, the protocols,
> every process, every module, every command. Companion to
> [`INVARIANTS.md`](INVARIANTS.md) (the spec) and
> [`PROJECT_STATUS.md`](PROJECT_STATUS.md) (verified state).
> Last verified: 2026-08-06 · suite `128 passed, 1 skipped` · DST `10,000 seeds, 0 failed`.

---

## 1. What this system is

A **durable execution runtime** for long-horizon agent workflows. Runs are
checkpointed LangGraph state machines; forks of any checkpoint become durable
**branches** executed by a pool of worker processes; workers coordinate through
Postgres with **leases + fencing tokens**, so any worker (or the whole fleet)
can be killed at any moment and the work continues from its last checkpoint —
exactly once.

The bundled workload is a self-improving coding-agent harness search
(Stanford Meta-Harness paper, arXiv:2603.28052). It exercises the runtime; the
runtime is the product.

## 2. Stack

| Layer | Choice | Notes |
|---|---|---|
| Language (backend) | Python 3.11+ | `uv` workspace (`sdk/` + `backend/`) |
| State machines | LangGraph 0.2+ | outer + inner graphs, compiled per run |
| Checkpointing | `AsyncPostgresSaver` (langgraph-checkpoint-postgres) | shared by API and all workers |
| Database | Postgres 16 (Docker, `infra/docker-compose.yml`, compose project `meta-harness`) | the only stateful service |
| DB driver | psycopg 3 (async) + psycopg_pool | store uses a dedicated autocommit connection |
| API | FastAPI + Uvicorn | REST + SSE |
| Worker | plain asyncio process (`app/worker.py`) | no Celery/Temporal/queues — deliberate non-goal |
| MCP | `mcp` SDK (`MCPServer`, stdio) | thin adapter over REST |
| Simulation | pure Python (`backend/sim/`), zero deps | seeded RNG + virtual clock |
| Property testing | Hypothesis (`RuleBasedStateMachine`) | sequences; the simulator covers interleavings |
| Frontend | Next.js 16 (App Router), React 19, Tailwind 4 | D3 (trajectory tree), ReactFlow, Monaco, framer-motion |
| Frontend auth | Auth0 (`@auth0/nextjs-auth0`) via `src/proxy.ts` | `/replay*` and `/` are public |
| Trial sandbox | subprocess+rlimits (default) or Docker-per-trial | `META_HARNESS_SANDBOX=docker` |
| CLI | Typer (`meta-harness …`) | loop, resume, worker, benchmark, fork, memory |
| Tests | pytest (`asyncio_mode="auto"`), Playwright available | 128 passed / 1 skipped (live-LLM skip) |

## 3. Processes and ports

```
Postgres 16          :5432   docker container meta-harness-postgres
FastAPI (uvicorn)    :8000   REST + SSE; owns run execution for API-started runs
meta-harness worker  (n)     separate processes; claim/execute branches
Next.js dashboard    :3000   dev/prod frontend
MCP adapter          stdio   spawned by the MCP client (Claude Code etc.)
```

Env vars that matter:

| Var | Effect |
|---|---|
| `POSTGRES_DSN` | default `postgresql://meta_harness:meta_harness@localhost:5432/meta_harness` |
| `META_HARNESS_API_PERSISTENT` | `auto` (default) / `0` — memory-mode fallback when Postgres is down |
| `META_HARNESS_CHAOS` | `1` enables `/debug/workers` + `/debug/kill-worker/{id}` |
| `META_HARNESS_SANDBOX` | `subprocess` (default) / `docker` — trial isolation |
| `META_HARNESS_API_URL` | MCP adapter → API base (default `http://localhost:8000`) |
| `ANTHROPIC_API_KEY` | only for the real (non-mock) proposer / live-LLM test |

## 4. Data model (Postgres)

Created idempotently by `StateStore.setup()` (`backend/app/meta_harness/store.py`):

```sql
branch_runs (          -- durable branch lifecycle; THE core table
    branch_id  PK, run_id, thread_id UNIQUE, parent_thread_id,
    parent_checkpoint_id, status,          -- created|running|completed|failed|cancelled
    mods JSONB, name, result JSONB, error,
    lease_owner, lease_generation BIGINT,  -- the fencing token
    lease_expires_at, created_at, started_at, finished_at )

iteration_log (        -- authoritative exactly-once iteration record (I1)
    run_id, iteration, candidate, row JSONB, ts,
    PRIMARY KEY (run_id, iteration, candidate) )

run_event_seq (run_id PK, last_seq)   -- gap-free per-run sequence counter
run_events (run_id, seq, event_type, payload JSONB, ts,
    PRIMARY KEY (run_id, seq))        -- durable SSE log (I7), NOTIFY on insert

workers (worker_id PK, pid, hostname, started_at, last_seen)  -- chaos/observability
```

Plus LangGraph's own checkpoint tables (managed by `AsyncPostgresSaver.setup()`),
and the filesystem per run: `runs/<run_id>/{manifest.json, evolution_summary.jsonl,
frontier_val.json, candidates/…}` — the jsonl is a projection; `iteration_log`
is authoritative.

## 5. The coordination protocol (the actual contribution)

All in `store.py`, one implementation each for Postgres and the deterministic
in-memory fake. The `StateStore` protocol is load-bearing: nothing touches
psycopg directly outside it.

**Claim** — one atomic UPDATE:

```sql
UPDATE branch_runs SET status='running', lease_owner=%(w)s,
       lease_generation = lease_generation + 1,        -- new fence
       lease_expires_at = now() + ttl, started_at = COALESCE(started_at, now())
WHERE branch_id = (
    SELECT branch_id FROM branch_runs
    WHERE status='created' OR (status='running' AND lease_expires_at < now())
    ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1)
RETURNING *;
```

**Fencing (I5).** The claimer holds `lease_generation` as its fence. Heartbeats,
`finish_branch`, and `record_iteration` all carry it (`AND lease_generation =
%(fence)s`); zero rows updated ⇒ `StaleFenceError` ⇒ abort, never retry. A
worker that stalls past its TTL and gets reclaimed learns it lost ownership
from the data layer itself.

**Exactly-once iterations (I1).** `record_iteration` is a single statement:
fence check + `INSERT … ON CONFLICT DO NOTHING` on `(run_id, iteration,
candidate)`. The worker installs it per claim as
`OuterLoopRunner.iteration_recorder`, called before the non-idempotent file
append (which additionally has a read-dedupe guard).

**Cancel (I6).** `request_cancel` bumps the fence *and* sets the terminal
status in one durable write before any in-process task is signalled — a crash
between the two can never leave a cancelled-but-resumable branch.

**Boot reconciliation (I2).** `reconcile_on_boot` requeues expired-lease
`running` rows to `created` (fence preserved — it is monotonic forever). Runs
at API startup and worker startup.

**Events (I7).** `append_event` allocates a gap-free per-run seq via an upsert
counter, inserts, and `pg_notify`s — one atomic statement. The API's SSE
endpoint LISTENs, replays `run_events` after `Last-Event-ID` (= seq), and only
ever reads by `after_seq`, so duplicate/reordered NOTIFYs are harmless.

**Worker loop** (`app/worker.py`): register in `workers` → reconcile → claim →
(fork-or-resume the branch thread) → run the LangGraph with a concurrent
heartbeat task (TTL/3 cadence, fails loudly on stale fence, kills the exec
task) → fenced finish. Fork-or-resume: if the branch thread already has
checkpoints, resume (`ainvoke(None)`); else `aupdate_state` from the parent
checkpoint values + mods, then invoke. Killing a worker at any instruction is
recoverable — that's tested, not asserted.

## 6. The workload (reference, not the claim)

- **Outer graph** (`outer.py`): `propose → validate → benchmark →
  update_frontier`, looping while budget remains. Mock proposer writes
  deterministic candidate stubs; real proposer shells out to the `claude` CLI
  with `SKILL.md`. Mock-bench scores follow `min(0.95, 0.60 + 0.20·(iter−1))`
  — a labeled fixture, never a measurement. Optional `demo_delay_s` state field
  paces iterations for chaos demos.
- **Inner graph** (`inner.py`): `orient → plan → act → verify → submit`, 6
  fixed tools (`read_file, write_file, apply_patch, run_bash, grep_search,
  task_complete` — the frozen contract), 11 override points (the search space).
- **Sandbox** (`sandbox.py`): every `run_bash`/pytest goes through
  `run_in_sandbox`. Subprocess mode = temp dir + rlimits. Docker mode = one
  container per trial (`--network none`, 512MB, 1 CPU, workspace bind mount);
  image `infra/sandbox.Dockerfile` (pytest baked in). The isolation mode is
  recorded in each `eval-result.json`. Wasm was spiked and rejected on a
  structural ground (WASI has no subprocess/shell vs the `run_bash` contract)
  — see `PHASE5_SANDBOX.md`.
- **Cross-run memory** (`memory.py`): `AsyncPostgresStore` patterns injected
  into later proposer prompts.

## 7. API surface (FastAPI, `backend/app/api/`)

| Route | Purpose |
|---|---|
| `POST /runs`, `GET /runs`, `GET /runs/{id}`, `DELETE /runs/{id}` | run lifecycle (API-started runs execute in-process) |
| `POST /runs/{id}/resume` | idempotent resume from last checkpoint (no-op with status if already executing) |
| `GET /runs/{id}/checkpoints[/{ckpt}]` | LangGraph checkpoint history |
| `POST /runs/{id}/fork` = `POST /runs/{id}/branches` | durable fork: creates a `created` branch row; workers pick it up |
| `GET /runs/{id}/branches`, `GET /runs/{id}/trajectory` | lineage (durable store when Postgres is up; in-process fallback otherwise) |
| `GET /branches/{branch_id}` | poll endpoint: status, `lease_generation`, `lease_valid`, last checkpoint, iteration |
| `DELETE /branches/{branch_id}` | durable cancel (I6) |
| `GET /runs/{id}/stream` | SSE; durable mode streams `run_events` via LISTEN/NOTIFY, `id:` = seq |
| `GET /runs/{id}/candidates/{name}/diff·/test-output` | candidate artifacts |
| `GET /memory/{ns}`, `POST /memory/{ns}/search` | cross-run memory |
| `GET /debug/workers`, `POST /debug/kill-worker/{id}` | chaos (gated: `META_HARNESS_CHAOS=1`, same-host, never self) |

SSE event types are a **closed set of 11** (`streaming.py`), registry-enforced —
unknown types are a 500 by design. Worker lifecycle narrative
(claimed/requeued/finished, with the fence) rides inside `state-update`.

## 8. MCP server (`backend/app/mcp_server.py`)

Six tools, 1:1 onto REST, zero business logic: `start_run`,
`fork_from_checkpoint`, `get_branch_status`, `list_branches`, `cancel_branch`,
`resume_run`. Tool descriptions state the durability property (runs survive
client disconnect and worker kills; watch `lease_generation`). Registered
workloads: `mock-loop` (fixture), `harness-search` (real proposer). Stdio
transport; `.mcp.json` registers it for Claude Code; console script
`meta-harness-mcp`. Acceptance scenario (spec §6) automated in
`tests/test_mcp_acceptance.py`: 4 processes, outside client, real SIGKILL,
fence 1→2 observed via polling, no duplicate iterations.

## 9. Deterministic simulation testing (`backend/sim/`)

- `harness.py` — a **synchronous seeded scheduler** drives simulated workers
  against the *real* `InMemoryStateStore` under a `VirtualClock`. Store
  coroutines are driven with a one-shot `coro.send(None)`; anything that truly
  awaits breaks loudly (would break determinism). Faults at scheduling points:
  kill -9, stall past lease, durable cancel, worker clock skew, boot+reconcile,
  duplicate/reordered NOTIFY. Two protocol modes: `unfenced_file` (historical,
  reproduces DST-1) and `fenced_store` (shipped). I1–I7 checked per step and at
  the end. Per-step labeled frames for the replay viewer (recording is inert —
  tested).
- `run.py` — `python -m sim.run --seeds 10000` (0 failures); every failure
  prints its seed; `--seed N -v` replays one.
- `export.py` — `python -m sim.export --seed N -o trace.json`, byte-identical
  every time. Bundled traces live in `frontend/dashboard/public/replays/`.
- Findings: **DST-1 seed 7** (unfenced double-append; fixed by
  `record_iteration`), **DST-2 seed 9270** (zombie trailing checkpoint;
  documented benign). Details in `INVARIANTS.md`.
- `tests/test_store_hypothesis.py` — Hypothesis `RuleBasedStateMachine` over the
  lifecycle: legal transitions only, ≤1 current-fence holder, fences never
  regress, I2 after every reconcile.

## 10. Frontend (`frontend/dashboard/`)

Next.js App Router; auth via Auth0 in `src/proxy.ts` (matcher excludes
`/replay`, `/replays/*`, `/`, static assets). Reducer-based dashboard state
(`lib/state.ts`) fed by REST + SSE (`lib/api.ts`, `lib/sse.ts`). **No
fabricated data anywhere**: backend down = explicit disconnected screen;
mock-bench runs are badged "fixture (mock-bench)".

Pages/components that matter:

- `/` — landing; launches preset runs through `POST /runs`.
- `/runs/[run_id]` — live dashboard: D3 trajectory tree, decision log, context
  panel with tabs `chart · diff · test · memory · chaos`.
  - **chaos tab** (`ChaosPanel.tsx`): worker list with `kill -9` buttons
    (gated), branch lease view (status, owner, countdown, **fence badge** that
    flashes on increment), fence-increment narrative log. Polls
    `/debug/workers` + `/runs/{id}/branches`.
- `/replay` — static seed-replay viewer: scrubber over exported DST frames,
  amber fault ticks, red violation ticks, per-step worker/branch state. Loads
  bundled traces or a local `sim.export` file; zero backend calls.

## 11. Testing map

| File | Covers |
|---|---|
| `test_store.py` | StateStore contract, both impls: lifecycle, I2, I5 (fence reclaim/reject), I6, I7 (+concurrency), worker registry, reaping |
| `test_worker_recovery.py` | kill -9 e2e: I1/I3/I5 with real processes |
| `test_mcp_acceptance.py` | MCP spec §6 acceptance: outside client + SIGKILL + fence observation |
| `test_sim.py` | 200-seed sweep, determinism, DST-1 regression pin (seed 7), DST-2 pin (9270), export determinism |
| `test_store_hypothesis.py` | stateful property tests over the lifecycle |
| `test_chaos.py` | debug endpoints: env gating, real SIGKILL, corpse handling |
| `test_sandbox_docker.py` | docker mode: mount, no network, pytest-in-container (gated on image) |
| `test_persistence.py` / `test_branches.py` / `test_memory*.py` | checkpointing (I1/I3), forks (I4), cross-run memory |
| `test_api.py` / `test_cli.py` / `test_streaming.py` / … | REST contract, CLI, closed SSE set |

Postgres-gated tests skip cleanly when the DB is down (session-scoped
healthcheck fixture in `tests/conftest.py`). One known caveat: live workers
polling the same dev database can steal test branches — don't run the suite
with a worker fleet up.

## 12. Command crib sheet

```bash
# infra
docker compose -f infra/docker-compose.yml up -d postgres
docker build -t meta-harness-sandbox -f infra/sandbox.Dockerfile infra

# verify everything
cd backend && uv run pytest tests -q                    # 128 passed, 1 skipped
cd backend && uv run python -m sim.run --seeds 10000    # 0 failures

# run the system
META_HARNESS_CHAOS=1 uv run uvicorn app.main:app --port 8000
uv run meta-harness worker --worker-id w1               # any number of these
uv run meta-harness loop --proposer mock --mock-bench --budget 2 --fresh
uv run meta-harness resume <run-name>

# frontend
cd frontend/dashboard && npm run dev                    # :3000

# MCP
claude mcp add meta-harness -- uv --directory backend run meta-harness-mcp

# replay artifacts
cd backend && uv run python -m sim.export --seed 7 --mode unfenced_file -o trace.json
```

## 13. Deliberate non-goals

No Temporal/Celery/RQ/Kafka/K8s/gRPC, no second database, no Rust, no new SSE
event types, no benchmark-accuracy claims, no Harbor migration. Each would add
a name without adding an idea — the point is that the durable-execution
mechanics are implemented *here*, small enough to read
(`store.py` + `worker.py` ≈ the whole protocol).
