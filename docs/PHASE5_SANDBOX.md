# Phase 5 — Sandbox: the wasm spike and the Docker outcome

**Status: landed as Docker-per-trial** (`META_HARNESS_SANDBOX=docker`),
per the plan's own fallback rule. Subprocess mode remains the default.

---

## The wasm spike (timeboxed, concluded fast)

The plan's preference was wasmtime: capability-based isolation,
millisecond startup, deterministic execution. The spike died on the
first structural check, before packaging even mattered:

**The inner-loop contract includes `run_bash` — arbitrary shell.**
The six fixed tools are the frozen contract with the evaluator
(`read_file`, `write_file`, `apply_patch`, `run_bash`, `grep_search`,
`task_complete`), and candidates rely on `run_bash` for everything from
`ls` to invoking `pytest`. WASI has no subprocess model and no shell to
exec — there is nothing inside a wasm sandbox that can honor a
`run_bash("pytest -q")` call. Making it work would mean either:

- reimplementing a shell + process model inside the guest (a project,
  not a phase), or
- narrowing the tool contract to remove `run_bash` — which the plan
  forbids: the 6 tools are fixed and evolving them is explicitly out of
  the workload's search space.

Pyodide/componentize-py packaging friction (the risk the plan
predicted) never had to be evaluated; the contract question is
dispositive on its own.

**What survives from the wasm reasoning (the interview answer):** wasm's
startup latency (~ms vs Docker's ~0.5–1s per-trial container) and its
capability-grant model (denied file-open attempts as first-class,
observable events) are real advantages — for a workload whose tool
surface is pure functions over files. This workload's isn't.

## What shipped instead

`META_HARNESS_SANDBOX=docker` switches `sandbox_for` to Docker-per-trial:

- one container per trial (`docker run -d --rm`), torn down with the
  sandbox — lifecycle matches the existing sandbox contextmanager
  exactly;
- `--network none` — trials cannot reach anything;
- 512MB memory cap, 1 CPU — the same budget the subprocess rlimits
  aimed for, now actually enforced on macOS too (where `RLIMIT_AS` is
  unreliable);
- workspace bind-mounted at `/workspace`; the container sees nothing
  else of the host filesystem;
- every `run_bash` / verify-phase `pytest` goes through `docker exec`
  in that container — one choke point, `run_in_sandbox`, unchanged
  callers.

Build the image once (pytest is baked in because `--network none`
containers can't install anything, deliberately):

```bash
docker build -t meta-harness-sandbox -f infra/sandbox.Dockerfile infra
```

Run trials under it:

```bash
META_HARNESS_SANDBOX=docker uv run meta-harness benchmark --candidate baseline --trials 1
```

The isolation boundary a trial actually ran under is recorded in its
`eval-result.json` (`"sandbox": "subprocess" | "docker" | "none
(mock-bench)"`), so results always say what they were produced with.

Verification (skips when Docker or the image is unavailable):

```bash
cd backend && uv run pytest tests/test_sandbox_docker.py -q
```
