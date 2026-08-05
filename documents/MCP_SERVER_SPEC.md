# MCP Server — Durable Execution as a Consumable Service

**Standalone spec. Companion to `REPOSITIONING_PLAN.md` (this is its Phase 6, expanded).**

---

## 0. Why this exists

The repositioning claims: *a durable execution runtime for long-horizon agent workflows.*

Right now that claim is asserted. The runtime runs one workload — your own harness search — and the only evidence it's a runtime is that you say so. Wrapping it in MCP makes the claim **demonstrable by a third party**: an agent with no knowledge of this repo forks a run from a mid-point checkpoint, survives a worker kill, and continues. Infrastructure is infrastructure when something else consumes it.

Secondary benefit: it produces a demo with zero dependence on the harness search, mock-bench, or any accuracy number.

### Hard gate

**Do not start this until Phase 2 of the repositioning plan is complete** (durable `branch_runs` table, lease claiming with fencing tokens, boot reconciliation, worker process split).

Wrapping the current in-process `branch_registry: dict[str, asyncio.Task]` in MCP would expose the fragility to *more* clients, not fewer. A client that holds a `branch_id` across a server restart and gets a 404 is a worse experience than not having the server. Durable branches first.

---

## 1. Architecture

```
Claude Code / Claude Desktop / other MCP client
        │  stdio (local) or HTTP (remote)
        ▼
   MCP adapter  ← thin, no business logic
        │
        ▼
   FastAPI app  ← existing REST surface, single source of truth
        │
        ▼
   StateStore (Postgres)  ← branch_runs, checkpoints
        │
        ▼
   Worker pool  ← lease claiming, SKIP LOCKED, fencing
```

**The adapter must be thin.** Every MCP tool maps to an existing REST call. If a tool needs logic that doesn't exist in the REST layer, add it to the REST layer first. Two parallel implementations of branch lifecycle is the failure mode to avoid — it's how I5 (lease safety) gets violated in one path and not the other.

---

## 2. Tool surface

Six tools. Resist adding more until something actually needs them.

### `start_run`

```json
{
  "name": "start_run",
  "description": "Start a durable, checkpointed run. Returns immediately with a run_id; the run continues on the server and survives client disconnect. Poll get_branch_status for progress.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "workload": { "type": "string", "description": "Registered workload identifier" },
      "config":   { "type": "object", "description": "Workload-specific configuration" }
    },
    "required": ["workload"]
  }
}
```

Returns: `{ run_id, thread_id, status: "created" }`

### `fork_from_checkpoint`

```json
{
  "name": "fork_from_checkpoint",
  "description": "Fork a new branch from any prior checkpoint of an existing run, optionally mutating state. The parent run is unaffected. Returns a branch_id immediately.",
  "inputSchema": {
    "type": "object",
    "properties": {
      "run_id":        { "type": "string" },
      "checkpoint_id": { "type": "string" },
      "mods":          { "type": "object", "description": "State mutations applied at fork point" },
      "name":          { "type": "string", "description": "Optional human label" }
    },
    "required": ["run_id", "checkpoint_id"]
  }
}
```

Returns: `{ branch_id, thread_id, parent_checkpoint_id }`

Wraps the existing `worktree_add`. Thread naming stays `{parent_thread_id}.fork.{branch_id}`.

### `get_branch_status`

The poll endpoint. Everything long-running resolves through this.

Returns:
```json
{
  "branch_id": "a3f9c1e2",
  "status": "running",
  "lease_generation": 3,
  "lease_owner": "worker-2",
  "lease_valid": true,
  "last_checkpoint_id": "ckpt_00017",
  "iteration": 4,
  "result": null,
  "error": null
}
```

**Expose `lease_generation`.** A client that caches the fence and later sees it incremented knows its branch was reclaimed by another worker after a lease expiry. This surfaces the fencing-token mechanism at the protocol boundary — it's the single most interesting field in the API and it's worth being able to point at.

### `list_branches`

Returns the lineage tree as data — the same structure the D3 view renders. Include `status`, `lease_valid`, `parent_checkpoint_id`, and `created_at` per node so a client can reconstruct the tree without a second call.

### `cancel_branch`

Must satisfy **I6 (durable cancel)**: a cancelled branch never resumes after restart. Cancellation writes to `branch_runs` before signalling the in-worker task, not after — otherwise a crash between the two leaves a cancelled-but-resumable branch.

### `resume_run`

Wraps `resume_outer_loop`. Idempotent: calling it on an already-running run is a no-op returning current status, not an error and not a second execution (**I1**).

---

## 3. Design constraints

### No blocking tool calls

MCP is request/response. Every long-running operation returns an ID immediately and the client polls `get_branch_status`.

This shape already exists twice in prior work — SSE-backed runs here, and FrameShift's `202` + status-endpoint + `pollJob` loop. Reuse the pattern; don't invent a third.

Optional: MCP progress notifications can carry stage updates for clients that support them. Nice-to-have, explicitly not required — polling must work standalone.

### Tool descriptions must state durability

A calling agent needs to know that runs survive client disconnect, because that changes how it behaves — it can start work, drop the connection, and reconnect later. That property is the entire differentiator, and if it's not in the tool description the agent will never exercise it. Say it in `start_run`, `fork_from_checkpoint`, and `resume_run`.

### Transport

Start with **stdio** — simplest, covers local Claude Code and Claude Desktop, and is enough for the demo.

Add HTTP only if remote clients actually matter. If you do: **auth before anything else.** This server starts arbitrary sandboxed code execution. An unauthenticated HTTP endpoint that runs untrusted workloads is not a portfolio project, it's an incident.

### Error semantics

- Unknown `run_id` / `branch_id` → tool error, not an empty success.
- Stale fence on a write → explicit `lease_reclaimed` error naming the current generation, so the client can re-poll rather than retry blindly.
- Server restart mid-poll → the client's next `get_branch_status` must succeed and reflect real state. This is the whole point; test it explicitly.

---

## 4. Non-MCP clients

If a target agent doesn't speak MCP, expose the same six operations as plain REST over the existing FastAPI app. **Do not build a second abstraction** — the MCP server stays a thin adapter over that REST surface.

Concretely:

| MCP tool | REST |
|---|---|
| `start_run` | `POST /runs` |
| `fork_from_checkpoint` | `POST /runs/{run_id}/branches` |
| `get_branch_status` | `GET /branches/{branch_id}` |
| `list_branches` | `GET /runs/{run_id}/branches` |
| `cancel_branch` | `DELETE /branches/{branch_id}` |
| `resume_run` | `POST /runs/{run_id}/resume` |

---

## 5. Build order

- [ ] Confirm Phase 2 exit criteria actually passes (kill `-9` a worker mid-branch; another picks it up; no duplicate iterations).
- [ ] Fill any gaps in the REST surface above so every MCP tool has a REST counterpart.
- [ ] Implement the stdio adapter. Six tools, no logic.
- [ ] Write tool descriptions with the durability property stated explicitly.
- [ ] Register with Claude Code locally and drive it by hand.
- [ ] Run the acceptance scenario below.
- [ ] HTTP transport + auth, only if needed.

---

## 6. Acceptance scenario

The thing to record and put in the README.

1. A Claude Code session, with no knowledge of this repo, calls `start_run`.
2. It polls, sees checkpoints accumulating, picks a mid-point `checkpoint_id`.
3. It calls `fork_from_checkpoint` with `mods` altering state at that point.
4. You `kill -9` the worker executing that branch.
5. The session's next `get_branch_status` poll shows the branch **running again, with an incremented `lease_generation`**, resumed from its last checkpoint.
6. `evolution_summary.jsonl` contains no duplicate iteration numbers (**I1**).

**Exit criteria:** steps 1–6 complete without you touching anything except the kill in step 4.

If that works, the runtime claim is demonstrated rather than asserted — by a client that has never seen the codebase.

---

## 7. What this is not

- Not a public service. Local-first, auth-gated if remote.
- Not a reason to add tools beyond the six. Surface area is a liability here.
- Not a replacement for the frontend. The chaos-button demo (Phase 4.1) and this are different audiences: one is visual, one is programmatic. Build the visual one first — it's cheaper and it's what people watch.
- Not a substitute for Phase 3. An MCP server over an unverified runtime is a nicer wrapper on an unproven claim.
