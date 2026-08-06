"""Sandbox for inner-loop tool execution (Phase 5).

Two isolation modes, selected by ``META_HARNESS_SANDBOX``:

- ``subprocess`` (default) — fresh ``/tmp/meta-harness-task-{uuid}/``
  dir, ``subprocess.run(..., cwd=task_dir, timeout=...)``, rlimit 512MB
  RAM + 60s CPU on Unix. Process isolation only: same trust boundary as
  the host, honestly labeled as such.
- ``docker`` — Docker-per-trial: ``sandbox_for`` starts one container
  per trial (``--network none``, 512MB memory cap, 1 CPU, workspace
  bind-mounted at /workspace) and every command runs via ``docker
  exec``. A real isolation boundary: no network, no host filesystem
  beyond the mounted workspace.

Why Docker and not wasmtime (the plan's preferred Phase 5 outcome): the
inner-loop contract includes ``run_bash`` — arbitrary shell — and WASI
has no subprocess or shell to offer, so pytest-under-WASM dies on the
contract itself, not on packaging. See docs/PHASE5_SANDBOX.md for the
spike write-up. The wasm trade (millisecond startup vs Docker's
~0.5-1s per-trial container) stays on record there.

The two layers:
- ``make_sandbox_dir`` / ``populate_sandbox`` / ``cleanup_sandbox`` /
  ``sandbox_for`` — sandbox lifecycle (create, copy task workspace, clean).
- ``run_in_sandbox`` — low-level exec used by ``tools.run_bash`` and
  by the inner loop's verify phase to invoke ``pytest``.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

# rlimit support is Unix-only; on other platforms we no-op.
try:
    import resource as _resource

    _HAS_RLIMIT = True
except ImportError:  # Windows
    _resource = None  # type: ignore[assignment]
    _HAS_RLIMIT = False


SANDBOX_PREFIX = "meta-harness-task-"
DEFAULT_RLIMIT_RAM = 512 * 1024 * 1024  # 512 MB
DEFAULT_RLIMIT_CPU = 60  # seconds

DOCKER_IMAGE = os.environ.get("META_HARNESS_SANDBOX_IMAGE", "meta-harness-sandbox:latest")
_containers: dict[Path, str] = {}  # sandbox dir → container id (docker mode)


def sandbox_mode() -> str:
    """Current isolation mode: ``subprocess`` (default) or ``docker``."""
    mode = os.environ.get("META_HARNESS_SANDBOX", "subprocess").lower()
    return mode if mode in {"subprocess", "docker"} else "subprocess"


def _start_container(sandbox_dir: Path) -> str:
    """Start the per-trial container for a sandbox dir."""
    result = subprocess.run(
        [
            "docker", "run", "-d", "--rm",
            "--network", "none",
            "--memory", "512m",
            "--cpus", "1",
            "-v", f"{sandbox_dir.resolve()}:/workspace",
            "-w", "/workspace",
            DOCKER_IMAGE,
            "sleep", "infinity",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"failed to start sandbox container from {DOCKER_IMAGE}: "
            f"{result.stderr.strip()} — build it with: "
            "docker build -t meta-harness-sandbox -f infra/sandbox.Dockerfile infra"
        )
    container_id = result.stdout.strip()
    _containers[sandbox_dir] = container_id
    return container_id


def _stop_container(sandbox_dir: Path) -> None:
    container_id = _containers.pop(sandbox_dir, None)
    if container_id:
        subprocess.run(
            ["docker", "rm", "-f", container_id],
            capture_output=True,
            timeout=30,
        )


def make_sandbox_dir() -> Path:
    """Create a fresh ``/tmp/meta-harness-task-{uuid}/`` and return it."""
    sandbox = Path("/tmp") / f"{SANDBOX_PREFIX}{uuid.uuid4().hex}"
    sandbox.mkdir(parents=True, exist_ok=False)
    return sandbox


def populate_sandbox(sandbox_dir: Path, source_workspace: Path) -> None:
    """Copy a task's pristine workspace into the sandbox."""
    if not source_workspace.is_dir():
        raise ValueError(f"source workspace is not a directory: {source_workspace}")
    for entry in source_workspace.iterdir():
        target = sandbox_dir / entry.name
        if entry.is_dir():
            shutil.copytree(entry, target)
        else:
            shutil.copy2(entry, target)


def cleanup_sandbox(sandbox_dir: Path) -> None:
    """Remove a sandbox directory. Idempotent and tolerant of missing dir."""
    shutil.rmtree(sandbox_dir, ignore_errors=True)


@contextmanager
def sandbox_for(source_workspace: Path) -> Iterator[Path]:
    """Context manager: fresh sandbox, populated from ``source_workspace``,
    cleaned up on exit. In docker mode this also owns the per-trial
    container's lifecycle (start → exec… → remove).
    """
    sandbox = make_sandbox_dir()
    try:
        populate_sandbox(sandbox, source_workspace)
        if sandbox_mode() == "docker":
            _start_container(sandbox)
        yield sandbox
    finally:
        _stop_container(sandbox)
        cleanup_sandbox(sandbox)


def _apply_rlimits() -> None:
    """preexec_fn: apply rlimits before exec'ing the child.

    Best-effort. Each setrlimit is wrapped so a single failure doesn't
    abort the exec. macOS's ``RLIMIT_AS`` enforcement is unreliable for
    Python child processes (Python's own address-space footprint can
    already exceed the cap before the child runs anything), so we skip
    it on Darwin and rely on the wall-clock timeout from
    ``subprocess.run`` instead.
    """
    if not _HAS_RLIMIT or _resource is None:
        return
    if sys.platform != "darwin":
        try:
            _resource.setrlimit(
                _resource.RLIMIT_AS,
                (DEFAULT_RLIMIT_RAM, DEFAULT_RLIMIT_RAM),
            )
        except (ValueError, OSError):
            pass
    try:
        _resource.setrlimit(
            _resource.RLIMIT_CPU,
            (DEFAULT_RLIMIT_CPU, DEFAULT_RLIMIT_CPU),
        )
    except (ValueError, OSError):
        pass


def run_in_sandbox(
    sandbox_dir: Path,
    command: str,
    *,
    timeout_sec: int = 30,
) -> subprocess.CompletedProcess[str]:
    """Run a shell command in the sandbox.

    Caller handles ``subprocess.TimeoutExpired``. Subprocess mode applies
    rlimits via ``preexec_fn`` (Unix only); docker mode execs inside the
    per-trial container, whose memory/cpu/network limits were set at
    ``docker run`` time. A timed-out ``docker exec`` kills the client —
    any lingering in-container process dies with the container at
    sandbox teardown.
    """
    container_id = _containers.get(sandbox_dir)
    if container_id is not None:
        return subprocess.run(
            ["docker", "exec", container_id, "bash", "-lc", command],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_sec,
        )
    return subprocess.run(  # noqa: S602 — controlled command in sandbox
        command,
        shell=True,
        cwd=sandbox_dir,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout_sec,
        preexec_fn=_apply_rlimits if _HAS_RLIMIT and sys.platform != "win32" else None,
    )
