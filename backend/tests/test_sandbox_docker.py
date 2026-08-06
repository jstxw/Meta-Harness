"""Docker-per-trial sandbox tests (Phase 5).

Skip when Docker or the sandbox image is unavailable; build the image
with::

    docker build -t meta-harness-sandbox -f infra/sandbox.Dockerfile infra
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.meta_harness import sandbox as sb  # noqa: E402


def _docker_ready() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        probe = subprocess.run(
            ["docker", "image", "inspect", sb.DOCKER_IMAGE],
            capture_output=True,
            timeout=20,
        )
    except Exception:  # noqa: BLE001
        return False
    return probe.returncode == 0


pytestmark = pytest.mark.skipif(
    not _docker_ready(),
    reason="docker or the meta-harness-sandbox image is unavailable; "
    "build with: docker build -t meta-harness-sandbox -f infra/sandbox.Dockerfile infra",
)


@pytest.fixture
def docker_mode(monkeypatch):
    monkeypatch.setenv("META_HARNESS_SANDBOX", "docker")


def test_docker_sandbox_runs_commands_in_mounted_workspace(tmp_path: Path, docker_mode):
    workspace = tmp_path / "ws"
    workspace.mkdir()
    (workspace / "hello.txt").write_text("from the host\n")

    with sb.sandbox_for(workspace) as sandbox:
        assert sandbox in sb._containers
        result = sb.run_in_sandbox(sandbox, "cat hello.txt && pwd", timeout_sec=30)
        assert result.returncode == 0
        assert "from the host" in result.stdout
        assert "/workspace" in result.stdout

        # Writes land back in the sandbox dir through the bind mount.
        sb.run_in_sandbox(sandbox, "echo escaped > out.txt", timeout_sec=30)
        assert (sandbox / "out.txt").read_text().strip() == "escaped"

    # Container removed with the sandbox.
    assert sandbox not in sb._containers


def test_docker_sandbox_has_no_network(tmp_path: Path, docker_mode):
    workspace = tmp_path / "ws"
    workspace.mkdir()
    with sb.sandbox_for(workspace) as sandbox:
        result = sb.run_in_sandbox(
            sandbox,
            "python -c \"import urllib.request;"
            "urllib.request.urlopen('http://example.com', timeout=3)\"",
            timeout_sec=30,
        )
        assert result.returncode != 0, "network must be unreachable (--network none)"


def test_docker_sandbox_runs_pytest(tmp_path: Path, docker_mode):
    workspace = tmp_path / "ws"
    workspace.mkdir()
    (workspace / "test_smoke.py").write_text(
        "def test_math():\n    assert 1 + 1 == 2\n"
    )
    with sb.sandbox_for(workspace) as sandbox:
        result = sb.run_in_sandbox(sandbox, "pytest -q", timeout_sec=60)
        assert result.returncode == 0, result.stdout + result.stderr
        assert "1 passed" in result.stdout


def test_subprocess_mode_untouched(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("META_HARNESS_SANDBOX", "subprocess")
    workspace = tmp_path / "ws"
    workspace.mkdir()
    with sb.sandbox_for(workspace) as sandbox:
        assert sandbox not in sb._containers
        result = sb.run_in_sandbox(sandbox, "echo plain", timeout_sec=10)
        assert result.stdout.strip() == "plain"
