"""Deterministic simulation tests (REPOSITIONING_PLAN Phase 3).

Fast pytest slice of the DST suite. The full sweep is::

    cd backend && uv run python -m sim.run --seeds 10000
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sim.harness import SimParams, run_seed  # noqa: E402


def test_fenced_protocol_passes_seed_sweep():
    """I1–I7 hold across 200 seeds under crash/stall/cancel/skew faults."""
    for seed in range(200):
        result = run_seed(seed, SimParams(protocol="fenced_store"))
        assert result.ok, f"seed={seed}: {result.violations}"


def test_simulation_is_deterministic():
    """Same seed → identical trace and identical verdict, twice."""
    a = run_seed(7, SimParams(protocol="unfenced_file"))
    b = run_seed(7, SimParams(protocol="unfenced_file"))
    assert a.trace == b.trace
    assert a.violations == b.violations
    assert a.steps == b.steps


def test_replay_export_is_deterministic_and_frame_recording_is_inert():
    """The exported trace is a pure function of (seed, mode): identical
    across exports, and recording frames must not change the schedule
    or the verdict (4.2 depends on both)."""
    from sim.export import export_seed

    a = export_seed(7, "unfenced_file")
    b = export_seed(7, "unfenced_file")
    assert a == b
    assert a["frames"], "frames must be populated for the viewer"
    assert any(f["new_violations"] for f in a["frames"]), (
        "the I1 violation must be attributed to a frame"
    )

    without_frames = run_seed(7, SimParams(protocol="unfenced_file"))
    assert without_frames.violations == a["violations"]
    assert without_frames.steps == a["steps"]


def test_dst1_documented_bug_seed_7_unfenced_double_append():
    """Regression pin for DST-1 (docs/INVARIANTS.md): the historical
    unfenced check-then-append protocol double-appends when a stalled
    worker wakes past a reclaimed lease. Seed 7 reproduces it; the
    fenced protocol is clean on the same seed."""
    buggy = run_seed(7, SimParams(protocol="unfenced_file"))
    assert any(v.startswith("I1") for v in buggy.violations), buggy.violations

    fixed = run_seed(7, SimParams(protocol="fenced_store"))
    assert fixed.ok, fixed.violations


def test_dst2_documented_zombie_checkpoint_seed_9270():
    """Regression pin for DST-2: seed 9270 exercises the zombie
    trailing-checkpoint write; the run still satisfies I1–I7 because the
    authoritative store log is fenced. The zombie write itself is
    surfaced in the trace."""
    result = run_seed(9270, SimParams(protocol="fenced_store"))
    assert result.ok, result.violations
