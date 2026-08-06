"""Export a deterministic simulation trace as JSON for the replay viewer.

    cd backend && uv run python -m sim.export --seed 7 --mode unfenced_file \
        -o ../frontend/dashboard/public/replays/seed-7-unfenced.json

The output is a pure function of (seed, mode, params): re-exporting the
same seed yields byte-identical JSON, so a trace file IS the bug report.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from pathlib import Path

from sim.harness import SimParams, run_seed


def export_seed(seed: int, mode: str) -> dict:
    params = SimParams(protocol=mode)
    result = run_seed(seed, params, record_frames=True)
    return {
        "seed": seed,
        "mode": mode,
        "params": dataclasses.asdict(params),
        "ok": result.ok,
        "steps": result.steps,
        "violations": result.violations,
        "frames": result.frames,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export one seed's replay trace")
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument(
        "--mode",
        choices=["fenced_store", "unfenced_file"],
        default="fenced_store",
    )
    parser.add_argument("-o", "--out", default="-", help="output path or - for stdout")
    args = parser.parse_args(argv)

    payload = json.dumps(export_seed(args.seed, args.mode), indent=1)
    if args.out == "-":
        print(payload)
    else:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(payload)
        print(f"wrote {out} ({len(payload)} bytes)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
