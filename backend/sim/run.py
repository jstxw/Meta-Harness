"""DST driver: run many seeds, print every failing seed.

    cd backend && uv run python -m sim.run --seeds 10000
    cd backend && uv run python -m sim.run --seed 4471 --mode unfenced_file -v

A failure without its seed is just a flaky test — the seed is always
printed, and a single ``--seed`` replays it exactly.
"""

from __future__ import annotations

import argparse
import sys

from sim.harness import SimParams, run_seed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Deterministic simulation runner")
    parser.add_argument("--seeds", type=int, default=1000, help="number of seeds")
    parser.add_argument("--start", type=int, default=0, help="first seed")
    parser.add_argument("--seed", type=int, default=None, help="replay ONE seed")
    parser.add_argument(
        "--mode",
        choices=["fenced_store", "unfenced_file"],
        default="fenced_store",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="print trace")
    args = parser.parse_args(argv)

    params = SimParams(protocol=args.mode)
    seeds = [args.seed] if args.seed is not None else range(
        args.start, args.start + args.seeds
    )

    failures = 0
    total = 0
    for seed in seeds:
        total += 1
        result = run_seed(seed, params)
        if not result.ok:
            failures += 1
            print(f"FAIL seed={seed} ({result.steps} steps)")
            for violation in result.violations:
                print(f"  {violation}")
            if args.verbose:
                for line in result.trace:
                    print(f"    {line}")
        elif args.verbose:
            print(f"ok seed={seed} ({result.steps} steps)")
        if total % 500 == 0:
            print(f"… {total} seeds, {failures} failures", file=sys.stderr)

    print(f"{total} seeds run, {failures} failed ({args.mode})")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
