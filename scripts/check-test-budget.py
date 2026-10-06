#!/usr/bin/env python3
"""Check test run times against the tier budgets in tests/budget.toml.

Reads `cargo test` logs (each test binary's `Running <path> (...)` line and
the "finished in" time of its `test result:` line) and, optionally, the
snapshot spec runner's log (its `Specs finished in` line). Each binary is
mapped to a tier by its source path, each tier's time is summed, and the
check fails when a tier runs more than the budget's tolerance over its
budget. Writes the per-binary and per-tier times as JSON and prints a
markdown table.

Usage:
  scripts/check-test-budget.py --cargo-log LOG [--cargo-log LOG ...]
      [--specs-log LOG] [--budget tests/budget.toml]
      [--json tests-timing.json]

Exit status: 0 when every measured tier is within budget, 1 when one is
over (or a binary maps to no tier), 2 on bad input.
"""

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path

SCHEMA = "inputlayer.test-timing.v1"
ANSI = re.compile(r"\x1b\[[0-9;]*m")
# The timestamp GitHub Actions puts before every line of a downloaded job log.
TIMESTAMP = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z ")
RUNNING = re.compile(r"^\s*Running (?:unittests )?(\S+) \((.*)\)\s*$")
DOC_TESTS = re.compile(r"^\s*Doc-tests (\S+)\s*$")
RESULT = re.compile(
    r"^test result: \w+\. (\d+) passed; (\d+) failed;.*finished in ([0-9.]+)s\s*$"
)
SPECS = re.compile(r"^Specs finished in ([0-9.]+)s\s*$")


def clean(line):
    return TIMESTAMP.sub("", ANSI.sub("", line))


def binaries(log_text):
    """(path, binary, tests, failed, seconds) per test binary, in log order.

    A binary that crashed before its result line has `None` seconds.
    """
    found = []
    current = None
    for line in log_text.splitlines():
        line = clean(line)
        if m := RUNNING.match(line):
            if current:
                found.append({**current, "tests": 0, "failed": 0, "seconds": None})
            binary = re.sub(r"-[0-9a-f]+$", "", Path(m.group(2)).name)
            current = {"path": m.group(1), "binary": binary}
        elif m := DOC_TESTS.match(line):
            if current:
                found.append({**current, "tests": 0, "failed": 0, "seconds": None})
            current = {"path": "doc-tests", "binary": f"doc-tests {m.group(1)}"}
        elif (m := RESULT.match(line)) and current:
            found.append(
                {
                    **current,
                    "tests": int(m.group(1)),
                    "failed": int(m.group(2)),
                    "seconds": float(m.group(3)),
                }
            )
            current = None
    if current:
        found.append({**current, "tests": 0, "failed": 0, "seconds": None})
    return found


def tier_of(path, tiers):
    """The tier of the longest prefix of `path` in `tiers`; doc tests are unit."""
    if path == "doc-tests":
        return "unit"
    matches = [prefix for prefix in tiers if path.startswith(prefix)]
    return tiers[max(matches, key=len)] if matches else None


def specs_seconds(log_text):
    for line in log_text.splitlines():
        if m := SPECS.match(clean(line)):
            return float(m.group(1))
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--cargo-log", action="append", required=True, type=Path)
    parser.add_argument("--specs-log", type=Path)
    parser.add_argument("--budget", type=Path, default=Path("tests/budget.toml"))
    parser.add_argument("--json", type=Path, help="write tests-timing.json here")
    args = parser.parse_args()

    try:
        config = tomllib.loads(args.budget.read_text())
        budgets = config["budget"]
        tiers = config["tiers"]
        tolerance = float(config["tolerance"])
        found = [b for log in args.cargo_log for b in binaries(log.read_text())]
        specs = specs_seconds(args.specs_log.read_text()) if args.specs_log else None
    except (OSError, KeyError, ValueError, tomllib.TOMLDecodeError) as error:
        print(f"check-test-budget: {error}", file=sys.stderr)
        return 2
    if not found:
        print("check-test-budget: no test binaries in the cargo log", file=sys.stderr)
        return 2
    if args.specs_log and specs is None:
        print(
            f"check-test-budget: no 'Specs finished in' line in {args.specs_log}",
            file=sys.stderr,
        )
        return 2

    unmapped = []
    for binary in found:
        binary["tier"] = tier_of(binary["path"], tiers)
        if binary["tier"] is None:
            unmapped.append(binary["path"])
        elif binary["tier"] not in budgets:
            print(
                f"check-test-budget: tier '{binary['tier']}' of {binary['path']} has no budget",
                file=sys.stderr,
            )
            return 2

    summary = {}
    for tier, budget in budgets.items():
        members = [b for b in found if b["tier"] == tier]
        if tier == "specs":
            seconds = specs
        else:
            timed = [b["seconds"] for b in members if b["seconds"] is not None]
            seconds = round(sum(timed), 2) if members else None
        limit = round(budget * (1 + tolerance), 2)
        if seconds is None:
            status = "unmeasured"
        elif seconds > limit:
            status = "over"
        else:
            status = "ok"
        summary[tier] = {
            "budget_s": budget,
            "limit_s": limit,
            "seconds": seconds,
            "binaries": len(members),
            "tests": sum(b["tests"] for b in members),
            "status": status,
        }

    print("| Tier | Binaries | Tests | Seconds | Budget | Limit (+{:.0%}) | Status |".format(tolerance))
    print("|---|---:|---:|---:|---:|---:|---|")
    for tier, row in summary.items():
        seconds = "-" if row["seconds"] is None else f"{row['seconds']:.2f}"
        binaries_column = "-" if tier == "specs" else row["binaries"]
        tests = "-" if tier == "specs" else row["tests"]
        print(
            f"| {tier} | {binaries_column} | {tests} | {seconds} | {row['budget_s']} "
            f"| {row['limit_s']} | {row['status']} |"
        )

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(
            json.dumps(
                {
                    "schema": SCHEMA,
                    "tolerance": tolerance,
                    "tiers": summary,
                    "binaries": found,
                },
                indent=2,
            )
            + "\n"
        )

    failed = False
    for tier, row in summary.items():
        if row["status"] == "over":
            failed = True
            slowest = sorted(
                (b for b in found if b["tier"] == tier and b["seconds"] is not None),
                key=lambda b: -b["seconds"],
            )[:5]
            names = ", ".join(f"{b['path']} {b['seconds']:.1f}s" for b in slowest)
            print(
                f"\nOVER BUDGET: {tier} took {row['seconds']:.1f}s, budget {row['budget_s']}s "
                f"+{tolerance:.0%} = {row['limit_s']}s"
                + (f"; slowest: {names}" if names else ""),
                file=sys.stderr,
            )
    for path in unmapped:
        failed = True
        print(
            f"\nNO TIER: {path} matches no prefix in [tiers] of {args.budget}",
            file=sys.stderr,
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
