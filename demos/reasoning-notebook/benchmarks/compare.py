#!/usr/bin/env python3
"""Compare benchmark results and generate a report.

Usage:
    uv run python compare.py                           # latest results
    uv run python compare.py results/benchmark_*.json  # specific file
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def load_results(path: Path) -> list[dict]:
    return json.loads(path.read_text())


def print_comparison(results: list[dict]) -> None:
    inputs = sorted({r["input"] for r in results})
    models = sorted({r["model"] for r in results})

    print(f"\n{'=' * 80}")
    print(f"  Benchmark Comparison — {len(results)} runs across {len(models)} models")
    print(f"{'=' * 80}\n")

    for input_name in inputs:
        input_results = [r for r in results if r["input"] == input_name]
        input_type = input_results[0].get("type", "?")
        print(f"  Input: {input_name} ({input_type})")
        print(f"  {'Model':<28} {'Ent':>5} {'Rel':>5} {'Time':>7} {'Status':<12}")
        print(f"  {'-'*28} {'-'*5} {'-'*5} {'-'*7} {'-'*12}")

        for r in sorted(input_results, key=lambda x: x.get("time_seconds", 999)):
            if not r["success"]:
                print(f"  {r['model']:<28} {'—':>5} {'—':>5} {'—':>7} FAILED")
                continue
            ents = r.get("entities_count", 0)
            rels = r.get("relationships_count", 0)
            t = f"{r['time_seconds']}s"
            print(f"  {r['model']:<28} {ents:>5} {rels:>5} {t:>7} OK")

        # Entity comparison
        print(f"\n  Entities extracted:")
        for r in sorted(input_results, key=lambda x: x["model"]):
            if not r["success"]:
                continue
            ent_names = sorted({e["name"] for e in r.get("entities", [])})
            print(f"    {r['model']:<25} {', '.join(ent_names[:10])}")

        # Image-specific fields
        if input_type == "image":
            print(f"\n  Scene analysis:")
            for r in sorted(input_results, key=lambda x: x["model"]):
                if not r["success"]:
                    continue
                print(f"    {r['model']:<25}")
                for field in ["scene", "emotion", "event_type", "aesthetic", "caption_seed"]:
                    val = r.get(field, "")
                    if val:
                        print(f"      {field:<16} {val[:60]}")

        print()

    # Overall summary
    print(f"{'=' * 80}")
    print(f"  Overall Summary")
    print(f"{'=' * 80}\n")
    print(f"  {'Model':<28} {'Avg Ent':>8} {'Avg Rel':>8} {'Avg Time':>9} {'Success':>8}")
    print(f"  {'-'*28} {'-'*8} {'-'*8} {'-'*9} {'-'*8}")

    for model in models:
        model_results = [r for r in results if r["model"] == model]
        successes = [r for r in model_results if r["success"]]
        if not successes:
            print(f"  {model:<28} {'—':>8} {'—':>8} {'—':>9} {f'0/{len(model_results)}':>8}")
            continue
        avg_ents = sum(r.get("entities_count", 0) for r in successes) / len(successes)
        avg_rels = sum(r.get("relationships_count", 0) for r in successes) / len(successes)
        avg_time = sum(r["time_seconds"] for r in successes) / len(successes)
        print(
            f"  {model:<28} {avg_ents:>8.1f} {avg_rels:>8.1f} {avg_time:>8.1f}s "
            f"{f'{len(successes)}/{len(model_results)}':>8}"
        )
    print()


if __name__ == "__main__":
    results_dir = Path(__file__).parent / "results"

    if len(sys.argv) > 1:
        path = Path(sys.argv[1])
    else:
        files = sorted(results_dir.glob("benchmark_*.json"))
        if not files:
            print("No benchmark results found. Run run_benchmark.py first.")
            sys.exit(1)
        path = files[-1]

    print(f"Loading: {path}")
    results = load_results(path)
    print_comparison(results)
