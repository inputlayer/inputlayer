#!/usr/bin/env python3
"""Deterministic false-alarm/revision gate for Verified Completions (#88).

Replays, against a live engine and the repo rule pack, with no model calls:

  corpus.json     every scenario's extractor-truth facts - flagged rows AND
                  controls (corrections retract the superseded fact)
  benchmark.json  the recorded reference extractions, batch by batch through
                  the ingestion validator (quotes verbatim, ids in batch)

and scores both with one contract (evaluator.py). Prints per-family
detection, the false-alarm rate over control rows and revision fidelity over
correction rows; exits nonzero on any row problem or contract failure.
CI runs it through scripts/run_vc_gate.sh on PRs touching the rule pack,
extraction prompts, the corpus/evaluator, or gateway ingestion.

Usage:
  python3 gate.py --server ws://127.0.0.1:8093/ws --api-key KEY [--json OUT]
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

POC_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(POC_DIR))

import corpus as corpus_gen  # noqa: E402
from engine_replay import (REPO, connect, expectation, has_data,  # noqa: E402
                           replay_corpus)
from evaluator import (CONTROL, FLAG, Checkpoint, Row, evaluate,  # noqa: E402
                       missing_rows, render)
from poc_verify import replay_benchmark  # noqa: E402


def corpus_sync_failures(scenarios):
    """corpus.json must be exactly what corpus.py generates: the gate scores
    the generator's contract, not a hand-edited copy."""
    generated = json.loads(json.dumps(
        corpus_gen.attach_labels(corpus_gen.scenarios())))
    if generated == scenarios:
        return []
    gen_ids = {s["id"] for s in generated}
    have_ids = {s["id"] for s in scenarios}
    diff = sorted((gen_ids ^ have_ids) or {
        s["id"] for s, g in zip(scenarios, generated) if s != g})
    return [f"corpus.json is out of sync with corpus.py "
            f"(regenerate: python3 corpus.py); differs at {diff[:8]}"]


def corpus_rows(scenarios, observations):
    rows = []
    for sc in scenarios:
        if sc["id"] not in observations:
            continue  # reported by missing_rows
        rows.append(Row(
            "corpus", sc["id"], sc["family"],
            CONTROL if sc["control"] else FLAG,
            bool(sc.get("retractions")),
            (Checkpoint(expectation(sc), observations[sc["id"]]),),
            has_data(sc)))
    return rows


def resolve_api_key(arg):
    if arg:
        return arg
    if os.environ.get("INPUTLAYER_API_KEY"):
        return os.environ["INPUTLAYER_API_KEY"]
    cred = REPO / ".inputlayer-credentials.toml"
    if cred.exists():
        m = re.search(r'^api_key\s*=\s*"([^"]+)"', cred.read_text(), re.M)
        if m:
            return m.group(1)
    sys.exit("no InputLayer API key: --api-key, INPUTLAYER_API_KEY or "
             ".inputlayer-credentials.toml")


def report_json(report):
    return {
        "passed": report.passed,
        "metrics": report.metrics,
        "contract": report.contract,
        "gaps": report.gaps,
        "failures": {f"{v.row.source}:{v.row.row_id}": v.problems
                     for v in report.verdicts if not v.ok},
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--server", default="ws://127.0.0.1:8080/ws")
    ap.add_argument("--api-key", default=None)
    ap.add_argument("--json", default=None,
                    help="also write the machine-readable report here")
    args = ap.parse_args()
    api_key = resolve_api_key(args.api_key)

    scenarios = json.loads((POC_DIR / "corpus.json").read_text())["scenarios"]
    bench = json.loads((POC_DIR / "benchmark.json").read_text())["rows"]
    extra = corpus_sync_failures(scenarios)

    t0 = time.monotonic()
    il = connect(args.server, api_key)
    try:
        observations, leaks = replay_corpus(il, scenarios)
    finally:
        il.close()
    t1 = time.monotonic()
    results, gaps = replay_benchmark(args.server, api_key, bench)
    t2 = time.monotonic()

    extra += leaks
    extra += missing_rows([s["id"] for s in scenarios], observations,
                          "corpus")
    scored = [r["id"] for r in bench if r["status"] != "GAP" and r["batches"]]
    extra += missing_rows(scored, [r["id"] for r in results], "benchmark")
    rows = corpus_rows(scenarios, observations) + [r["verdict"].row
                                                    for r in results]
    report = evaluate(rows, gaps, extra)

    print(f"=== Verified Completions gate: {len(scenarios)} corpus "
          f"scenarios ({t1 - t0:.1f}s), {len(results)} recorded-extraction "
          f"rows ({t2 - t1:.1f}s) ===")
    print(render(report))
    if args.json:
        Path(args.json).write_text(json.dumps(report_json(report), indent=1))
    sys.exit(0 if report.passed else 1)


if __name__ == "__main__":
    main()
