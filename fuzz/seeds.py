#!/usr/bin/env python3
"""Write the fuzz targets' seed corpora to fuzz/seeds/<target>/.

Seeds are the repo's example IQL programs and their statements, the frames
of the /ws protocol, the deep-nesting and oversized-body cases of #295
around each limit (the default nesting limit 128, the ceiling 1024, and the
4,096-element body cap), and past findings from fuzz/regressions/. Generated, not committed: rerun after the examples
change. Usage: python3 fuzz/seeds.py
"""

import hashlib
import json
import pathlib

FUZZ = pathlib.Path(__file__).resolve().parent
REPO = FUZZ.parent
SEEDS = FUZZ / "seeds"

# Largest example program used as a seed; keeps libFuzzer's max_len sane.
MAX_PROGRAM_BYTES = 32 * 1024
DEPTHS = [1, 127, 128, 129, 1023, 1024, 1025, 4000]
BODY_ELEMENTS = [4095, 4096, 4097]


def nested_term(kind, depth):
    """One term `depth` levels deep (as in the parser's proptest)."""
    if kind == 0:
        return "abs(" * depth + "Y" + ")" * depth
    if kind == 1:
        groups = max(depth - 1, 0)
        return "(" * groups + "Y+1" + ")" * groups
    if kind == 2:
        return "Y" + "+1" * depth
    return "Y" + "%2" * depth


def deep_statements():
    """#295's deep input in every statement form, around each limit."""
    for depth in DEPTHS:
        for kind in range(4):
            term = nested_term(kind, depth)
            yield f"+r({term})"
            yield f"?r(Y), X = {term}"
            yield f"+p(X) <- r(Y), X = {term}"
            yield f"p(X) <- r(Y), {term} > X"
            yield f"-r(Y) <- r(Y), Y = {term}"
        # The audit's repro shape, and a constant inside it.
        yield "+nr(" + "abs(" * depth + "1" + ")" * depth + ")"
        # Type expressions and records nest too.
        yield "type T: " + "list[" * depth + "int" + "]" * depth + "."
        yield "+s(x: " + "list[" * depth + "int" + "]" * depth + ")."
        yield "type R: " + "{ a: " * depth + "int" + " }" * depth + "."


def oversized_bodies():
    """Rule and query bodies around the 4,096-element cap (#295)."""
    for elements in BODY_ELEMENTS:
        atoms = elements // 2  # a one-argument atom counts two elements
        joins = ", ".join(["e(X)"] * atoms)
        yield f"+p(X) <- {joins}"
        yield f"?e(X), {joins}"
        distinct = ", ".join(f"e{i}(X)" for i in range(atoms))
        yield f"p(X) <- {distinct}"
        negations = ", ".join(f"!n{i}(X)" for i in range(atoms))
        yield f"+p(X) <- e(X), {negations}"
        comparisons = ", ".join(f"X > {i}" for i in range(elements))
        yield f"?e(X), {comparisons}"


def examples():
    """(program, statements) of each example .iql file."""
    for path in sorted((REPO / "examples").rglob("*.iql")):
        text = path.read_text(encoding="utf-8", errors="replace")
        if len(text.encode()) > MAX_PROGRAM_BYTES:
            continue
        lines = [line.strip() for line in text.splitlines()]
        statements = [line for line in lines if line and not line.startswith(("%", "//"))]
        yield text, statements


def frames(programs):
    """/ws client frames: each frame type, then programs as executes."""
    yield {"type": "login", "id": "l1", "username": "admin", "password": "pw"}
    yield {"type": "authenticate", "api_key": "il_0123456789abcdef"}
    yield {"type": "ping", "id": "p1"}
    yield {"type": "cancel", "id": "c1", "target": "q1"}
    yield {"type": "read", "id": "r1", "timeout_ms": 250,
           "queries": [{"name": "a", "query": "?edge(X, Y)"},
                       {"name": "b", "query": "?node(X)"}]}
    yield {"type": "subscribe", "id": "s1", "subscription": "g",
           "queries": [{"name": "a", "query": "?edge(X, Y)"}]}
    yield {"type": "execute", "program": ".subscribe s ?edge(X, Y)"}
    yield {"type": "execute", "program": ".unsubscribe s"}
    yield {"type": "execute", "id": "w1", "program": "+edge(1, 2)",
           "expect_revision": 3, "expect_relations": ["edge"], "expect_epoch": "00ff"}
    yield {"type": "execute", "id": "q1", "program": "?edge($a, Y)",
           "params": {"a": 1, "f": 1.5, "s": "x", "b": True, "v": [0.5, 2]}}
    for program in programs:
        yield {"type": "execute", "id": "x", "program": program}


def regressions(target):
    """Inputs that once failed `target`, kept in fuzz/regressions/<target>/."""
    for path in sorted((FUZZ / "regressions" / target).glob("*")):
        yield path.read_text(encoding="utf-8")


def write(target, items):
    out = SEEDS / target
    out.mkdir(parents=True, exist_ok=True)
    seen = set()
    for item in items:
        data = item.encode()
        name = hashlib.sha1(data).hexdigest()
        if name in seen:
            continue
        seen.add(name)
        (out / name).write_bytes(data)
    print(f"{target}: {len(seen)} seeds")


def main():
    corpus = list(examples())
    programs = [program for program, _ in corpus]
    statements = [s for _, lines in corpus for s in lines]
    deep = list(deep_statements()) + list(oversized_bodies())

    fixed = list(regressions("iql_statement"))
    write("iql_statement", statements + deep + fixed)
    params = '\0{"a": 1, "f": 2.5e-3, "s": "S-77", "b": false, "v": [0.5, -1e2]}'
    write(
        "iql_program",
        programs + deep + fixed + ["?edge($a, Y)" + params, "+r($s, $f)" + params],
    )
    frames_in = programs + deep + fixed
    write("ws_client_frame", [json.dumps(frame) for frame in frames(frames_in)])


if __name__ == "__main__":
    main()
