"""Engine replay of corpus.json: extractor-truth facts -> rule pack -> findings.

Every scenario - flagged rows AND controls - goes through the real engine
running rules/consistency-core.iql, in ONE knowledge graph. Corpus fact
ids, entities and events are namespaced per scenario (sid__x); constraint
ids/attrs are namespaced here. A finding maps back to its scenario through
the sid__ prefix of BOTH its ids, so a finding that pairs two scenarios is
reported as a leak instead of being silently credited to one of them.

The replay runs in two phases, like a conversation: every fact is
asserted, then each correction's `retractions` delete the superseded facts
exactly as inserted. Findings, violations and the presence of every fact a
scenario names are then observed per scenario.

Used by gate.py (the CI gate), full_bench.py and verify_each.py.
"""

import sys
from pathlib import Path

from evaluator import Expectation, Observation

POC_DIR = Path(__file__).resolve().parent
VC_DIR = POC_DIR.parent
REPO = VC_DIR.parent.parent.parent
PACK = VC_DIR / "rules" / "consistency-core.iql"
BATCH = 250  # tuples per insert statement

sys.path.insert(0, str(REPO / "packages" / "inputlayer-py" / "src"))


def esc(s):
    return str(s).replace('"', "'")


def load_pack_statements():
    return [line.strip() for line in PACK.read_text().splitlines()
            if line.strip() and not line.strip().startswith("//")]


def _lit(v):
    return str(v) if isinstance(v, int) else f'"{esc(v)}"'


def _tuple(values):
    body = ", ".join(_lit(v) for v in values)
    return f"({body},)" if len(values) == 1 else f"({body})"


class _Facts:
    """Rows to insert, per relation, kept in first-seen order."""

    def __init__(self):
        self.rel = {}

    def add(self, relation, *values):
        rows = self.rel.setdefault(relation, {})
        rows.setdefault(values, None)

    def statements(self):
        for relation, rows in self.rel.items():
            rows = list(rows)
            for i in range(0, len(rows), BATCH):
                chunk = ", ".join(_tuple(r) for r in rows[i:i + BATCH])
                yield f"+{relation}[{chunk}]"


def _fact_rows(f):
    """(relation, values) rows one corpus fact contributes."""
    cid, e, a, v = f["id"], f["entity"], f["attribute"], f["value"]
    rows = [("claim", (cid, e, a, v)),
            ("claim_modality", (cid, f["modality"]))]
    if "num" in f:
        rows.append(("claim_num", (cid, e, a, f["num"])))
    return rows


def scenario_inserts(sc, facts):
    """Queue every row scenario `sc` asserts."""
    sid = sc["id"]
    for f in sc.get("facts", []):
        for relation, values in _fact_rows(f):
            facts.add(relation, *values)
    for bid, a, b in sc.get("before", []):
        facts.add("before_claim", bid, a, b)
    for a, b in sc.get("same_as", []):
        facts.add("same_as", a, b)
    for kid, ktype, attr, val in sc.get("constraints", []):
        pk, pa = f"{sid}__{kid}", f"{sid}__{attr}"
        if ktype in ("max_value", "min_value"):
            facts.add("constraint_num", pk, ktype, pa, val)
        else:
            facts.add("constraint", pk, ktype, pa, val)
    for rel, arg in sc.get("ontology", []):
        # extractor-style ontology EXTENSION (never overrides seeds)
        facts.add(rel, arg)


def scenario_retractions(sc):
    """Delete statements for the facts this scenario's correction retracts."""
    by_id = {f["id"]: f for f in sc.get("facts", [])}
    out = []
    for target in sc.get("retractions", []):
        if target not in by_id:
            raise ValueError(f"{sc['id']}: retraction target {target} "
                             f"is not one of its facts")
        for relation, values in _fact_rows(by_id[target]):
            out.append(f"-{relation}{_tuple(values)}")
    return out


def expectation(sc):
    retracted = set(sc.get("retractions", []))
    kept = {f["id"] for f in sc.get("facts", [])} - retracted
    return Expectation(hard=frozenset(sc["expect_kinds"]),
                       soft=frozenset(sc.get("expect_soft", [])),
                       must_absent=frozenset(retracted),
                       must_present=frozenset(kept))


def has_data(sc):
    return any(sc.get(k) for k in ("facts", "before", "constraints",
                                   "same_as"))


def _owner(*ids):
    """The single scenario that owns every id, or None for a leak."""
    owners = {i.split("__", 1)[0] for i in ids}
    return owners.pop() if len(owners) == 1 else None


def _rows(kg, query):
    res = kg.execute(query)
    if res.truncated:
        raise RuntimeError(f"{query} truncated at {len(res.rows)} rows; "
                           f"raise max_result_rows for the gate server")
    return res.rows


def observe(kg, scenarios):
    """sid -> Observation for every scenario, plus attribution leaks."""
    known = {sc["id"] for sc in scenarios}
    hard, soft, viol, present = {}, {}, {}, {}
    leaks = []

    def credit(table, kind, *ids):
        sid = _owner(*ids)
        if sid not in known:
            leaks.append(f"finding {kind} {ids} spans scenarios or names "
                         f"an unknown one")
            return
        table.setdefault(sid, set()).add(kind)

    for kind, sev, c1, c2 in _rows(kg, "?finding(K, Sev, C1, C2)"):
        credit(hard if sev == "hard" else soft, kind, c1, c2)
    for kind, c, k in _rows(kg, "?violation(K, C, Kc)"):
        credit(viol, kind, c, k)
    for cid, *_ in _rows(kg, "?claim(Id, E, A, V)"):
        present.setdefault(cid.split("__", 1)[0], set()).add(cid)
    obs = {sid: Observation(hard=frozenset(hard.get(sid, ())),
                            soft=frozenset(soft.get(sid, ())),
                            violations=frozenset(viol.get(sid, ())),
                            present=frozenset(present.get(sid, ())))
           for sid in known}
    return obs, leaks


def replay(kg, scenarios):
    """Load the pack into `kg`, assert, retract, observe."""
    for stmt in load_pack_statements():
        kg.execute(stmt)
    facts = _Facts()
    for sc in scenarios:
        scenario_inserts(sc, facts)
    for stmt in facts.statements():
        kg.execute(stmt)
    for sc in scenarios:
        for stmt in scenario_retractions(sc):
            kg.execute(stmt)
    return observe(kg, scenarios)


def connect(server, api_key):
    from inputlayer.client_sync import InputLayerSync
    il = InputLayerSync(server, api_key=api_key)
    il.connect()
    return il


def replay_corpus(il, scenarios, kg_name="vc_gate_corpus"):
    """Run `scenarios` in a fresh KG on an open connection; drops it after."""
    try:
        il.drop_knowledge_graph(kg_name)
    except Exception:  # absent is the normal case
        pass
    kg = il.knowledge_graph(kg_name, create=True)
    try:
        return replay(kg, scenarios)
    finally:
        il.drop_knowledge_graph(kg_name)


def engine_pass(scenarios, server, api_key):
    """Per-scenario exact-match summary for full_bench/verify_each reports."""
    from evaluator import CONTROL, FLAG, Checkpoint, Row, judge
    il = connect(server, api_key)
    try:
        obs, leaks = replay_corpus(il, scenarios, "fb_corpus")
    finally:
        il.close()
    if leaks:
        raise RuntimeError(f"cross-scenario findings: {leaks[:5]}")
    out = {}
    for sc in scenarios:
        o = obs[sc["id"]]
        v = judge(Row("corpus", sc["id"], sc["family"],
                      CONTROL if sc["control"] else FLAG,
                      bool(sc.get("retractions")),
                      (Checkpoint(expectation(sc), o),), has_data(sc)))
        out[sc["id"]] = {
            "expected": sorted(sc["expect_kinds"]),
            "found": sorted(o.hard | o.violations),
            "soft": sorted(o.soft),
            "problems": [f"[{c}] {d}" for c, d in v.problems],
            "ok": v.ok and has_data(sc),
        }
    return out
