#!/usr/bin/env python3
"""Verified Completions proof of concept: conversation -> facts -> findings.

This runs the product's core loop end to end:

  1. EXTRACT   a conversation becomes atomic claims (Claude Haiku in --live
               mode; the benchmark's recorded reference extractions in
               --reference mode, which needs no API key)
  2. VALIDATE  every claim must quote its source verbatim; claims that fail
               validation are dropped and counted, never inserted
  3. LOAD      validated claims become facts in a per-conversation knowledge
               graph running the consistency-core rule pack
  4. JUDGE     the engine derives findings; we compare them to what each
               benchmark row says must (or must NOT) be found

Usage:
  python3 poc_verify.py --reference                  # engine + validator, no key
  ANTHROPIC_API_KEY=... python3 poc_verify.py --live # full loop with Claude
  python3 poc_verify.py --reference --rows A1,B4     # subset
  python3 poc_verify.py --reference --verbose        # per-row detail

Server: needs a running InputLayer server whose WS rate limit is off
(the pack is ~90 statements per conversation KG). Start one with the same
config the snapshot harness generates, or pass --server / --api-key.
"""

import argparse
import json
import re
import sys
from pathlib import Path

POC_DIR = Path(__file__).resolve().parent
VC_DIR = POC_DIR.parent
REPO = VC_DIR.parent.parent.parent
PACK = VC_DIR / "rules" / "consistency-core.iql"
LIFECYCLE_PROMPT = VC_DIR / "extraction" / "fact-lifecycle-prompt.md"
CLAIM_SCHEMA = VC_DIR / "extraction" / "claim-schema.json"

sys.path.insert(0, str(REPO / "packages" / "inputlayer-py" / "src"))
from inputlayer.client_sync import InputLayerSync  # noqa: E402

MODALITIES = {"asserted", "negated", "hedged", "conditional", "opinion", "question"}
CONSTRAINT_TYPES = {"forbid", "require", "max_value", "min_value", "persona"}
# Seeded ontology entries the extractor may never override (mirror of pack section 9)
SEEDED_FUNCTIONAL = {
    "departure_date", "departure_city", "return_date", "arrival_city", "age",
    "birth_date", "death_date", "start_date", "end_date", "check_in",
    "check_out", "total_price", "capacity", "capital_of", "ceo_of",
    "headquartered_in", "assistant_identity", "member_count",
}
DATE_ATTRS_SUFFIX = "_date"
DATE_ATTRS_EXTRA = {"check_in", "check_out"}


def esc(s):
    return str(s).replace('"', "'")


def numeric_mirror(attribute, value):
    """Return the int the gateway mirrors into claim_num/constraint_num, or None.

    Dates ride as YYYYMMDD ints because the engine cannot order-compare
    strings variable to variable (see pack header). Quantities keep their
    leading number; bare years on date attributes become Jan 1 of that year.
    """
    v = value.strip()
    is_date_attr = attribute.endswith(DATE_ATTRS_SUFFIX) or attribute in DATE_ATTRS_EXTRA
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", v)
    if m:
        return int(m.group(1)) * 10000 + int(m.group(2)) * 100 + int(m.group(3))
    if is_date_attr and re.match(r"^\d{4}$", v):
        return int(v) * 10000 + 101
    m = re.match(r"^(\d+)(\s|$)", v)
    if m:
        return int(m.group(1))
    return None


class Validator:
    """The gateway's ingestion gate: shape is the schema's job, THIS enforces meaning.

    A claim that fails any check is dropped and counted - it never reaches
    the engine, so a hallucinating extractor cannot create false findings.
    """

    def __init__(self, messages_by_idx):
        self.messages = messages_by_idx
        self.seen_ids = set()
        self.rejects = []

    def reject(self, item_id, reason):
        self.rejects.append((item_id, reason))

    def check_common(self, item, batch_msgs):
        iid = item.get("id", "<missing id>")
        if iid in self.seen_ids:
            self.reject(iid, "duplicate id")
            return False
        msg = item.get("msg")
        if msg not in batch_msgs:
            self.reject(iid, f"msg {msg} outside extracted batch {sorted(batch_msgs)}")
            return False
        surface = item.get("surface", "")
        if not surface or surface not in self.messages.get(msg, ""):
            self.reject(iid, f"surface not a verbatim substring of message {msg}: {surface!r}")
            return False
        self.seen_ids.add(iid)
        return True

    def valid_claim(self, c, batch_msgs):
        if not self.check_common(c, batch_msgs):
            return False
        if c.get("modality") not in MODALITIES:
            self.reject(c["id"], f"bad modality {c.get('modality')!r}")
            self.seen_ids.discard(c["id"])
            return False
        return True

    def valid_constraint(self, k, batch_msgs):
        if not self.check_common(k, batch_msgs):
            return False
        if k.get("type") not in CONSTRAINT_TYPES:
            self.reject(k["id"], f"bad constraint type {k.get('type')!r}")
            self.seen_ids.discard(k["id"])
            return False
        return True

    def valid_ontology(self, ontology):
        """Extending is allowed, overriding seeded entries is not."""
        out = []
        for attr in ontology.get("functional", []):
            if attr in SEEDED_FUNCTIONAL:
                self.reject(attr, "attempt to redeclare seeded functional attribute")
            else:
                out.append(attr)
        return out


class Session:
    """One conversation = one knowledge graph with the rule pack loaded."""

    def __init__(self, il, name, pack_statements):
        self.il = il
        self.kg = il.knowledge_graph(name, create=True)
        for stmt in pack_statements:
            self.kg.execute(stmt)
        # id -> full inserted tuple, so retractions can name the exact fact
        self.claims = {}
        self.constraints = {}
        self.befores = {}
        self.claim_nums = {}
        self.constraint_nums = {}

    def x(self, stmt):
        return self.kg.execute(stmt)

    def insert_claim(self, c, origin):
        cid, e, a, v = c["id"], c["entity"], c["attribute"], c["value"]
        self.x(f'+claim[("{cid}", "{e}", "{a}", "{esc(v)}")]')
        self.x(f'+claim_modality[("{cid}", "{c["modality"]}")]')
        self.x(f'+claim_source[("{cid}", {c["msg"]}, "{esc(c["surface"])}")]')
        self.x(f'+claim_origin[("{cid}", "{origin}")]')
        self.claims[cid] = (cid, e, a, v, c["modality"], origin)
        n = numeric_mirror(a, v)
        if n is not None:
            self.x(f'+claim_num[("{cid}", "{e}", "{a}", {n})]')
            self.claim_nums[cid] = (cid, e, a, n)

    def insert_before(self, b):
        bid = b["id"]
        self.x(f'+before_claim[("{bid}", "{b["event_a"]}", "{b["event_b"]}")]')
        self.x(f'+claim_source[("{bid}", {b["msg"]}, "{esc(b["surface"])}")]')
        self.befores[bid] = (bid, b["event_a"], b["event_b"])

    def insert_constraint(self, k):
        kid, t, a, v = k["id"], k["type"], k["attr"], k["value"]
        if t in ("max_value", "min_value"):
            n = numeric_mirror(a, v)
            if n is None:
                return False
            self.x(f'+constraint_num[("{kid}", "{t}", "{a}", {n})]')
            self.constraint_nums[kid] = (kid, t, a, n)
        else:
            self.x(f'+constraint[("{kid}", "{t}", "{a}", "{esc(v)}")]')
            self.constraints[kid] = (kid, t, a, v)
        self.x(f'+constraint_source[("{kid}", {k["msg"]}, "{esc(k["surface"])}")]')
        return True

    def retract(self, r):
        """Belief revision: delete the exact facts previously inserted under this id."""
        t = r["target"]
        if t in self.claims:
            cid, e, a, v, modality, origin = self.claims.pop(t)
            self.x(f'-claim("{cid}", "{e}", "{a}", "{esc(v)}")')
            self.x(f'-claim_modality("{cid}", "{modality}")')
            self.x(f'-claim_origin("{cid}", "{origin}")')
            if cid in self.claim_nums:
                _, e2, a2, n = self.claim_nums.pop(cid)
                self.x(f'-claim_num("{cid}", "{e2}", "{a2}", {n})')
            return True
        if t in self.constraints:
            kid, ct, a, v = self.constraints.pop(t)
            self.x(f'-constraint("{kid}", "{ct}", "{a}", "{esc(v)}")')
            return True
        if t in self.constraint_nums:
            kid, ct, a, n = self.constraint_nums.pop(t)
            self.x(f'-constraint_num("{kid}", "{ct}", "{a}", {n})')
            return True
        if t in self.befores:
            bid, ea, eb = self.befores.pop(t)
            self.x(f'-before_claim("{bid}", "{ea}", "{eb}")')
            return True
        return False  # no matching target: retract nothing (lifecycle prompt section 4)

    def findings(self):
        hard, soft = set(), set()
        pairs = set()
        for row in self.x("?finding(K, Sev, C1, C2)").rows:
            kind, sev, c1, c2 = row
            key = (kind, tuple(sorted((c1, c2))))
            if key in pairs:
                continue  # mirrored pair dedup, like the response assembler
            pairs.add(key)
            (hard if sev == "hard" else soft).add(kind)
        violations = {row[0] for row in self.x("?violation(K, C, Kc)").rows}
        return hard, soft, violations

    def claim_present(self, cid):
        if cid in self.claims:
            return len(self.x(f'?claim("{cid}", E, A, V)').rows) > 0
        if cid in self.constraints:
            return len(self.x(f'?constraint("{cid}", T, A, V)').rows) > 0
        if cid in self.constraint_nums:
            return len(self.x(f'?constraint_num("{cid}", T, A, N)').rows) > 0
        return False

    def drop(self):
        self.il.drop_knowledge_graph(self.kg.name)


def load_pack_statements():
    stmts = []
    for line in PACK.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("//"):
            stmts.append(line)
    return stmts


def extract_live(row, batch, prior_batches, current_date, model):
    """The real thing: Claude turns conversation text into claims."""
    import anthropic

    prompt_doc = LIFECYCLE_PROMPT.read_text()
    system = prompt_doc.split("```", 2)[1].lstrip("\n")  # the fenced prompt block
    claims_digest = "\n".join(
        f"{c['id']} - {c['entity']} - {c['attribute']} - {c['value']} - {c['modality']}"
        for pb in prior_batches
        for c in pb.get("claims", [])
    ) or "(none)"
    prior = "\n".join(
        f"[msg {m['idx']}] {m['role']}: {m['content']}"
        for m in row["messages"] if m["idx"] not in batch["messages"]
    ) or "(none)"
    new = "\n".join(
        f"[msg {m['idx']}] {m['role']}: {m['content']}"
        for m in row["messages"] if m["idx"] in batch["messages"]
    )
    task = ("extract_assistant_output" if batch.get("origin") == "output"
            else "extract_prompt_suffix")
    system = (system
              .replace("{{current_date}}", current_date)
              .replace("{{extract_prompt_suffix | extract_assistant_output}}", task)
              .replace("{{claims_digest}}", claims_digest)
              .replace("{{prior_messages}}", prior)
              .replace("{{new_messages_with_indices}}", new))

    asserts_schema = json.loads(CLAIM_SCHEMA.read_text())
    asserts_schema.pop("$comment", None)
    wrapper = {
        "type": "object", "additionalProperties": False,
        "required": ["asserts", "retractions"],
        "properties": {
            "asserts": asserts_schema,
            "retractions": {"type": "array", "items": {
                "type": "object", "additionalProperties": False,
                "required": ["target", "kind", "msg", "surface"],
                "properties": {
                    "target": {"type": "string"},
                    "kind": {"type": "string", "enum": ["claim", "constraint", "before"]},
                    "msg": {"type": "integer"},
                    "surface": {"type": "string"}}}},
        },
    }
    client = anthropic.Anthropic()
    kwargs = dict(model=model, max_tokens=4096, temperature=0, system=system,
                  messages=[{"role": "user", "content": "Extract now."}])
    try:
        resp = client.messages.create(
            output_config={"format": {"type": "json_schema", "schema": wrapper}}, **kwargs)
        out = json.loads(resp.content[0].text)
    except TypeError:  # SDK without output_config: fall back to prompted JSON
        kwargs["system"] += "\n\nReturn ONLY a JSON object {\"asserts\": {...}, \"retractions\": [...]} matching the documented schema."
        resp = client.messages.create(**kwargs)
        raw = resp.content[0].text
        out = json.loads(raw[raw.find("{"): raw.rfind("}") + 1])
    a = out.get("asserts", {})
    return {
        "claims": a.get("claims", []),
        "before_claims": a.get("before_claims", []),
        "constraints": a.get("constraints", []),
        "ontology": a.get("ontology", {}),
        "retractions": out.get("retractions", []),
    }


def run_row(il, row, pack_statements, mode, current_date, model, verbose):
    sess = Session(il, f"poc_{row['id'].lower()}", pack_statements)
    result = {"id": row["id"], "status": row["status"], "title": row["title"],
              "ok": True, "notes": [], "rejects": []}
    try:
        for pair in row.get("setup_same_as", []):
            sess.x(f'+same_as[("{pair[0]}", "{pair[1]}")]')

        messages_by_idx = {m["idx"]: m["content"] for m in row["messages"]}
        prior_refs = []
        for batch in row["batches"]:
            ext = (extract_live(row, batch, prior_refs, current_date, model)
                   if mode == "live" else dict(batch.get("reference", {})))
            prior_refs.append(ext)
            v = Validator(messages_by_idx)
            batch_msgs = set(batch["messages"])
            origin = batch.get("origin", "prompt")

            for c in ext.get("claims", []):
                if v.valid_claim(c, batch_msgs):
                    sess.insert_claim(c, origin)
            for b in ext.get("before_claims", []):
                if v.check_common(b, batch_msgs):
                    sess.insert_before(b)
            for k in ext.get("constraints", []):
                if v.valid_constraint(k, batch_msgs):
                    if not sess.insert_constraint(k):
                        v.reject(k["id"], "numeric constraint with non-numeric value")
            for attr in v.valid_ontology(ext.get("ontology", {})):
                sess.x(f'+functional[("{attr}",)]')
            for r in ext.get("retractions", []):
                if not sess.retract(r):
                    result["notes"].append(f"retraction target {r['target']!r} not found; nothing retracted")
            result["rejects"].extend(v.rejects)

            hard, soft, violations = sess.findings()
            exp = batch.get("expect", {})
            missing_hard = set(exp.get("hard", [])) - hard
            missing_viol = set(exp.get("violations", [])) - violations
            if missing_hard or missing_viol:
                result["ok"] = False
                result["notes"].append(
                    f"missed findings: hard {sorted(missing_hard)} violations {sorted(missing_viol)} "
                    f"(got hard {sorted(hard)}, violations {sorted(violations)})")
            if row["status"] == "CONTROL":
                extra = hard | violations
                allowed = set(exp.get("hard", [])) | set(exp.get("violations", []))
                if extra - allowed:
                    result["ok"] = False
                    result["notes"].append(f"FALSE ALARM: {sorted(extra - allowed)}")
            else:
                extra = (hard - set(exp.get("hard", []))) | (violations - set(exp.get("violations", [])))
                if extra:
                    result["notes"].append(f"extra findings (review): {sorted(extra)}")
            missing_soft = set(exp.get("soft", [])) - soft
            if missing_soft:
                result["notes"].append(f"expected soft tension not seen: {sorted(missing_soft)}")
            for cid in exp.get("must_absent", []):
                if sess.claim_present(cid):
                    result["ok"] = False
                    result["notes"].append(f"retraction failed: {cid} still in the graph")
            for cid in exp.get("must_present", []):
                if not sess.claim_present(cid):
                    result["ok"] = False
                    result["notes"].append(f"over-retraction: {cid} missing from the graph")
        if verbose:
            tag = "PASS" if result["ok"] else "FAIL"
            print(f"  {tag} {row['id']} ({row['status']}) {row['title']}")
            for n in result["notes"]:
                print(f"        note: {n}")
            for rid, reason in result["rejects"]:
                print(f"        validator dropped {rid}: {reason}")
    finally:
        sess.drop()
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--reference", action="store_true",
                   help="use the benchmark's recorded extractions (no API key)")
    g.add_argument("--live", action="store_true",
                   help="extract with Claude (needs ANTHROPIC_API_KEY)")
    ap.add_argument("--server", default="ws://127.0.0.1:8080/ws")
    ap.add_argument("--api-key", default=None, help="InputLayer API key (default: .inputlayer-credentials.toml)")
    ap.add_argument("--model", default="claude-haiku-4-5")
    ap.add_argument("--rows", default=None, help="comma-separated row ids, e.g. A1,B4")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    bench = json.loads((POC_DIR / "benchmark.json").read_text())
    rows = bench["rows"]
    if args.rows:
        wanted = {r.strip().upper() for r in args.rows.split(",")}
        rows = [r for r in rows if r["id"] in wanted]

    api_key = args.api_key
    if not api_key:
        cred = REPO / ".inputlayer-credentials.toml"
        if cred.exists():
            m = re.search(r'^api_key\s*=\s*"([^"]+)"', cred.read_text(), re.M)
            api_key = m.group(1) if m else None
    if not api_key:
        sys.exit("No InputLayer API key: pass --api-key or create .inputlayer-credentials.toml")

    pack_statements = load_pack_statements()
    mode = "live" if args.live else "reference"
    current_date = bench["meta"]["current_date"]

    il = InputLayerSync(args.server, api_key=api_key)
    il.connect()
    results, gaps = [], []
    try:
        for row in rows:
            if row["status"] == "GAP" or not row["batches"]:
                gaps.append(row)
                continue
            results.append(run_row(il, row, pack_statements, mode, current_date,
                                   args.model, args.verbose))
    finally:
        il.close()

    flag_rows = [r for r in results if r["status"] in ("CORE", "NUM", "M3")]
    control_rows = [r for r in results if r["status"] == "CONTROL"]
    detected = [r for r in flag_rows if r["ok"]]
    clean = [r for r in control_rows if r["ok"]]

    print()
    print(f"=== Verified Completions PoC - {mode} mode ===")
    by_family = {}
    for r in results:
        by_family.setdefault(r["id"][0], []).append(r)
    for fam in sorted(by_family):
        rs = by_family[fam]
        line = " ".join(f"{r['id']}:{'PASS' if r['ok'] else 'FAIL'}" for r in rs)
        print(f"  Family {fam}: {line}")
    print()
    print(f"  Detection: {len(detected)}/{len(flag_rows)} planted problems found "
          f"({', '.join(r['id'] for r in flag_rows if not r['ok']) or 'none missed'})")
    print(f"  False alarms: {len(control_rows) - len(clean)}/{len(control_rows)} control rows flagged "
          f"({', '.join(r['id'] for r in control_rows if not r['ok']) or 'zero false alarms'})")
    print(f"  Declared gaps (not scored): {len(gaps)}")
    total_rejects = sum(len(r["rejects"]) for r in results)
    if total_rejects:
        print(f"  Validator drops: {total_rejects} (run --verbose for details)")
    failed = [r for r in results if not r["ok"]]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
