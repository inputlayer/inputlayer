"""The evaluator contract for the deterministic false-alarm/revision gate.

One scorer for every source the gate replays (corpus.json scenarios and
benchmark.json recorded extractions). A source turns each row into
checkpoints - what must be true (Expectation) next to what the engine
reported (Observation) - and everything below is pure: no server, no SDK,
so the contract itself is unit-tested (test_evaluator.py).

Per row the verdict is exact:

  missed         an expected hard finding or violation did not fire
  false_alarm    a control row raised a hard finding or violation it
                 does not expect
  drift          a flagged row raised an unexpected hard kind, or any row's
                 soft tensions differ from the expected set
  stale          a retracted fact is still in the graph
  over_retracted a fact that must survive is gone

The three reported metrics:

  detection          per family, flagged rows without a `missed` problem
  false-alarm rate   control rows with a `false_alarm` problem
  revision fidelity  revision rows with no stale/over_retracted/false_alarm

The gate fails on ANY row problem and on any contract failure: a missing
scenario, a vacuous row (a control with no facts proves nothing), a
required family absent or under its minimum size, or a metric with an
empty denominator. A gate that cannot evaluate something must not pass.
"""

from dataclasses import dataclass, field

# Problem categories (see module docstring).
MISSED = "missed"
FALSE_ALARM = "false_alarm"
DRIFT = "drift"
STALE = "stale"
OVER_RETRACTED = "over_retracted"
REVISION_PROBLEMS = (STALE, OVER_RETRACTED, FALSE_ALARM)

FLAG = "flag"
CONTROL = "control"

# Corpus families the gate requires, each with at least MIN_FAMILY_ROWS rows
# (the corpus generator holds every sub-variant at n >= 30 for the same
# reason: rates on smaller samples carry no usable confidence interval).
REQUIRED_CORPUS_FAMILIES = (
    "functional_date", "functional_city", "functional_price", "polarity",
    "cycle", "relation", "interval", "range", "cardinality",
    "instruction_clash", "spatial", "causal", "disjoint_class",
    "domain_violation", "identity", "correction_control",
)
MIN_FAMILY_ROWS = 30

METRICS = ("detection", "false_alarm_rate", "revision_fidelity")


@dataclass(frozen=True)
class Expectation:
    hard: frozenset = frozenset()
    soft: frozenset = frozenset()
    violations: frozenset = frozenset()
    must_absent: frozenset = frozenset()
    must_present: frozenset = frozenset()


@dataclass(frozen=True)
class Observation:
    hard: frozenset = frozenset()
    soft: frozenset = frozenset()
    violations: frozenset = frozenset()
    # ids (of must_absent | must_present) found in the graph
    present: frozenset = frozenset()


@dataclass(frozen=True)
class Checkpoint:
    expect: Expectation
    observed: Observation


@dataclass(frozen=True)
class Row:
    source: str          # "corpus" | "benchmark"
    row_id: str
    family: str
    role: str            # FLAG | CONTROL
    revision: bool       # a correction/retraction row
    checkpoints: tuple
    # the row inserted at least one fact; a control without facts is vacuous
    has_data: bool = True


@dataclass
class Verdict:
    row: Row
    problems: list = field(default_factory=list)  # [(category, detail)]

    @property
    def ok(self):
        return not self.problems

    def has(self, *categories):
        return any(c in categories for c, _ in self.problems)


def judge_checkpoint(role, cp):
    """Problems for one checkpoint, as (category, detail) pairs."""
    exp, obs = cp.expect, cp.observed
    problems = []
    missed = (exp.hard - obs.hard) | (exp.violations - obs.violations)
    if missed:
        problems.append((MISSED, f"expected {sorted(missed)} did not fire"))
    extra = (obs.hard - exp.hard) | (obs.violations - exp.violations)
    if extra:
        cat = FALSE_ALARM if role == CONTROL else DRIFT
        problems.append((cat, f"unexpected {sorted(extra)}"))
    if obs.soft != exp.soft:
        problems.append((DRIFT, f"soft tensions {sorted(obs.soft)}, "
                                f"expected {sorted(exp.soft)}"))
    stale = exp.must_absent & obs.present
    if stale:
        problems.append((STALE, f"retracted {sorted(stale)} still present"))
    gone = exp.must_present - obs.present
    if gone:
        problems.append((OVER_RETRACTED, f"{sorted(gone)} missing"))
    return problems


def judge(row):
    v = Verdict(row)
    for i, cp in enumerate(row.checkpoints):
        tag = f"checkpoint {i + 1}: " if len(row.checkpoints) > 1 else ""
        v.problems += [(c, tag + d) for c, d in judge_checkpoint(row.role, cp)]
    return v


def _rate(hits, n):
    return {"hits": hits, "n": n}


def metrics(verdicts):
    """The three gate metrics from judged rows."""
    detection = {}
    for v in verdicts:
        if v.row.role == FLAG:
            fam = f"{v.row.source}:{v.row.family}"
            d = detection.setdefault(fam, _rate(0, 0))
            d["n"] += 1
            d["hits"] += not v.has(MISSED)
    controls = [v for v in verdicts if v.row.role == CONTROL]
    revisions = [v for v in verdicts if v.row.revision]
    return {
        "detection": dict(sorted(detection.items())),
        "false_alarm_rate": _rate(
            sum(v.has(FALSE_ALARM) for v in controls), len(controls)),
        "revision_fidelity": _rate(
            sum(not v.has(*REVISION_PROBLEMS) for v in revisions),
            len(revisions)),
    }


def contract_failures(rows, metric_values):
    """Everything that makes the run unable to vouch for the corpus."""
    failures = []
    families = {}
    for r in rows:
        if r.source == "corpus":
            families[r.family] = families.get(r.family, 0) + 1
        if not r.checkpoints:
            failures.append(f"{r.row_id}: no checkpoint evaluated")
        if not r.has_data:
            failures.append(f"{r.row_id}: vacuous row, no facts inserted")
        if r.role == FLAG and not any(
                cp.expect.hard or cp.expect.violations for cp in r.checkpoints):
            failures.append(f"{r.row_id}: flagged row expects no finding")
    for fam in REQUIRED_CORPUS_FAMILIES:
        n = families.get(fam, 0)
        if n < MIN_FAMILY_ROWS:
            failures.append(f"corpus family {fam}: {n} rows, "
                            f"need >= {MIN_FAMILY_ROWS}")
    for name in METRICS:
        if name not in metric_values:
            failures.append(f"metric {name} missing")
    det = metric_values.get("detection", {})
    for fam in REQUIRED_CORPUS_FAMILIES:
        if fam != "correction_control" and not det.get(f"corpus:{fam}",
                                                       {}).get("n"):
            failures.append(f"detection metric missing for corpus:{fam}")
    for source in sorted({r.source for r in rows}):
        if not any(r.source == source and r.role == CONTROL for r in rows):
            failures.append(f"{source}: no control rows")
        if not any(r.source == source and r.revision for r in rows):
            failures.append(f"{source}: no revision rows")
    for name in ("false_alarm_rate", "revision_fidelity"):
        if not metric_values.get(name, {}).get("n"):
            failures.append(f"metric {name} has an empty denominator")
    return failures


def missing_rows(expected_ids, observed_ids, source):
    """Contract failures for rows the replay never reported back."""
    return [f"{source} row {rid} was not evaluated"
            for rid in sorted(set(expected_ids) - set(observed_ids))]


@dataclass
class GateReport:
    verdicts: list
    metrics: dict
    contract: list
    gaps: list

    @property
    def passed(self):
        return not self.contract and all(v.ok for v in self.verdicts)


def evaluate(rows, gaps=(), extra_contract=()):
    verdicts = [judge(r) for r in rows]
    m = metrics(verdicts)
    contract = list(extra_contract) + contract_failures(rows, m)
    return GateReport(verdicts, m, contract, list(gaps))


def _pct(r):
    return f"{r['hits']}/{r['n']}" + (
        f" ({100 * r['hits'] / r['n']:.1f}%)" if r["n"] else " (n/a)")


def render(report):
    """Human-readable report: the three metrics, then every problem."""
    m = report.metrics
    lines = ["DETECTION per family (flagged rows; exact kinds)"]
    for fam, r in m["detection"].items():
        lines.append(f"  {fam:32s} {_pct(r)}")
    fa = m["false_alarm_rate"]
    lines.append(f"FALSE-ALARM RATE over control rows: "
                 f"{fa['hits']}/{fa['n']} flagged")
    lines.append(f"REVISION FIDELITY over correction rows: "
                 f"{_pct(m['revision_fidelity'])}")
    lines.append(f"DECLARED GAPS (documented, not scored): "
                 f"{len(report.gaps)} ({', '.join(report.gaps) or 'none'})")
    bad = [v for v in report.verdicts if not v.ok]
    for v in bad:
        for cat, detail in v.problems:
            lines.append(f"  FAIL {v.row.source}:{v.row.row_id} "
                         f"[{cat}] {detail}")
    for c in report.contract:
        lines.append(f"  CONTRACT {c}")
    lines.append("GATE " + ("PASSED" if report.passed else
                            f"FAILED ({len(bad)} rows, "
                            f"{len(report.contract)} contract failures)"))
    return "\n".join(lines)
