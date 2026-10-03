"""Self-tests of the gate contract (evaluator.py, engine_replay statements).

The gate is only worth something if it can fail. Each test plants one of
the defects the gate exists to catch and asserts the run fails for that
reason: a false finding on a control, a stale correction, finding-kind
drift, a missing scenario, a missing metric, and a vacuous control.

Run: python3 -m unittest test_evaluator   (no server needed)
"""

import unittest

import engine_replay
from evaluator import (CONTROL, DRIFT, FALSE_ALARM, FLAG, MIN_FAMILY_ROWS,
                       MISSED, OVER_RETRACTED, REQUIRED_CORPUS_FAMILIES,
                       STALE, Checkpoint, Expectation, Observation, Row,
                       evaluate, judge, missing_rows)

F = frozenset


def row(role=FLAG, family="functional_date", expect=None, observed=None,
        revision=False, row_id="r", source="corpus", has_data=True):
    expect = expect or (Expectation(hard=F({"functional"})) if role == FLAG
                        else Expectation())
    observed = observed or Observation(hard=expect.hard,
                                       soft=expect.soft,
                                       violations=expect.violations,
                                       present=expect.must_present)
    return Row(source, row_id, family, role, revision,
               (Checkpoint(expect, observed),), has_data)


def clean_corpus():
    """A minimal corpus that satisfies the whole contract."""
    rows = []
    for fam in REQUIRED_CORPUS_FAMILIES:
        for i in range(MIN_FAMILY_ROWS):
            rid = f"{fam}_{i}"
            if fam == "correction_control":
                exp = Expectation(must_absent=F({f"{rid}__c1"}),
                                  must_present=F({f"{rid}__c2"}))
                rows.append(row(CONTROL, fam, exp, revision=True,
                                row_id=rid))
            else:
                rows.append(row(FLAG, fam, row_id=rid))
    return rows


def replace(rows, rid, new):
    return [new if r.row_id == rid else r for r in rows]


class CleanRun(unittest.TestCase):
    def test_clean_corpus_passes_with_all_three_metrics(self):
        report = evaluate(clean_corpus())
        self.assertTrue(report.passed, report.contract)
        m = report.metrics
        self.assertEqual(m["false_alarm_rate"],
                         {"hits": 0, "n": MIN_FAMILY_ROWS})
        self.assertEqual(m["revision_fidelity"],
                         {"hits": MIN_FAMILY_ROWS, "n": MIN_FAMILY_ROWS})
        self.assertEqual(m["detection"]["corpus:cycle"],
                         {"hits": MIN_FAMILY_ROWS, "n": MIN_FAMILY_ROWS})


class PlantedDefects(unittest.TestCase):
    def test_false_finding_on_control_fails(self):
        rid = "correction_control_3"
        exp = Expectation(must_absent=F({f"{rid}__c1"}),
                          must_present=F({f"{rid}__c2"}))
        planted = row(CONTROL, "correction_control", exp, Observation(
            hard=F({"functional"}), present=F({f"{rid}__c2"})),
            revision=True, row_id=rid)
        report = evaluate(replace(clean_corpus(), rid, planted))
        self.assertFalse(report.passed)
        self.assertEqual(report.metrics["false_alarm_rate"]["hits"], 1)
        # a correction that raises a conflict is also not a faithful revision
        self.assertEqual(report.metrics["revision_fidelity"]["hits"],
                         MIN_FAMILY_ROWS - 1)

    def test_stale_correction_fails(self):
        rid = "correction_control_5"
        exp = Expectation(must_absent=F({f"{rid}__c1"}),
                          must_present=F({f"{rid}__c2"}))
        stale = row(CONTROL, "correction_control", exp, Observation(
            present=F({f"{rid}__c1", f"{rid}__c2"})),
            revision=True, row_id=rid)
        report = evaluate(replace(clean_corpus(), rid, stale))
        self.assertFalse(report.passed)
        self.assertIn(STALE, [c for c, _ in judge(stale).problems])
        self.assertEqual(report.metrics["revision_fidelity"]["hits"],
                         MIN_FAMILY_ROWS - 1)

    def test_over_retraction_fails(self):
        exp = Expectation(must_present=F({"x__c2"}))
        v = judge(row(CONTROL, observed=Observation(), expect=exp))
        self.assertEqual([c for c, _ in v.problems], [OVER_RETRACTED])

    def test_missed_and_extra_kinds_on_flagged_row_fail(self):
        missed = judge(row(observed=Observation()))
        self.assertEqual([c for c, _ in missed.problems], [MISSED])
        extra = judge(row(observed=Observation(hard=F({"functional",
                                                       "cycle"}))))
        self.assertEqual([c for c, _ in extra.problems], [DRIFT])

    def test_soft_tension_drift_fails_but_is_not_a_false_alarm(self):
        v = judge(row(CONTROL, observed=Observation(
            soft=F({"hedge_vs_assert"}))))
        self.assertEqual([c for c, _ in v.problems], [DRIFT])

    def test_expected_violation_on_control_is_not_a_false_alarm(self):
        exp = Expectation(violations=F({"limit_exceeded"}))
        self.assertTrue(judge(row(CONTROL, expect=exp)).ok)
        v = judge(row(CONTROL, expect=exp, observed=Observation(
            violations=F({"limit_exceeded", "persona_break"}))))
        self.assertEqual([c for c, _ in v.problems], [FALSE_ALARM])


class ContractFailures(unittest.TestCase):
    def test_missing_scenario_fails(self):
        rows = clean_corpus()
        failures = missing_rows([r.row_id for r in rows] + ["ghost"],
                                [r.row_id for r in rows], "corpus")
        report = evaluate(rows, extra_contract=failures)
        self.assertFalse(report.passed)
        self.assertIn("corpus row ghost was not evaluated", report.contract)

    def test_family_below_minimum_fails(self):
        rows = [r for r in clean_corpus() if r.row_id != "spatial_0"]
        report = evaluate(rows)
        self.assertFalse(report.passed)
        self.assertTrue(any("corpus family spatial" in c
                            for c in report.contract))

    def test_no_controls_means_no_false_alarm_metric(self):
        rows = [r for r in clean_corpus() if r.role != CONTROL]
        report = evaluate(rows)
        self.assertFalse(report.passed)
        self.assertIn("metric false_alarm_rate has an empty denominator",
                      report.contract)
        self.assertIn("metric revision_fidelity has an empty denominator",
                      report.contract)

    def test_vacuous_control_fails(self):
        rid = "correction_control_0"
        vacuous = row(CONTROL, "correction_control", Expectation(),
                      has_data=False, row_id=rid)
        report = evaluate(replace(clean_corpus(), rid, vacuous))
        self.assertFalse(report.passed)
        self.assertIn(f"{rid}: vacuous row, no facts inserted",
                      report.contract)

    def test_flagged_row_expecting_nothing_fails(self):
        rid = "range_0"
        report = evaluate(replace(clean_corpus(), rid,
                                  row(FLAG, "range", Expectation(),
                                      row_id=rid)))
        self.assertIn(f"{rid}: flagged row expects no finding",
                      report.contract)


class CorpusStatements(unittest.TestCase):
    SC = {"id": "ctrl_9", "facts": [
        {"id": "ctrl_9__c1", "entity": "ctrl_9__trip",
         "attribute": "departure_date", "value": "2026-08-12",
         "modality": "asserted", "num": 20260812},
        {"id": "ctrl_9__c2", "entity": "ctrl_9__trip",
         "attribute": "departure_date", "value": "2026-08-14",
         "modality": "asserted", "num": 20260814}],
        "retractions": ["ctrl_9__c1"], "expect_kinds": [],
        "ontology": [["functional", "venue_city"]]}

    def test_retraction_deletes_every_inserted_row_of_the_fact(self):
        self.assertEqual(engine_replay.scenario_retractions(self.SC), [
            '-claim("ctrl_9__c1", "ctrl_9__trip", "departure_date", '
            '"2026-08-12")',
            '-claim_modality("ctrl_9__c1", "asserted")',
            '-claim_num("ctrl_9__c1", "ctrl_9__trip", "departure_date", '
            '20260812)'])

    def test_retraction_of_unknown_fact_is_a_fixture_error(self):
        bad = dict(self.SC, retractions=["ctrl_9__nope"])
        with self.assertRaises(ValueError):
            engine_replay.scenario_retractions(bad)

    def test_expectation_keeps_unretracted_facts_present(self):
        exp = engine_replay.expectation(self.SC)
        self.assertEqual(exp.must_absent, F({"ctrl_9__c1"}))
        self.assertEqual(exp.must_present, F({"ctrl_9__c2"}))

    def test_inserts_batch_and_deduplicate(self):
        facts = engine_replay._Facts()
        engine_replay.scenario_inserts(self.SC, facts)
        engine_replay.scenario_inserts(dict(self.SC, facts=[]), facts)
        stmts = list(facts.statements())
        self.assertIn('+functional[("venue_city",)]', stmts)
        self.assertEqual(sum(s.startswith("+claim[") for s in stmts), 1)


if __name__ == "__main__":
    unittest.main()
