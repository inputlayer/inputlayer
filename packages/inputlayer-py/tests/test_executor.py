"""Tests for inputlayer.migrations.executor and recorder - apply/revert with mocked KG."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from inputlayer.migrations.executor import (
    MigrationError,
    apply_migration,
    migrate,
    revert_migration,
    revert_to,
)
from inputlayer.migrations.loader import MigrationInfo
from inputlayer.migrations.operations import (
    CreateRelation,
    CreateRule,
    DropRelation,
    ReplaceRule,
)
from inputlayer.migrations.recorder import MIGRATION_RELATION, MigrationRecorder


# ── Mock KG ──────────────────────────────────────────────────────────


@dataclass
class MockResult:
    rows: list = field(default_factory=list)
    columns: list = field(default_factory=list)


class MockKG:
    """Records all executed commands for verification."""

    def __init__(self, query_results: dict[str, list] | None = None):
        self.commands: list[str] = []
        self._query_results = query_results or {}

    def execute(self, datalog: str) -> MockResult:
        self.commands.append(datalog)
        # Check for query patterns to return mock results
        for pattern, rows in self._query_results.items():
            if pattern in datalog:
                return MockResult(rows=rows)
        return MockResult()


def _make_migration(
    name: str,
    number: int,
    operations: list,
    dependencies: list[str] | None = None,
) -> MigrationInfo:
    return MigrationInfo(
        name=name,
        number=number,
        filename=f"{name}.py",
        dependencies=dependencies or [],
        operations=operations,
        state={},
    )


# ── apply_migration ─────────────────────────────────────────────────


class TestApplyMigration:
    def test_executes_forward_commands(self):
        kg = MockKG()
        m = _make_migration("0001_initial", 1, [
            CreateRelation("t", [("a", "int")]),
        ])
        apply_migration(kg, m)
        assert "+t(a: int)" in kg.commands

    def test_multiple_operations(self):
        kg = MockKG()
        m = _make_migration("0001_initial", 1, [
            CreateRelation("t", [("a", "int")]),
            CreateRule("r", ["+r(X) <- t(X)"]),
        ])
        apply_migration(kg, m)
        assert len(kg.commands) == 2
        assert "+t(a: int)" in kg.commands
        assert "+r(X) <- t(X)" in kg.commands

    def test_replace_rule_commands(self):
        kg = MockKG()
        m = _make_migration("0002_auto", 2, [
            ReplaceRule("r", ["old"], ["new1", "new2"]),
        ])
        apply_migration(kg, m)
        assert ".rule drop r" in kg.commands
        assert "new1" in kg.commands
        assert "new2" in kg.commands


# ── revert_migration ────────────────────────────────────────────────


class TestRevertMigration:
    def test_executes_backward_commands_in_reverse(self):
        kg = MockKG()
        m = _make_migration("0001_initial", 1, [
            CreateRelation("t", [("a", "int")]),
            CreateRule("r", ["+r(X) <- t(X)"]),
        ])
        revert_migration(kg, m)
        # Reversed: rule first, then relation
        assert kg.commands[0] == ".rule drop r"
        assert kg.commands[1] == ".rel drop t"


# ── MigrationRecorder ───────────────────────────────────────────────


class TestMigrationRecorder:
    def test_ensure_schema(self):
        kg = MockKG()
        recorder = MigrationRecorder(kg)
        recorder.ensure_schema()
        assert any(MIGRATION_RELATION in cmd for cmd in kg.commands)

    def test_get_applied_empty(self):
        kg = MockKG()
        recorder = MigrationRecorder(kg)
        assert recorder.get_applied() == []

    def test_get_applied_returns_names(self):
        kg = MockKG(query_results={
            MIGRATION_RELATION: [["0001_initial", "2024-01-01"], ["0002_auto", "2024-01-02"]],
        })
        recorder = MigrationRecorder(kg)
        applied = recorder.get_applied()
        assert applied == ["0001_initial", "0002_auto"]

    def test_record_applied(self):
        kg = MockKG()
        recorder = MigrationRecorder(kg)
        recorder.record_applied("0001_initial")
        cmd = kg.commands[-1]
        assert MIGRATION_RELATION in cmd
        assert "0001_initial" in cmd

    def test_record_reverted(self):
        kg = MockKG()
        recorder = MigrationRecorder(kg)
        recorder.record_reverted("0001_initial")
        cmd = kg.commands[-1]
        assert "-" in cmd
        assert "0001_initial" in cmd


# ── migrate (full workflow) ──────────────────────────────────────────


class TestMigrate:
    def test_applies_all_unapplied(self):
        kg = MockKG()
        recorder = MigrationRecorder(kg)
        m1 = _make_migration("0001_initial", 1, [CreateRelation("t", [("a", "int")])])
        m2 = _make_migration("0002_auto", 2, [CreateRule("r", ["+r(X) <- t(X)"])])
        applied = migrate(kg, [m1, m2], recorder)
        assert applied == ["0001_initial", "0002_auto"]

    def test_skips_already_applied(self):
        kg = MockKG(query_results={
            MIGRATION_RELATION: [["0001_initial", "2024-01-01"]],
        })
        recorder = MigrationRecorder(kg)
        m1 = _make_migration("0001_initial", 1, [CreateRelation("t", [("a", "int")])])
        m2 = _make_migration("0002_auto", 2, [CreateRule("r", ["+r(X) <- t(X)"])])
        applied = migrate(kg, [m1, m2], recorder)
        assert applied == ["0002_auto"]
        # Should not have recreated t
        assert "+t(a: int)" not in kg.commands

    def test_empty_migrations_list(self):
        kg = MockKG()
        recorder = MigrationRecorder(kg)
        assert migrate(kg, [], recorder) == []


# ── revert_to ────────────────────────────────────────────────────────


class TestRevertTo:
    def test_reverts_after_target(self):
        kg = MockKG(query_results={
            MIGRATION_RELATION: [
                ["0001_initial", "2024-01-01"],
                ["0002_auto", "2024-01-02"],
                ["0003_auto", "2024-01-03"],
            ],
        })
        recorder = MigrationRecorder(kg)
        m1 = _make_migration("0001_initial", 1, [CreateRelation("t", [("a", "int")])])
        m2 = _make_migration("0002_auto", 2, [CreateRule("r", ["+r(X) <- t(X)"])])
        m3 = _make_migration("0003_auto", 3, [CreateRelation("s", [("b", "string")])])
        reverted = revert_to(kg, [m1, m2, m3], recorder, "0001_initial")
        assert "0003_auto" in reverted
        assert "0002_auto" in reverted
        assert "0001_initial" not in reverted

    def test_revert_nothing_if_at_target(self):
        kg = MockKG(query_results={
            MIGRATION_RELATION: [["0001_initial", "2024-01-01"]],
        })
        recorder = MigrationRecorder(kg)
        m1 = _make_migration("0001_initial", 1, [CreateRelation("t", [("a", "int")])])
        reverted = revert_to(kg, [m1], recorder, "0001_initial")
        assert reverted == []

    def test_unknown_target_raises(self):
        kg = MockKG()
        recorder = MigrationRecorder(kg)
        with pytest.raises(MigrationError, match="not found"):
            revert_to(kg, [], recorder, "0099_nope")

    def test_reverts_in_reverse_order(self):
        kg = MockKG(query_results={
            MIGRATION_RELATION: [
                ["0001_initial", "2024-01-01"],
                ["0002_auto", "2024-01-02"],
            ],
        })
        recorder = MigrationRecorder(kg)
        m1 = _make_migration("0001_initial", 1, [CreateRelation("t", [("a", "int")])])
        m2 = _make_migration("0002_auto", 2, [
            CreateRelation("s", [("b", "string")]),
            CreateRule("r", ["+r(X) <- s(X)"]),
        ])
        reverted = revert_to(kg, [m1, m2], recorder, "0001_initial")
        assert reverted == ["0002_auto"]
        # Backward commands: rule drop first, then rel drop
        backward_cmds = [c for c in kg.commands if ".rule drop" in c or ".rel drop s" in c]
        assert len(backward_cmds) == 2
