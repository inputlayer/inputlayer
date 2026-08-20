"""Safety tests for migration loading and execution.

Covers the failure modes an adversarial review found unguarded: duplicate
migrations double-applying, malformed JSON producing raw tracebacks, the
format marker being write-only, engine error frames being invisible, and
unvalidated names reaching IQL and the filesystem.
"""

from __future__ import annotations

import json
import textwrap

import pytest

from inputlayer.migrations.errors import MigrationError, check_engine_result
from inputlayer.migrations.executor import apply_migration, migrate
from inputlayer.migrations.loader import MigrationInfo, load_migrations
from inputlayer.migrations.operations import CreateRelation
from inputlayer.migrations.recorder import MigrationRecorder
from inputlayer.migrations.writer import generate_migration

STATE = {"relations": {}, "rules": {}, "indexes": {}}

LEGACY_PY = textwrap.dedent("""\
    from inputlayer.migrations import Migration
    from inputlayer.migrations import operations as ops

    class M(Migration):
        dependencies = []
        operations = [ops.CreateRelation(name="t", columns=[("a", "int")])]
        state = {"relations": {"t": [("a", "int")]}, "rules": {}, "indexes": {}}
""")


def _write_json(directory, filename, **overrides):
    document = {
        "format": 1,
        "dependencies": [],
        "operations": [{"type": "CreateRelation", "name": "t", "columns": [["a", "int"]]}],
        "state": STATE,
    }
    document.update(overrides)
    (directory / filename).write_text(json.dumps(document))


class TestDuplicateDetection:
    def test_same_name_across_formats_is_rejected(self, tmp_path):
        # The exact double-apply trap: a stale legacy .py twin of a
        # regenerated .json.
        (tmp_path / "0001_initial.py").write_text(LEGACY_PY)
        _write_json(tmp_path, "0001_initial.json")
        with pytest.raises(MigrationError, match="duplicate migration name"):
            load_migrations(tmp_path)

    def test_same_number_different_names_is_rejected(self, tmp_path):
        _write_json(tmp_path, "0002_a.json")
        _write_json(tmp_path, "0002_b.json")
        with pytest.raises(MigrationError, match="duplicate migration number"):
            load_migrations(tmp_path)

    def test_mixed_formats_with_distinct_numbers_load(self, tmp_path):
        # The legitimate mixed directory: legacy history + new JSON.
        (tmp_path / "0001_initial.py").write_text(LEGACY_PY)
        _write_json(tmp_path, "0002_auto.json")
        loaded = load_migrations(tmp_path)
        assert [m.name for m in loaded] == ["0001_initial", "0002_auto"]


class TestJsonValidation:
    def test_malformed_json_names_the_file(self, tmp_path):
        (tmp_path / "0001_broken.json").write_text('{"format": 1, "oops"')
        with pytest.raises(MigrationError, match="0001_broken.json"):
            load_migrations(tmp_path)

    def test_unknown_operation_type_names_the_file(self, tmp_path):
        _write_json(tmp_path, "0001_x.json", operations=[{"type": "Nope"}])
        with pytest.raises(MigrationError, match="0001_x.json"):
            load_migrations(tmp_path)

    def test_operation_missing_fields_names_the_file(self, tmp_path):
        _write_json(tmp_path, "0001_x.json", operations=[{"type": "CreateRelation"}])
        with pytest.raises(MigrationError, match="0001_x.json"):
            load_migrations(tmp_path)

    def test_future_format_is_rejected(self, tmp_path):
        _write_json(tmp_path, "0001_x.json", format=999)
        with pytest.raises(MigrationError, match="upgrade the SDK"):
            load_migrations(tmp_path)


class _ErrorResult:
    columns = ["error"]
    rows = [["Parse error: nope"]]


class _OkResult:
    columns = ["message"]
    rows = [["ok"]]


class TestEngineErrorVisibility:
    def test_check_engine_result_raises_on_error_frame(self):
        with pytest.raises(MigrationError, match="Parse error: nope"):
            check_engine_result(_ErrorResult(), "testing")
        assert check_engine_result(_OkResult(), "testing") is not None

    def test_failed_operation_is_not_recorded_as_applied(self):
        class FailingKG:
            def __init__(self):
                self.commands = []

            def execute(self, iql):
                self.commands.append(iql)
                if iql.startswith("+t("):
                    return _ErrorResult()
                return _OkResult()

        kg = FailingKG()
        m = MigrationInfo(
            name="0001_initial", number=1, filename="0001_initial.json",
            dependencies=[], operations=[CreateRelation("t", [("a", "int")])],
            state=STATE,
        )
        recorder = MigrationRecorder(kg)
        with pytest.raises(MigrationError, match="applying 0001_initial"):
            migrate(kg, [m], recorder)
        assert not any("0001_initial" in c and c.startswith("+inputlayer_migrations")
                       for c in kg.commands), "failed migration must not be recorded"

    def test_get_applied_raises_on_error_frame(self):
        class ErrorKG:
            def execute(self, iql):
                return _ErrorResult()

        with pytest.raises(MigrationError):
            MigrationRecorder(ErrorKG()).get_applied()


class TestNameSafety:
    def test_writer_rejects_hostile_suffix(self):
        with pytest.raises(MigrationError, match="invalid migration name"):
            generate_migration(3, [], STATE, [], name_suffix="a/b")
        with pytest.raises(MigrationError, match="invalid migration name"):
            generate_migration(3, [], STATE, [], name_suffix='x") +evil[("y')

    def test_recorder_escapes_quotes_in_names(self):
        class CapturingKG:
            def __init__(self):
                self.commands = []

            def execute(self, iql):
                self.commands.append(iql)
                return _OkResult()

        kg = CapturingKG()
        MigrationRecorder(kg).record_applied('0001_a"), +evil[("x')
        assert '\\"' in kg.commands[-1]
        assert '+evil[("x' not in kg.commands[-1].replace('\\"', "")


def test_apply_migration_checks_every_command():
    class OkKG:
        def __init__(self):
            self.commands = []

        def execute(self, iql):
            self.commands.append(iql)
            return _OkResult()

    kg = OkKG()
    m = MigrationInfo(
        name="0001_initial", number=1, filename="0001_initial.json",
        dependencies=[], operations=[CreateRelation("t", [("a", "int")])],
        state=STATE,
    )
    apply_migration(kg, m)
    assert kg.commands == ["+t(a: int)"]
