"""Tests for inputlayer.migrations.operations."""

import pytest

from inputlayer.migrations.operations import (
    CreateIndex,
    CreateRelation,
    CreateRule,
    DropIndex,
    DropRelation,
    DropRule,
    ReplaceRule,
    RunDatalog,
    operation_from_dict,
)


# ── CreateRelation ───────────────────────────────────────────────────


class TestCreateRelation:
    def test_forward_commands(self):
        op = CreateRelation("employee", [("id", "int"), ("name", "string")])
        assert op.forward_commands() == ["+employee(id: int, name: string)"]

    def test_backward_commands(self):
        op = CreateRelation("employee", [("id", "int"), ("name", "string")])
        assert op.backward_commands() == [".rel drop employee"]

    def test_describe(self):
        op = CreateRelation("employee", [("id", "int")])
        assert op.describe() == "Create relation employee"

    def test_roundtrip_serialization(self):
        op = CreateRelation("employee", [("id", "int"), ("name", "string")])
        d = op.to_dict()
        assert d["type"] == "CreateRelation"
        restored = CreateRelation.from_dict(d)
        assert restored == op

    def test_from_dict_via_registry(self):
        op = CreateRelation("x", [("a", "int")])
        assert operation_from_dict(op.to_dict()) == op

    def test_single_column(self):
        op = CreateRelation("counter", [("value", "int")])
        assert op.forward_commands() == ["+counter(value: int)"]

    def test_many_columns(self):
        cols = [("a", "int"), ("b", "string"), ("c", "float"), ("d", "bool")]
        op = CreateRelation("wide", cols)
        cmd = op.forward_commands()[0]
        assert cmd == "+wide(a: int, b: string, c: float, d: bool)"


# ── DropRelation ─────────────────────────────────────────────────────


class TestDropRelation:
    def test_forward_commands(self):
        op = DropRelation("employee", [("id", "int"), ("name", "string")])
        assert op.forward_commands() == [".rel drop employee"]

    def test_backward_commands_recreate(self):
        op = DropRelation("employee", [("id", "int"), ("name", "string")])
        assert op.backward_commands() == ["+employee(id: int, name: string)"]

    def test_describe(self):
        op = DropRelation("x", [])
        assert op.describe() == "Drop relation x"

    def test_roundtrip(self):
        op = DropRelation("t", [("a", "int")])
        assert DropRelation.from_dict(op.to_dict()) == op


# ── CreateRule ───────────────────────────────────────────────────────


class TestCreateRule:
    def test_forward_commands(self):
        clauses = ["+reach(X, Y) <- edge(X, Y)"]
        op = CreateRule("reach", clauses)
        assert op.forward_commands() == clauses

    def test_backward_drops(self):
        op = CreateRule("reach", ["+reach(X, Y) <- edge(X, Y)"])
        assert op.backward_commands() == [".rule drop reach"]

    def test_describe_singular(self):
        op = CreateRule("r", ["clause1"])
        assert "1 clause" in op.describe()

    def test_describe_plural(self):
        op = CreateRule("r", ["c1", "c2"])
        assert "2 clauses" in op.describe()

    def test_roundtrip(self):
        op = CreateRule("x", ["a", "b"])
        assert CreateRule.from_dict(op.to_dict()) == op


# ── DropRule ─────────────────────────────────────────────────────────


class TestDropRule:
    def test_forward_commands(self):
        op = DropRule("reach", ["+reach(X, Y) <- edge(X, Y)"])
        assert op.forward_commands() == [".rule drop reach"]

    def test_backward_recreates(self):
        clauses = ["+reach(X, Y) <- edge(X, Y)", "+reach(X, Z) <- reach(X, Y), edge(Y, Z)"]
        op = DropRule("reach", clauses)
        assert op.backward_commands() == clauses

    def test_roundtrip(self):
        op = DropRule("x", ["c1"])
        assert DropRule.from_dict(op.to_dict()) == op


# ── ReplaceRule ──────────────────────────────────────────────────────


class TestReplaceRule:
    def test_forward_drops_then_adds_new(self):
        op = ReplaceRule("r", ["old1"], ["new1", "new2"])
        cmds = op.forward_commands()
        assert cmds[0] == ".rule drop r"
        assert cmds[1:] == ["new1", "new2"]

    def test_backward_drops_then_adds_old(self):
        op = ReplaceRule("r", ["old1"], ["new1"])
        cmds = op.backward_commands()
        assert cmds[0] == ".rule drop r"
        assert cmds[1:] == ["old1"]

    def test_describe(self):
        op = ReplaceRule("r", [], [])
        assert op.describe() == "Replace rule r"

    def test_roundtrip(self):
        op = ReplaceRule("x", ["a"], ["b", "c"])
        assert ReplaceRule.from_dict(op.to_dict()) == op


# ── CreateIndex ──────────────────────────────────────────────────────


class TestCreateIndex:
    def test_forward_commands(self):
        op = CreateIndex("idx", "doc", "embedding")
        cmd = op.forward_commands()[0]
        assert ".index create idx on doc(embedding)" in cmd
        assert "type hnsw" in cmd
        assert "metric cosine" in cmd

    def test_backward_commands(self):
        op = CreateIndex("idx", "doc", "embedding")
        assert op.backward_commands() == [".index drop idx"]

    def test_custom_params(self):
        op = CreateIndex("idx", "doc", "emb", metric="l2", m=32, ef_construction=200, ef_search=100)
        cmd = op.forward_commands()[0]
        assert "metric l2" in cmd
        assert "m 32" in cmd
        assert "ef_construction 200" in cmd
        assert "ef_search 100" in cmd

    def test_roundtrip(self):
        op = CreateIndex("i", "r", "c", metric="l2", m=32)
        assert CreateIndex.from_dict(op.to_dict()) == op

    def test_describe(self):
        op = CreateIndex("idx", "doc", "emb")
        assert op.describe() == "Create index idx on doc(emb)"


# ── DropIndex ────────────────────────────────────────────────────────


class TestDropIndex:
    def test_forward_commands(self):
        op = DropIndex("idx", "doc", "emb")
        assert op.forward_commands() == [".index drop idx"]

    def test_backward_recreates(self):
        op = DropIndex("idx", "doc", "emb", metric="l2", m=32)
        cmd = op.backward_commands()[0]
        assert ".index create idx on doc(emb)" in cmd
        assert "metric l2" in cmd
        assert "m 32" in cmd

    def test_roundtrip(self):
        op = DropIndex("i", "r", "c")
        assert DropIndex.from_dict(op.to_dict()) == op


# ── RunDatalog ───────────────────────────────────────────────────────


class TestRunDatalog:
    def test_forward_commands(self):
        op = RunDatalog(forward=["+x(1)"], backward=["-x(1)"])
        assert op.forward_commands() == ["+x(1)"]

    def test_backward_commands(self):
        op = RunDatalog(forward=["+x(1)"], backward=["-x(1)"])
        assert op.backward_commands() == ["-x(1)"]

    def test_describe_singular(self):
        op = RunDatalog(forward=["cmd"], backward=[])
        assert "1 custom" in op.describe()

    def test_describe_plural(self):
        op = RunDatalog(forward=["a", "b"], backward=[])
        assert "2 custom" in op.describe()

    def test_roundtrip(self):
        op = RunDatalog(forward=["a"], backward=["b"])
        assert RunDatalog.from_dict(op.to_dict()) == op


# ── Registry ─────────────────────────────────────────────────────────


class TestOperationRegistry:
    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown operation type"):
            operation_from_dict({"type": "Bogus"})

    def test_all_types_roundtrip(self):
        ops = [
            CreateRelation("r", [("a", "int")]),
            DropRelation("r", [("a", "int")]),
            CreateRule("r", ["clause"]),
            DropRule("r", ["clause"]),
            ReplaceRule("r", ["old"], ["new"]),
            CreateIndex("i", "r", "c"),
            DropIndex("i", "r", "c"),
            RunDatalog(forward=["a"], backward=["b"]),
        ]
        for op in ops:
            assert operation_from_dict(op.to_dict()) == op
