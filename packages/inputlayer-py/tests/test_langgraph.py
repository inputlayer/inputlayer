"""Tests for inputlayer.integrations.langgraph, nodes, router, state."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from inputlayer.integrations.langgraph import InputLayerState, escape_iql, kg_node, kg_router
from inputlayer.result import ResultSet

# ── Helpers ──────────────────────────────────────────────────────────


def _mock_kg(
    columns: list[str] | None = None,
    rows: list[list] | None = None,
) -> MagicMock:
    columns = columns or []
    rows = rows or []
    result = ResultSet(columns=columns, rows=rows)
    kg = MagicMock()
    kg.execute = AsyncMock(return_value=result)
    kg.insert = AsyncMock()
    kg.delete = AsyncMock()
    return kg


# ═══════════════════════════════════════════════════════════════════════
#  kg_node - query mode
# ═══════════════════════════════════════════════════════════════════════


class TestKgNodeQuery:
    async def test_basic_query(self) -> None:
        kg = _mock_kg(columns=["name", "dept"], rows=[["alice", "eng"]])
        node = kg_node(query="?emp(Name, Dept)")

        result = await node({"kg": kg})

        kg.execute.assert_awaited_once_with("?emp(Name, Dept)")
        assert result["results"]["rows"] == [["alice", "eng"]]
        assert result["results"]["columns"] == ["name", "dept"]

    async def test_parameterized_query(self) -> None:
        kg = _mock_kg(columns=["title"], rows=[["ML Intro"]])
        node = kg_node(
            query=lambda s: f'?article(Id, Title, "{s["category"]}", Emb)',
            state_key="articles",
        )

        result = await node({"kg": kg, "category": "ml"})

        kg.execute.assert_awaited_once_with('?article(Id, Title, "ml", Emb)')
        assert result["articles"]["rows"] == [["ML Intro"]]

    async def test_empty_results(self) -> None:
        kg = _mock_kg(columns=["x"], rows=[])
        node = kg_node(query="?empty(X)")

        result = await node({"kg": kg})

        assert result["results"]["rows"] == []
        assert result["results"]["row_count"] == 0

    async def test_custom_state_key(self) -> None:
        kg = _mock_kg(columns=["val"], rows=[["data"]])
        node = kg_node(query="?test(X)", state_key="my_data")

        result = await node({"kg": kg})

        assert "my_data" in result
        assert result["my_data"]["rows"] == [["data"]]

    async def test_custom_kg_key(self) -> None:
        kg = _mock_kg(columns=["x"], rows=[["ok"]])
        node = kg_node(query="?test(X)", kg_key="my_kg")

        result = await node({"my_kg": kg})

        kg.execute.assert_awaited_once()
        assert result["results"]["rows"] == [["ok"]]

    def test_query_required_for_query_mode(self) -> None:
        with pytest.raises(ValueError, match="Must provide 'query'"):
            kg_node(operation="query")


# ═══════════════════════════════════════════════════════════════════════
#  kg_node - insert mode
# ═══════════════════════════════════════════════════════════════════════


class TestKgNodeInsert:
    async def test_insert_list_of_dicts(self) -> None:
        from inputlayer.relation import Relation

        class Emp(Relation):
            name: str
            dept: str

        kg = _mock_kg()
        node = kg_node(relation=Emp, operation="insert", state_key="new_emps")

        data = [{"name": "alice", "dept": "eng"}, {"name": "bob", "dept": "hr"}]
        result = await node({"kg": kg, "new_emps": data})

        kg.insert.assert_awaited_once_with(Emp, data)
        assert result == {}

    async def test_insert_single_dict(self) -> None:
        from inputlayer.relation import Relation

        class Emp(Relation):
            name: str

        kg = _mock_kg()
        node = kg_node(relation=Emp, operation="insert", state_key="new_emp")

        result = await node({"kg": kg, "new_emp": {"name": "alice"}})

        kg.insert.assert_awaited_once_with(Emp, {"name": "alice"})
        assert result == {}

    async def test_insert_none_is_noop(self) -> None:
        from inputlayer.relation import Relation

        class Emp(Relation):
            name: str

        kg = _mock_kg()
        node = kg_node(relation=Emp, operation="insert", state_key="data")

        result = await node({"kg": kg})

        kg.insert.assert_not_awaited()
        assert result == {}

    async def test_insert_empty_list_is_noop(self) -> None:
        """Empty list must not trigger a KG insert - same as None."""
        from inputlayer.relation import Relation

        class Emp(Relation):
            name: str

        kg = _mock_kg()
        node = kg_node(relation=Emp, operation="insert", state_key="data")

        result = await node({"kg": kg, "data": []})

        kg.insert.assert_not_awaited()
        assert result == {}

    def test_relation_required_for_insert(self) -> None:
        with pytest.raises(ValueError, match="Must provide 'relation'"):
            kg_node(operation="insert", state_key="data")

    async def test_insert_list_of_relation_instances(self) -> None:
        from inputlayer.relation import Relation

        class Emp(Relation):
            name: str

        kg = _mock_kg()
        emp1 = Emp(name="alice")
        emp2 = Emp(name="bob")
        node = kg_node(relation=Emp, operation="insert", state_key="emps")

        result = await node({"kg": kg, "emps": [emp1, emp2]})

        # List of Relation instances: kg.insert(list) called directly
        kg.insert.assert_awaited_once_with([emp1, emp2])
        assert result == {}

    async def test_insert_single_relation_instance(self) -> None:
        from inputlayer.relation import Relation

        class Emp(Relation):
            name: str

        kg = _mock_kg()
        emp = Emp(name="alice")
        node = kg_node(relation=Emp, operation="insert", state_key="new_emp")

        result = await node({"kg": kg, "new_emp": emp})

        # Single Relation instance: wrapped in list for kg.insert
        kg.insert.assert_awaited_once_with([emp])
        assert result == {}


# ═══════════════════════════════════════════════════════════════════════
#  kg_node - delete mode
# ═══════════════════════════════════════════════════════════════════════


class TestKgNodeDelete:
    async def test_delete_list(self) -> None:
        from inputlayer.relation import Relation

        class Emp(Relation):
            name: str

        kg = _mock_kg()
        emp1 = Emp(name="alice")
        emp2 = Emp(name="bob")
        node = kg_node(relation=Emp, operation="delete", state_key="to_delete")

        await node({"kg": kg, "to_delete": [emp1, emp2]})

        assert kg.delete.await_count == 2

    async def test_delete_single_item(self) -> None:
        from inputlayer.relation import Relation

        class Emp(Relation):
            name: str

        kg = _mock_kg()
        emp = Emp(name="alice")
        node = kg_node(relation=Emp, operation="delete", state_key="to_delete")

        result = await node({"kg": kg, "to_delete": emp})

        kg.delete.assert_awaited_once_with(emp)
        assert result == {}

    async def test_delete_none_is_noop(self) -> None:
        from inputlayer.relation import Relation

        class Emp(Relation):
            name: str

        kg = _mock_kg()
        node = kg_node(relation=Emp, operation="delete", state_key="data")

        await node({"kg": kg})

        kg.delete.assert_not_awaited()

    async def test_delete_empty_list_is_noop(self) -> None:
        """Empty list must not trigger any KG deletes - same as None."""
        from inputlayer.relation import Relation

        class Emp(Relation):
            name: str

        kg = _mock_kg()
        node = kg_node(relation=Emp, operation="delete", state_key="data")

        result = await node({"kg": kg, "data": []})

        kg.delete.assert_not_awaited()
        assert result == {}

    def test_relation_required_for_delete(self) -> None:
        with pytest.raises(ValueError, match="Must provide 'relation'"):
            kg_node(operation="delete", state_key="data")


# ═══════════════════════════════════════════════════════════════════════
#  kg_router
# ═══════════════════════════════════════════════════════════════════════


class TestKgRouter:
    async def test_first_matching_branch_wins(self) -> None:
        kg = MagicMock()
        # First query returns empty, second returns results
        kg.execute = AsyncMock(
            side_effect=[
                ResultSet(columns=["x"], rows=[]),
                ResultSet(columns=["x"], rows=[["found"]]),
            ]
        )

        router = kg_router(
            branches={
                "branch_a": "?empty(X)",
                "branch_b": "?has_data(X)",
            },
        )

        result = await router({"kg": kg})

        assert result == "branch_b"
        assert kg.execute.await_count == 2

    async def test_returns_default_when_no_match(self) -> None:
        kg = MagicMock()
        kg.execute = AsyncMock(return_value=ResultSet(columns=["x"], rows=[]))

        router = kg_router(
            branches={"a": "?no_match(X)"},
            default="fallback",
        )

        result = await router({"kg": kg})

        assert result == "fallback"

    async def test_default_is_end(self) -> None:
        kg = MagicMock()
        kg.execute = AsyncMock(return_value=ResultSet(columns=["x"], rows=[]))

        router = kg_router(branches={"a": "?no_match(X)"})

        result = await router({"kg": kg})

        assert result == "end"

    async def test_first_branch_matches_immediately(self) -> None:
        kg = MagicMock()
        kg.execute = AsyncMock(return_value=ResultSet(columns=["x"], rows=[["yes"]]))

        router = kg_router(
            branches={
                "first": "?ready(X)",
                "second": "?other(X)",
            },
        )

        result = await router({"kg": kg})

        assert result == "first"
        kg.execute.assert_awaited_once()  # Only first query executed

    async def test_parameterized_branch_query(self) -> None:
        kg = MagicMock()
        kg.execute = AsyncMock(return_value=ResultSet(columns=["x"], rows=[["match"]]))

        router = kg_router(
            branches={
                "found": lambda s: f'?search("{s["query"]}", X)',
            },
        )

        result = await router({"kg": kg, "query": "hello"})

        assert result == "found"
        kg.execute.assert_awaited_once_with('?search("hello", X)')

    async def test_custom_kg_key(self) -> None:
        kg = MagicMock()
        kg.execute = AsyncMock(return_value=ResultSet(columns=["x"], rows=[["yes"]]))

        router = kg_router(
            branches={"match": "?test(X)"},
            kg_key="my_kg",
        )

        result = await router({"my_kg": kg})

        assert result == "match"

    def test_empty_branches_raises(self) -> None:
        with pytest.raises(ValueError, match="at least one branch"):
            kg_router(branches={})


# ═══════════════════════════════════════════════════════════════════════
#  InputLayerState
# ═══════════════════════════════════════════════════════════════════════


class TestInputLayerState:
    def test_state_is_typeddict(self) -> None:
        state: InputLayerState = {"kg": MagicMock(), "results": {}}
        assert "kg" in state
        assert "results" in state

    def test_state_extensible(self) -> None:
        class MyState(InputLayerState):
            question: str
            answer: str

        state: MyState = {
            "kg": MagicMock(),
            "results": {"columns": [], "rows": [], "row_count": 0},
            "question": "hello",
            "answer": "world",
        }
        assert state["question"] == "hello"


# ═══════════════════════════════════════════════════════════════════════
#  Integration: node + router together
# ═══════════════════════════════════════════════════════════════════════


class TestIntegration:
    async def test_node_then_router_flow(self) -> None:
        """Simulate: query node populates state, router checks results."""
        kg = MagicMock()
        # Node query returns data
        kg.execute = AsyncMock(
            side_effect=[
                # First call: the node query
                ResultSet(columns=["topic"], rows=[["ml"], ["db"]]),
                # Second call: the router check
                ResultSet(columns=["x"], rows=[["yes"]]),
            ]
        )

        # Step 1: Node queries the KG
        search_node = kg_node(query="?active_topic(T)", state_key="topics")
        state = {"kg": kg}
        state.update(await search_node(state))

        assert state["topics"]["rows"] == [["ml"], ["db"]]

        # Step 2: Router decides next step
        router = kg_router(
            branches={"process": "?has_results(X)"},
            default="retry",
        )
        next_step = await router(state)

        assert next_step == "process"

    async def test_node_works_from_running_loop(self) -> None:
        """Verify nodes work inside an async context (LangGraph's runtime)."""
        kg = _mock_kg(columns=["v"], rows=[["ok"]])
        node = kg_node(query="?test(X)")

        # Simulating being inside LangGraph's async execution
        result = await node({"kg": kg})

        assert result["results"]["rows"] == [["ok"]]

    async def test_multiple_nodes_sequential(self) -> None:
        """Multiple nodes updating state sequentially."""
        call_count = 0

        async def make_result(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return ResultSet(columns=["name"], rows=[["alice"]])
            return ResultSet(columns=["skill"], rows=[["ml"]])

        kg = MagicMock()
        kg.execute = AsyncMock(side_effect=make_result)

        node1 = kg_node(query="?employee(Name)", state_key="employees")
        node2 = kg_node(query="?skill(Skill)", state_key="skills")

        state: dict = {"kg": kg}
        state.update(await node1(state))
        state.update(await node2(state))

        assert state["employees"]["rows"] == [["alice"]]
        assert state["skills"]["rows"] == [["ml"]]


# ═══════════════════════════════════════════════════════════════════════
#  Error paths
# ═══════════════════════════════════════════════════════════════════════


class TestKgNodeErrors:
    async def test_missing_kg_key_raises_with_helpful_message(self) -> None:
        node = kg_node(query="?test(X)")
        with pytest.raises(KeyError, match="kg"):
            await node({})  # no 'kg' in state

    async def test_missing_custom_kg_key_raises(self) -> None:
        node = kg_node(query="?test(X)", kg_key="my_kg")
        with pytest.raises(KeyError, match="my_kg"):
            await node({"kg": MagicMock()})  # 'my_kg' not in state


class TestKgRouterErrors:
    async def test_missing_kg_key_raises_with_helpful_message(self) -> None:
        router = kg_router(branches={"a": "?test(X)"})
        with pytest.raises(KeyError, match="kg"):
            await router({})

    async def test_failing_branch_is_skipped_continues_to_next(self) -> None:
        """A branch query that raises must be skipped; next branch is tried."""
        kg = MagicMock()
        kg.execute = AsyncMock(
            side_effect=[
                RuntimeError("server error"),
                ResultSet(columns=["x"], rows=[["found"]]),
            ]
        )

        router = kg_router(
            branches={
                "fails": "?broken(X)",
                "works": "?good(X)",
            },
            default="fallback",
        )

        result = await router({"kg": kg})

        assert result == "works"
        assert kg.execute.await_count == 2

    async def test_all_branches_fail_returns_default(self) -> None:
        kg = MagicMock()
        kg.execute = AsyncMock(side_effect=RuntimeError("down"))

        router = kg_router(
            branches={"a": "?x(X)", "b": "?y(X)"},
            default="safe",
        )

        result = await router({"kg": kg})

        assert result == "safe"


# ═══════════════════════════════════════════════════════════════════════
#  escape_iql
# ═══════════════════════════════════════════════════════════════════════


class TestEscapeIql:
    def test_backslash_escaped_first(self) -> None:
        # Backslash must be escaped before quote, or the backslash
        # before a quote would get double-escaped.
        assert escape_iql('\\') == '\\\\'

    def test_double_quote_escaped(self) -> None:
        assert escape_iql('"hello"') == '\\"hello\\"'

    def test_backslash_then_quote(self) -> None:
        # The tricky case: backslash followed by quote
        assert escape_iql('\\"') == '\\\\\\"'

    def test_newline_escaped(self) -> None:
        assert escape_iql("line1\nline2") == "line1\\nline2"

    def test_carriage_return_escaped(self) -> None:
        assert escape_iql("a\rb") == "a\\rb"

    def test_tab_escaped(self) -> None:
        assert escape_iql("a\tb") == "a\\tb"

    def test_nul_byte_escaped(self) -> None:
        assert escape_iql("a\x00b") == "a\\0b"

    def test_plain_string_unchanged(self) -> None:
        assert escape_iql("hello world 123") == "hello world 123"

    def test_unicode_passthrough(self) -> None:
        # Unicode chars that don't need escaping should pass through unchanged
        assert escape_iql("cafe\u0301") == "cafe\u0301"
        assert escape_iql("\U0001f600") == "\U0001f600"

    def test_empty_string(self) -> None:
        assert escape_iql("") == ""

    def test_all_control_chars_in_one_string(self) -> None:
        result = escape_iql('\\"test\n\r\t\x00')
        assert result == '\\\\\\"test\\n\\r\\t\\0'
