"""Tests for kg_node: query, insert, delete operations."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from inputlayer.integrations.langgraph import InputLayerState, kg_node
from inputlayer.result import ResultSet


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

        kg.insert.assert_awaited_once()
        call_args = kg.insert.call_args
        assert call_args[0][0] is Emp, "First arg must be the Relation class"
        assert call_args[0][1] == data, "Second arg must be the exact data list"
        assert call_args[0][1][0]["name"] == "alice"
        assert call_args[0][1][1]["dept"] == "hr"
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
        deleted = [call.args[0] for call in kg.delete.call_args_list]
        assert deleted[0].name == "alice"
        assert deleted[1].name == "bob"

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
#  Error paths
# ═══════════════════════════════════════════════════════════════════════


class TestKgNodeErrors:
    async def test_missing_kg_key_raises_with_helpful_message(self) -> None:
        node = kg_node(query="?test(X)")
        with pytest.raises(KeyError, match="kg"):
            await node({})

    async def test_missing_custom_kg_key_raises(self) -> None:
        node = kg_node(query="?test(X)", kg_key="my_kg")
        with pytest.raises(KeyError, match="my_kg"):
            await node({"kg": MagicMock()})


class TestKgNodeInsertTypeValidation:
    async def test_insert_unsupported_type_raises(self) -> None:
        """Passing a string/int/etc should raise TypeError, not silently fail."""
        from inputlayer.relation import Relation

        class Emp(Relation):
            name: str

        kg = _mock_kg()
        node = kg_node(relation=Emp, operation="insert", state_key="data")

        with pytest.raises(TypeError, match="must be a dict"):
            await node({"kg": kg, "data": "not a dict or relation"})


# ═══════════════════════════════════════════════════════════════════════
#  Integration: node + router together
# ═══════════════════════════════════════════════════════════════════════


class TestIntegration:
    async def test_node_then_router_flow(self) -> None:
        """Simulate: query node populates state, router checks results."""
        from inputlayer.integrations.langgraph import kg_router

        kg = MagicMock()
        kg.execute = AsyncMock(
            side_effect=[
                ResultSet(columns=["topic"], rows=[["ml"], ["db"]]),
                ResultSet(columns=["x"], rows=[["yes"]]),
            ]
        )

        search_node = kg_node(query="?active_topic(T)", state_key="topics")
        state = {"kg": kg}
        state.update(await search_node(state))

        assert state["topics"]["rows"] == [["ml"], ["db"]]

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
