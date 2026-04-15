"""Tests for kg_router: conditional edge routing based on IQL queries."""

from __future__ import annotations

import builtins
from unittest.mock import AsyncMock, MagicMock

import pytest

from inputlayer.integrations.langgraph import kg_router
from inputlayer.result import ResultSet

builtins_ConnectionError = builtins.__dict__["ConnectionError"]


class TestKgRouter:
    async def test_first_matching_branch_wins(self) -> None:
        kg = MagicMock()
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
        # Verify branch evaluation order: branch_a first, then branch_b
        queries = [call.args[0] for call in kg.execute.call_args_list]
        assert queries[0] == "?empty(X)", "branch_a must be evaluated first"
        assert queries[1] == "?has_data(X)", "branch_b must be evaluated second"

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
        kg.execute.assert_awaited_once()

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


class TestKgRouterErrors:
    async def test_missing_kg_key_raises_with_helpful_message(self) -> None:
        router = kg_router(branches={"a": "?test(X)"})
        with pytest.raises(KeyError, match="kg"):
            await router({})

    async def test_failing_branch_is_skipped_continues_to_next(self) -> None:
        """A branch query that raises QueryError must be skipped; next branch is tried."""
        from inputlayer.exceptions import QueryError

        kg = MagicMock()
        kg.execute = AsyncMock(
            side_effect=[
                QueryError("unknown relation: broken"),
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
        # Verify the failing branch was tried first
        queries = [call.args[0] for call in kg.execute.call_args_list]
        assert queries[0] == "?broken(X)", "failing branch must be tried first"
        assert queries[1] == "?good(X)", "working branch tried after failure"

    async def test_all_branches_fail_returns_default(self) -> None:
        from inputlayer.exceptions import QueryError

        kg = MagicMock()
        kg.execute = AsyncMock(side_effect=QueryError("bad query"))

        router = kg_router(
            branches={"a": "?x(X)", "b": "?y(X)"},
            default="safe",
        )

        result = await router({"kg": kg})

        assert result == "safe"

    async def test_runtime_error_propagates(self) -> None:
        """RuntimeError is not a query error and must propagate."""
        kg = MagicMock()
        kg.execute = AsyncMock(side_effect=RuntimeError("unexpected bug"))

        router = kg_router(
            branches={"a": "?test(X)"},
            default="fallback",
        )

        with pytest.raises(RuntimeError, match="unexpected bug"):
            await router({"kg": kg})

    async def test_value_error_propagates(self) -> None:
        """ValueError is not a query error and must propagate."""
        kg = MagicMock()
        kg.execute = AsyncMock(side_effect=ValueError("bad input"))

        router = kg_router(
            branches={"a": "?test(X)"},
            default="fallback",
        )

        with pytest.raises(ValueError, match="bad input"):
            await router({"kg": kg})


class TestKgRouterConnectionErrors:
    async def test_connection_error_is_not_swallowed(self) -> None:
        """Python builtin ConnectionError must propagate."""
        kg = MagicMock()
        kg.execute = AsyncMock(side_effect=builtins_ConnectionError("server down"))

        router = kg_router(
            branches={"a": "?test(X)"},
            default="fallback",
        )

        with pytest.raises(builtins_ConnectionError, match="server down"):
            await router({"kg": kg})

    async def test_inputlayer_connection_error_is_not_swallowed(self) -> None:
        """InputLayerConnectionError must propagate, not be silently skipped."""
        from inputlayer.exceptions import InputLayerConnectionError

        kg = MagicMock()
        kg.execute = AsyncMock(side_effect=InputLayerConnectionError("websocket closed"))

        router = kg_router(
            branches={"a": "?test(X)"},
            default="fallback",
        )

        with pytest.raises(InputLayerConnectionError, match="websocket closed"):
            await router({"kg": kg})

    async def test_auth_error_is_not_swallowed(self) -> None:
        """AuthenticationError must propagate."""
        from inputlayer.exceptions import AuthenticationError

        kg = MagicMock()
        kg.execute = AsyncMock(side_effect=AuthenticationError("bad token"))

        router = kg_router(
            branches={"a": "?test(X)"},
            default="fallback",
        )

        with pytest.raises(AuthenticationError, match="bad token"):
            await router({"kg": kg})

    async def test_os_error_is_not_swallowed(self) -> None:
        """OSError (network failures) must propagate."""
        kg = MagicMock()
        kg.execute = AsyncMock(side_effect=OSError("network unreachable"))

        router = kg_router(
            branches={"a": "?test(X)"},
            default="fallback",
        )

        with pytest.raises(OSError, match="network unreachable"):
            await router({"kg": kg})

    async def test_query_error_still_skipped(self) -> None:
        """QueryError should be skipped, not propagated."""
        from inputlayer.exceptions import QueryError

        kg = MagicMock()
        kg.execute = AsyncMock(
            side_effect=[
                QueryError("bad query syntax"),
                ResultSet(columns=["x"], rows=[["ok"]]),
            ]
        )

        router = kg_router(
            branches={
                "bad": "?broken(X)",
                "good": "?works(X)",
            },
        )

        result = await router({"kg": kg})
        assert result == "good"


class TestKgRouterEmptyQuery:
    async def test_callable_returning_empty_string_is_skipped(self) -> None:
        """A branch callable that returns '' must be skipped, not sent to KG."""
        kg = MagicMock()
        kg.execute = AsyncMock(return_value=ResultSet(columns=["x"], rows=[["ok"]]))

        router = kg_router(
            branches={
                "empty": lambda s: "",
                "valid": "?test(X)",
            },
            default="fallback",
        )

        result = await router({"kg": kg})

        assert result == "valid"
        # Only the valid branch query should be executed
        kg.execute.assert_awaited_once_with("?test(X)")

    async def test_callable_returning_none_is_skipped(self) -> None:
        """A branch callable that returns None must be skipped."""
        kg = MagicMock()
        kg.execute = AsyncMock(return_value=ResultSet(columns=["x"], rows=[["ok"]]))

        router = kg_router(
            branches={
                "none_branch": lambda s: None,
                "valid": "?test(X)",
            },
            default="fallback",
        )

        result = await router({"kg": kg})

        assert result == "valid"
        kg.execute.assert_awaited_once_with("?test(X)")

    async def test_all_empty_returns_default(self) -> None:
        """When all callables return empty/None, must return default."""
        kg = MagicMock()

        router = kg_router(
            branches={
                "a": lambda s: "",
                "b": lambda s: None,
            },
            default="fallback",
        )

        result = await router({"kg": kg})

        assert result == "fallback"
        kg.execute.assert_not_called()
