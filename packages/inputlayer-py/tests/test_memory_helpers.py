"""Direct unit tests for _memory_helpers: extract_topics, node factories."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from inputlayer.integrations.langgraph._memory_helpers import (
    extract_topics,
    make_recall_node,
    make_store_node,
)


class TestExtractTopics:
    def test_python_detected(self) -> None:
        assert "python" in extract_topics("I'm learning Python")

    def test_ml_detected(self) -> None:
        topics = extract_topics("Training a machine learning model")
        assert "ml" in topics

    def test_devops_detected(self) -> None:
        topics = extract_topics("Deploy with Docker and Kubernetes")
        assert "devops" in topics

    def test_multiple_topics(self) -> None:
        topics = extract_topics("Python machine learning with Docker")
        assert "python" in topics
        assert "ml" in topics
        assert "devops" in topics

    def test_no_topics_for_generic(self) -> None:
        assert extract_topics("I went to the store") == []

    def test_empty_string(self) -> None:
        assert extract_topics("") == []

    def test_case_insensitive(self) -> None:
        assert "python" in extract_topics("PYTHON is great")

    def test_rust_detected(self) -> None:
        assert "rust" in extract_topics("Rust borrow checker is strict")

    def test_javascript_detected(self) -> None:
        topics = extract_topics("Building a React component in TypeScript")
        assert "javascript" in topics

    def test_deep_learning_detected(self) -> None:
        topics = extract_topics("Training a transformer model")
        assert "deep_learning" in topics

    def test_security_detected(self) -> None:
        assert "security" in extract_topics("Fix the auth vulnerability")

    def test_performance_detected(self) -> None:
        assert "performance" in extract_topics("Optimize latency")

    def test_api_detected(self) -> None:
        assert "api" in extract_topics("Build a REST endpoint")

    def test_data_detected(self) -> None:
        assert "data" in extract_topics("SQL database ETL pipeline")

    def test_returns_list(self) -> None:
        result = extract_topics("test")
        assert isinstance(result, list)


class TestMakeStoreNode:
    async def test_basic_store(self) -> None:
        memory = AsyncMock()
        memory.astore = AsyncMock()
        node = make_store_node(memory, state_key="msg", thread_key="tid", strict=True)

        result = await node({"tid": "t1", "msg": {"role": "user", "content": "hi"}})

        memory.astore.assert_awaited_once_with("t1", "user", "hi", topics=None)
        assert result == {}

    async def test_none_message_skipped(self) -> None:
        memory = AsyncMock()
        node = make_store_node(memory, state_key="msg", thread_key="tid", strict=True)

        result = await node({"tid": "t1"})

        memory.astore.assert_not_awaited()
        assert result == {}

    async def test_strict_missing_thread_raises(self) -> None:
        memory = AsyncMock()
        node = make_store_node(memory, state_key="msg", thread_key="tid", strict=True)

        with pytest.raises(ValueError, match="tid"):
            await node({"msg": {"role": "user", "content": "hi"}})

    async def test_non_strict_missing_thread_uses_default(self) -> None:
        memory = AsyncMock()
        memory.astore = AsyncMock()
        node = make_store_node(memory, state_key="msg", thread_key="tid", strict=False)

        await node({"msg": {"role": "user", "content": "hi"}})

        memory.astore.assert_awaited_once()
        assert memory.astore.call_args[0][0] == "default"

    async def test_strict_non_dict_raises(self) -> None:
        memory = AsyncMock()
        node = make_store_node(memory, state_key="msg", thread_key="tid", strict=True)

        with pytest.raises(TypeError, match="dict"):
            await node({"tid": "t1", "msg": "not a dict"})

    async def test_non_strict_non_dict_skipped(self) -> None:
        memory = AsyncMock()
        node = make_store_node(memory, state_key="msg", thread_key="tid", strict=False)

        result = await node({"tid": "t1", "msg": "not a dict"})

        memory.astore.assert_not_awaited()
        assert result == {}

    async def test_forwards_topics(self) -> None:
        memory = AsyncMock()
        memory.astore = AsyncMock()
        node = make_store_node(memory, state_key="msg", thread_key="tid", strict=True)

        await node(
            {
                "tid": "t1",
                "msg": {"role": "user", "content": "hi", "topics": ["ml"]},
            }
        )

        memory.astore.assert_awaited_once_with("t1", "user", "hi", topics=["ml"])

    def test_node_name(self) -> None:
        memory = AsyncMock()
        node = make_store_node(memory, state_key="msg", thread_key="tid", strict=True)
        assert node.__name__ == "memory_store"

    async def test_empty_thread_id_in_strict_raises(self) -> None:
        memory = AsyncMock()
        node = make_store_node(memory, state_key="msg", thread_key="tid", strict=True)

        with pytest.raises(ValueError, match="tid"):
            await node({"tid": "", "msg": {"role": "user", "content": "hi"}})


class TestMakeRecallNode:
    async def test_basic_recall(self) -> None:
        memory = AsyncMock()
        memory.arecall = AsyncMock(return_value={"topics": ["ml"], "recent": []})
        node = make_recall_node(memory, state_key="ctx", thread_key="tid", strict=True)

        result = await node({"tid": "t1"})

        memory.arecall.assert_awaited_once_with("t1")
        assert result == {"ctx": {"topics": ["ml"], "recent": []}}

    async def test_strict_missing_thread_raises(self) -> None:
        memory = AsyncMock()
        node = make_recall_node(memory, state_key="ctx", thread_key="tid", strict=True)

        with pytest.raises(ValueError, match="tid"):
            await node({})

    async def test_non_strict_uses_default(self) -> None:
        memory = AsyncMock()
        memory.arecall = AsyncMock(return_value={"topics": []})
        node = make_recall_node(memory, state_key="ctx", thread_key="tid", strict=False)

        await node({})

        memory.arecall.assert_awaited_once_with("default")

    def test_node_name(self) -> None:
        memory = AsyncMock()
        node = make_recall_node(memory, state_key="ctx", thread_key="tid", strict=True)
        assert node.__name__ == "memory_recall"
