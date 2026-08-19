"""Tests for KnowledgeGraph.load: local file read, sent as one atomic program."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from inputlayer.knowledge_graph import KnowledgeGraph


def _kg_with_mocked_execute() -> KnowledgeGraph:
    kg = KnowledgeGraph.__new__(KnowledgeGraph)
    kg._execute = AsyncMock()  # type: ignore[method-assign]
    return kg


class TestLoad:
    async def test_sends_file_contents_as_one_program(self, tmp_path) -> None:
        pack = tmp_path / "pack.iql"
        pack.write_text(
            "// a comment the server strips\n"
            '+claim(id: string, val: string)\n'
            '+claim[("c1", "v1")]\n'
        )

        kg = _kg_with_mocked_execute()
        await kg.load(str(pack))

        kg._execute.assert_awaited_once()
        program = kg._execute.await_args.args[0]
        # The whole file goes over in a single execute, comments included
        # (the server strips them), never as a `.load` meta command.
        assert '+claim[("c1", "v1")]' in program
        assert "// a comment" in program
        assert not program.lstrip().startswith(".load")

    async def test_mode_is_rejected(self, tmp_path) -> None:
        pack = tmp_path / "pack.iql"
        pack.write_text("+r(x: string)\n")

        kg = _kg_with_mocked_execute()
        with pytest.raises(NotImplementedError):
            await kg.load(str(pack), mode="--replace")
        kg._execute.assert_not_awaited()

    async def test_missing_file_raises(self) -> None:
        kg = _kg_with_mocked_execute()
        with pytest.raises(FileNotFoundError):
            await kg.load("/nonexistent/pack.iql")
        kg._execute.assert_not_awaited()
