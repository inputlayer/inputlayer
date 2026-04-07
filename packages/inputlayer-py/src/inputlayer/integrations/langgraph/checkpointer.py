"""InputLayerCheckpointer — LangGraph BaseCheckpointSaver backed by InputLayer.

Stores graph checkpoints as facts in a KG, allowing graph executions
to be persisted and resumed across processes/restarts.

Schema (created automatically):
    +graph_checkpoint(thread_id, checkpoint_id, parent_id, blob, metadata, ts)
    +graph_write(thread_id, checkpoint_id, task_id, idx, channel, blob)

Usage::

    from inputlayer.integrations.langgraph import InputLayerCheckpointer

    checkpointer = InputLayerCheckpointer(kg=kg)
    await checkpointer.setup()  # creates relations if needed

    graph = StateGraph(State)
    # ... build graph ...
    app = graph.compile(checkpointer=checkpointer)

    config = {"configurable": {"thread_id": "user-123"}}
    result = await app.ainvoke({"messages": [...]}, config=config)

    # Later, in another process:
    state = await app.aget_state(config)
"""

from __future__ import annotations

import base64
import time
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)
from langgraph.checkpoint.serde.base import SerializerProtocol
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from inputlayer._sync import run_sync


def _b64_encode(data: bytes) -> str:
    """Encode bytes as base64 string for safe Datalog string storage."""
    return base64.b64encode(data).decode("ascii")


def _b64_decode(data: str) -> bytes:
    """Decode base64 string back to bytes."""
    return base64.b64decode(data.encode("ascii"))


def _escape(s: str) -> str:
    """Escape a string for Datalog literal."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


class InputLayerCheckpointer(BaseCheckpointSaver[str]):
    """LangGraph checkpointer backed by an InputLayer KnowledgeGraph.

    Persists graph state as facts so that graph executions can be
    resumed across processes, restarts, and machines.
    """

    def __init__(
        self,
        kg: Any,
        *,
        serde: SerializerProtocol | None = None,
    ) -> None:
        super().__init__(serde=serde or JsonPlusSerializer())
        self.kg = kg
        self._setup_done = False

    async def _exec(self, datalog: str) -> Any:
        """Thin wrapper around kg.execute().

        Concurrent safety is handled by the underlying Connection's
        internal lock — multiple coroutines can call this safely.
        """
        return await self.kg.execute(datalog)

    async def setup(self) -> None:
        """Create the checkpoint relations if they don't exist.

        Idempotent — safe to call multiple times.
        """
        if self._setup_done:
            return

        # Best-effort schema creation; if relations exist, the server
        # returns an error which we silently ignore.
        for ddl in [
            "+graph_checkpoint(thread_id: string, checkpoint_id: string, "
            "parent_id: string, blob: string, metadata: string, ts: int)",
            "+graph_write(thread_id: string, checkpoint_id: string, "
            "task_id: string, idx: int, channel: string, blob: string)",
        ]:
            await self._exec(ddl)

        self._setup_done = True

    def setup_sync(self) -> None:
        run_sync(self.setup())

    # ── Async API (native) ───────────────────────────────────────────

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Persist a checkpoint."""
        await self.setup()

        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = checkpoint["id"]
        parent_id = config["configurable"].get("checkpoint_id", "")

        # Serialize checkpoint and metadata
        type_, blob = self.serde.dumps_typed(checkpoint)
        meta_type, meta_blob = self.serde.dumps_typed(metadata)

        # Pack type + blob together so we can reconstruct on read
        packed_blob = f"{type_}|{_b64_encode(blob)}"
        packed_meta = f"{meta_type}|{_b64_encode(meta_blob)}"

        await self._exec(
            f'+graph_checkpoint("{_escape(thread_id)}", '
            f'"{_escape(checkpoint_id)}", '
            f'"{_escape(parent_id)}", '
            f'"{_escape(packed_blob)}", '
            f'"{_escape(packed_meta)}", '
            f"{time.time_ns()})"
        )

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": config["configurable"].get("checkpoint_ns", ""),
                "checkpoint_id": checkpoint_id,
            }
        }

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Persist intermediate writes for a checkpoint."""
        await self.setup()

        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = config["configurable"]["checkpoint_id"]

        for idx, (channel, value) in enumerate(writes):
            type_, blob = self.serde.dumps_typed(value)
            packed = f"{type_}|{_b64_encode(blob)}"
            await self._exec(
                f'+graph_write("{_escape(thread_id)}", '
                f'"{_escape(checkpoint_id)}", '
                f'"{_escape(task_id)}", '
                f"{idx}, "
                f'"{_escape(channel)}", '
                f'"{_escape(packed)}")'
            )

    async def aget_tuple(
        self,
        config: RunnableConfig,
    ) -> CheckpointTuple | None:
        """Retrieve a checkpoint by config."""
        await self.setup()

        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = config["configurable"].get("checkpoint_id")

        if checkpoint_id:
            # Get a specific checkpoint
            r = await self._exec(
                f'?graph_checkpoint("{_escape(thread_id)}", '
                f'"{_escape(checkpoint_id)}", ParentId, Blob, Metadata, Ts)'
            )
        else:
            # Get the latest checkpoint for this thread
            r = await self._exec(
                f'?graph_checkpoint("{_escape(thread_id)}", '
                f"CheckpointId, ParentId, Blob, Metadata, Ts)"
            )

        if not r.rows:
            return None

        # Pick the latest by timestamp (last column).
        # Parse from the end of the row — works whether the server
        # includes bound columns in the result or strips them.
        # Column order: thread_id, checkpoint_id, parent_id, blob, metadata, ts
        row = max(r.rows, key=lambda r: r[-1])
        meta_packed = str(row[-2])
        blob_packed = str(row[-3])
        parent_id = str(row[-4])
        actual_id = checkpoint_id if checkpoint_id else str(row[-5])

        # Unpack and deserialize
        type_, b64_blob = blob_packed.split("|", 1)
        checkpoint = self.serde.loads_typed((type_, _b64_decode(b64_blob)))

        meta_type, meta_b64 = meta_packed.split("|", 1)
        metadata = self.serde.loads_typed((meta_type, _b64_decode(meta_b64)))

        # Get pending writes for this checkpoint
        pending_writes: list[tuple[str, str, Any]] = []
        r_writes = await self._exec(
            f'?graph_write("{_escape(thread_id)}", '
            f'"{_escape(actual_id)}", TaskId, Idx, Channel, Blob)'
        )
        # Sort by task_id, idx — parse from end of row for resilience
        sorted_writes = sorted(r_writes.rows, key=lambda r: (r[-4], r[-3]))
        for w_row in sorted_writes:
            task_id = str(w_row[-4])
            channel = str(w_row[-2])
            packed = str(w_row[-1])
            w_type, w_b64 = packed.split("|", 1)
            value = self.serde.loads_typed((w_type, _b64_decode(w_b64)))
            pending_writes.append((task_id, channel, value))

        new_config: RunnableConfig = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": config["configurable"].get("checkpoint_ns", ""),
                "checkpoint_id": actual_id,
            }
        }

        parent_config: RunnableConfig | None = None
        if parent_id:
            parent_config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": config["configurable"].get("checkpoint_ns", ""),
                    "checkpoint_id": parent_id,
                }
            }

        return CheckpointTuple(
            config=new_config,
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=parent_config,
            pending_writes=pending_writes,
        )

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """List checkpoints for a thread."""
        await self.setup()

        if config is None:
            return

        thread_id = config["configurable"]["thread_id"]

        r = await self._exec(
            f'?graph_checkpoint("{_escape(thread_id)}", CheckpointId, ParentId, Blob, Metadata, Ts)'
        )

        # Sort by ts descending (newest first)
        sorted_rows = sorted(r.rows, key=lambda r: r[-1], reverse=True)

        if limit:
            sorted_rows = sorted_rows[:limit]

        for row in sorted_rows:
            # Parse from end of row — server may include bound thread_id
            checkpoint_id = str(row[-5])
            parent_id = str(row[-4])
            blob_packed = str(row[-3])
            meta_packed = str(row[-2])

            type_, b64_blob = blob_packed.split("|", 1)
            checkpoint = self.serde.loads_typed((type_, _b64_decode(b64_blob)))

            meta_type, meta_b64 = meta_packed.split("|", 1)
            metadata = self.serde.loads_typed((meta_type, _b64_decode(meta_b64)))

            ckpt_config: RunnableConfig = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": "",
                    "checkpoint_id": checkpoint_id,
                }
            }

            parent_config: RunnableConfig | None = None
            if parent_id:
                parent_config = {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": "",
                        "checkpoint_id": parent_id,
                    }
                }

            yield CheckpointTuple(
                config=ckpt_config,
                checkpoint=checkpoint,
                metadata=metadata,
                parent_config=parent_config,
                pending_writes=[],
            )

    # ── Sync API (via run_sync bridge) ───────────────────────────────

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return run_sync(self.aput(config, checkpoint, metadata, new_versions))

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        run_sync(self.aput_writes(config, writes, task_id, task_path))

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return run_sync(self.aget_tuple(config))

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        async def _collect() -> list[CheckpointTuple]:
            results = []
            async for tup in self.alist(config, filter=filter, before=before, limit=limit):
                results.append(tup)
            return results

        items = run_sync(_collect())
        yield from items
