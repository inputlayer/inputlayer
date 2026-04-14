"""InputLayerCheckpointer: LangGraph BaseCheckpointSaver backed by InputLayer.

Stores graph checkpoints as facts in a KG, allowing graph executions
to be persisted and resumed across processes/restarts.

Schema (created automatically):
    +graph_checkpoint(thread_id, checkpoint_ns, checkpoint_id, parent_id, blob, metadata, ts)
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

import asyncio
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
from inputlayer.integrations.langgraph._utils import escape_iql


def _b64_encode(data: bytes) -> str:
    """Encode bytes as base64 string for safe IQL string storage."""
    return base64.b64encode(data).decode("ascii")


def _b64_decode(data: str) -> bytes:
    """Decode base64 string back to bytes."""
    return base64.b64decode(data.encode("ascii"))


def _pack(serde: SerializerProtocol, obj: Any) -> str:
    """Serialize obj and pack as 'type|base64blob'."""
    type_, blob = serde.dumps_typed(obj)
    return f"{type_}|{_b64_encode(blob)}"


def _unpack(serde: SerializerProtocol, packed: str) -> Any:
    """Unpack 'type|base64blob' and deserialize."""
    type_, b64 = packed.split("|", 1)
    return serde.loads_typed((type_, _b64_decode(b64)))


def _parse_writes(
    serde: SerializerProtocol,
    rows: list[Any],
) -> list[tuple[str, str, Any]]:
    """Parse graph_write rows into (task_id, channel, value) triples.

    Rows are expected to have columns:
    thread_id, checkpoint_id, task_id, idx, channel, blob
    Parsed from the end of the row for resilience to bound-column inclusion.
    """
    sorted_rows = sorted(rows, key=lambda r: (str(r[-4]), int(r[-3])))
    result = []
    for row in sorted_rows:
        task_id = str(row[-4])
        channel = str(row[-2])
        value = _unpack(serde, str(row[-1]))
        result.append((task_id, channel, value))
    return result


class InputLayerCheckpointer(BaseCheckpointSaver[str]):
    """LangGraph checkpointer backed by an InputLayer KnowledgeGraph.

    Persists graph state as facts so that graph executions can be
    resumed across processes, restarts, and machines.

    Thread safety: ``setup()`` is guarded by a lock; the underlying
    KnowledgeGraph connection serializes commands, so concurrent
    ``aput``/``aget_tuple`` calls are safe.

    LangGraph protocol compliance:
    - ``aput`` / ``put``: persist a checkpoint
    - ``aput_writes`` / ``put_writes``: persist intermediate writes,
      deleting any previous writes for the same (thread, checkpoint, task)
      to prevent duplicates on retry
    - ``aget_tuple`` / ``get_tuple``: retrieve latest or specific checkpoint
    - ``alist`` / ``list``: list checkpoints with full ``before`` filtering,
      ``filter`` support, ``limit``, and ``pending_writes`` populated
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
        self._setup_lock = asyncio.Lock()

    async def _exec(self, iql: str) -> Any:
        """Execute IQL against the KG."""
        return await self.kg.execute(iql)

    async def setup(self) -> None:
        """Create the checkpoint relations if they don't exist.

        Idempotent and concurrency-safe. The first caller runs the DDL;
        simultaneous callers wait and return once it completes.
        """
        if self._setup_done:
            return

        async with self._setup_lock:
            if self._setup_done:
                return

            # No try/except: exceptions here mean the server is unreachable.
            # Don't mark setup as done so the next operation retries cleanly.
            # Server-side "already exists" responses come back as ResultSet rows,
            # not exceptions, so they don't need to be caught here.
            for ddl in [
                "+graph_checkpoint(thread_id: string, checkpoint_ns: string, "
                "checkpoint_id: string, parent_id: string, blob: string, "
                "metadata: string, ts: int)",
                "+graph_write(thread_id: string, checkpoint_id: string, "
                "task_id: string, idx: int, channel: string, blob: string)",
            ]:
                await self._exec(ddl)

            self._setup_done = True

    def setup_sync(self) -> None:
        run_sync(self.setup())

    def __repr__(self) -> str:
        kg_name = getattr(self.kg, "name", repr(self.kg))
        return f"InputLayerCheckpointer(kg={kg_name!r}, setup_done={self._setup_done})"

    # ── Async API ────────────────────────────────────────────────────

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Persist a checkpoint."""
        await self.setup()

        try:
            thread_id = config["configurable"]["thread_id"]
        except KeyError:
            raise KeyError(
                "InputLayerCheckpointer.aput requires config['configurable']['thread_id']. "
                "Pass config={'configurable': {'thread_id': 'your-thread-id'}} to ainvoke()."
            ) from None
        checkpoint_id = checkpoint["id"]
        parent_id = config["configurable"].get("checkpoint_id", "")
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        packed_blob = _pack(self.serde, checkpoint)
        packed_meta = _pack(self.serde, metadata)

        await self._exec(
            f'+graph_checkpoint("{escape_iql(thread_id)}", '
            f'"{escape_iql(checkpoint_ns)}", '
            f'"{escape_iql(checkpoint_id)}", '
            f'"{escape_iql(parent_id)}", '
            f'"{escape_iql(packed_blob)}", '
            f'"{escape_iql(packed_meta)}", '
            f"{time.time_ns()})"
        )

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",  # required by BaseCheckpointSaver protocol, unused here
    ) -> None:
        """Persist intermediate writes for a checkpoint.

        Deletes any existing writes for this (thread, checkpoint, task)
        before inserting so that retries don't accumulate duplicate rows.
        """
        await self.setup()

        try:
            thread_id = config["configurable"]["thread_id"]
            checkpoint_id = config["configurable"]["checkpoint_id"]
        except KeyError as exc:
            raise KeyError(
                f"InputLayerCheckpointer.aput_writes requires config['configurable'][{exc}]. "
                "Ensure your graph was compiled with this checkpointer and that "
                "config includes thread_id and checkpoint_id."
            ) from None

        # Nothing to write - don't touch existing writes for this checkpoint
        if not writes:
            return

        # Delete existing writes for this task to prevent duplicates on retry
        await self._exec(
            f'-graph_write(ThreadId, CkptId, TaskId, Idx, Channel, Blob) <- '
            f'ThreadId = "{escape_iql(thread_id)}", '
            f'CkptId = "{escape_iql(checkpoint_id)}", '
            f'TaskId = "{escape_iql(task_id)}"'
        )

        for idx, (channel, value) in enumerate(writes):
            packed = _pack(self.serde, value)
            await self._exec(
                f'+graph_write("{escape_iql(thread_id)}", '
                f'"{escape_iql(checkpoint_id)}", '
                f'"{escape_iql(task_id)}", '
                f"{idx}, "
                f'"{escape_iql(channel)}", '
                f'"{escape_iql(packed)}")'
            )

    async def aget_tuple(
        self,
        config: RunnableConfig,
    ) -> CheckpointTuple | None:
        """Retrieve a checkpoint by config."""
        await self.setup()

        try:
            thread_id = config["configurable"]["thread_id"]
        except KeyError:
            raise KeyError(
                "InputLayerCheckpointer.aget_tuple requires config['configurable']['thread_id']. "
                "Pass config={'configurable': {'thread_id': 'your-thread-id'}}."
            ) from None
        checkpoint_id = config["configurable"].get("checkpoint_id")
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        if checkpoint_id:
            r = await self._exec(
                f'?graph_checkpoint("{escape_iql(thread_id)}", '
                f'"{escape_iql(checkpoint_ns)}", '
                f'"{escape_iql(checkpoint_id)}", ParentId, Blob, Metadata, Ts)'
            )
        else:
            r = await self._exec(
                f'?graph_checkpoint("{escape_iql(thread_id)}", '
                f'"{escape_iql(checkpoint_ns)}", '
                f"CheckpointId, ParentId, Blob, Metadata, Ts)"
            )

        if not r.rows:
            return None

        # Pick the latest by timestamp (last column)
        row = max(r.rows, key=lambda row: int(row[-1]))
        parent_id = str(row[-4])
        actual_id = checkpoint_id if checkpoint_id else str(row[-5])

        checkpoint = _unpack(self.serde, str(row[-3]))
        metadata = _unpack(self.serde, str(row[-2]))

        # Fetch pending writes for this checkpoint
        r_writes = await self._exec(
            f'?graph_write("{escape_iql(thread_id)}", '
            f'"{escape_iql(actual_id)}", TaskId, Idx, Channel, Blob)'
        )
        pending_writes = _parse_writes(self.serde, r_writes.rows)

        new_config: RunnableConfig = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": actual_id,
            }
        }

        parent_config: RunnableConfig | None = None
        if parent_id:
            parent_config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
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
        """List checkpoints for a thread, newest first.

        Args:
            config: Must contain ``configurable.thread_id``.
            filter: Metadata field filters. Each key/value must match the
                checkpoint's metadata exactly (checked after deserialization).
            before: If given, only return checkpoints with a timestamp
                strictly before this checkpoint's timestamp.
            limit: Maximum number of checkpoints to return.
        """
        await self.setup()

        if config is None:
            return

        try:
            thread_id = config["configurable"]["thread_id"]
        except KeyError:
            raise KeyError(
                "InputLayerCheckpointer.alist requires config['configurable']['thread_id']. "
                "Pass config={'configurable': {'thread_id': 'your-thread-id'}}."
            ) from None
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        r = await self._exec(
            f'?graph_checkpoint("{escape_iql(thread_id)}", '
            f'"{escape_iql(checkpoint_ns)}", '
            f"CheckpointId, ParentId, Blob, Metadata, Ts)"
        )

        # Sort newest first
        sorted_rows = sorted(r.rows, key=lambda row: int(row[-1]), reverse=True)

        # Resolve the 'before' timestamp cutoff
        if before is not None:
            before_id = before["configurable"].get("checkpoint_id")
            if before_id:
                r_before = await self._exec(
                    f'?graph_checkpoint("{escape_iql(thread_id)}", '
                    f'"{escape_iql(checkpoint_ns)}", '
                    f'"{escape_iql(before_id)}", _, _, _, Ts)'
                )
                if r_before.rows:
                    cutoff_ts = int(r_before.rows[0][-1])
                    sorted_rows = [row for row in sorted_rows if int(row[-1]) < cutoff_ts]

        # Fetch all writes for this thread at once (one query, not N)
        r_all_writes = await self._exec(
            f'?graph_write("{escape_iql(thread_id)}", '
            f"CheckpointId, TaskId, Idx, Channel, Blob)"
        )
        writes_by_ckpt: dict[str, list[Any]] = {}
        for w_row in r_all_writes.rows:
            ckpt_id = str(w_row[-5])
            writes_by_ckpt.setdefault(ckpt_id, []).append(w_row)

        count = 0
        for row in sorted_rows:
            checkpoint_id = str(row[-5])
            parent_id = str(row[-4])

            checkpoint = _unpack(self.serde, str(row[-3]))
            metadata = _unpack(self.serde, str(row[-2]))

            # Apply metadata filter
            if filter and not all(metadata.get(k) == v for k, v in filter.items()):
                continue

            pending_writes = _parse_writes(
                self.serde, writes_by_ckpt.get(checkpoint_id, [])
            )

            ckpt_config: RunnableConfig = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            }

            parent_config: RunnableConfig | None = None
            if parent_id:
                parent_config = {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": parent_id,
                    }
                }

            yield CheckpointTuple(
                config=ckpt_config,
                checkpoint=checkpoint,
                metadata=metadata,
                parent_config=parent_config,
                pending_writes=pending_writes,
            )

            count += 1
            if limit is not None and count >= limit:
                return

    # ── Sync API ─────────────────────────────────────────────────────

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
        task_path: str = "",  # required by BaseCheckpointSaver protocol, unused here
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

        yield from run_sync(_collect())
