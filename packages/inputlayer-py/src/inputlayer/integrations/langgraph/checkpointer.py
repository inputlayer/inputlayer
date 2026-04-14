"""InputLayerCheckpointer: LangGraph BaseCheckpointSaver backed by InputLayer.

Stores graph checkpoints as facts in a KG, allowing graph executions
to be persisted and resumed across processes/restarts.

Schema (created automatically):
    +graph_checkpoint(thread_id, checkpoint_ns, checkpoint_id, parent_id,
                      blob, metadata, ts)
    +graph_write(thread_id, checkpoint_ns, checkpoint_id, task_id,
                 task_path, idx, channel, blob)

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
import logging
import threading
import time
from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from inputlayer.integrations.langgraph._checkpoint_serde import (
    CKPT_BLOB,
    CKPT_ID,
    CKPT_METADATA,
    CKPT_PARENT_ID,
    CKPT_TS,
    pack as _pack,
    parse_writes as _parse_writes,
    unpack as _unpack,
)
from inputlayer.integrations.langgraph._checkpointer_mixin import (
    _SyncAndMaintenanceMixin,
)
from inputlayer.integrations.langgraph._utils import escape_iql, validate_row_length

if TYPE_CHECKING:
    from langgraph.checkpoint.serde.base import SerializerProtocol

logger = logging.getLogger(__name__)

_DEFAULT_KG_TIMEOUT = 30.0

# 4 columns when checkpoint_id is bound (ParentId, Blob, Metadata, Ts).
# 5 columns when checkpoint_id is unbound (adds CheckpointId).
_MIN_CKPT_ROW_LEN = 4
_MIN_CKPT_ROW_LEN_WITH_ID = 5

# graph_write column indices for alist (6 unbound columns)
_WRITE_CKPT_ID = -6
_MIN_WRITE_ROW_LEN_ALIST = 6


class InputLayerCheckpointer(_SyncAndMaintenanceMixin, BaseCheckpointSaver[str]):
    """LangGraph checkpointer backed by an InputLayer KnowledgeGraph.

    Persists graph state as facts so that graph executions can be
    resumed across processes, restarts, and machines.

    Thread safety: ``setup()`` is guarded by a lock; the underlying
    KnowledgeGraph connection serializes commands, so concurrent
    ``aput``/``aget_tuple`` calls are safe.
    """

    def __init__(
        self,
        kg: Any,
        *,
        serde: SerializerProtocol | None = None,
        kg_timeout: float = _DEFAULT_KG_TIMEOUT,
    ) -> None:
        super().__init__(serde=serde or JsonPlusSerializer())
        self.kg = kg
        self._kg_timeout = kg_timeout
        self._setup_done = False
        self._setup_lock_guard = threading.Lock()
        self._setup_lock: asyncio.Lock | None = None

    async def _exec(self, iql: str) -> Any:
        """Execute IQL against the KG with a timeout."""
        try:
            return await asyncio.wait_for(
                self.kg.execute(iql),
                timeout=self._kg_timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"KG operation timed out after {self._kg_timeout}s. "
                f"Query: {iql[:100]}{'...' if len(iql) > 100 else ''}"
            ) from None

    def _get_setup_lock(self) -> asyncio.Lock:
        with self._setup_lock_guard:
            if self._setup_lock is None:
                self._setup_lock = asyncio.Lock()
            return self._setup_lock

    async def setup(self) -> None:
        """Create the checkpoint relations if they don't exist (idempotent)."""
        if self._setup_done:
            return

        async with self._get_setup_lock():
            if self._setup_done:
                return

            logger.debug("InputLayerCheckpointer: creating checkpoint relations")

            for ddl in [
                "+graph_checkpoint(thread_id: string, checkpoint_ns: string, "
                "checkpoint_id: string, parent_id: string, blob: string, "
                "metadata: string, ts: int)",
                "+graph_write(thread_id: string, checkpoint_ns: string, "
                "checkpoint_id: string, task_id: string, task_path: string, "
                "idx: int, channel: string, blob: string)",
            ]:
                await self._exec(ddl)

            self._setup_done = True
            logger.debug("InputLayerCheckpointer: setup complete")

    def __repr__(self) -> str:
        kg_name = getattr(self.kg, "name", repr(self.kg))
        return (
            f"InputLayerCheckpointer(kg={kg_name!r}, "
            f"kg_timeout={self._kg_timeout}, "
            f"setup_done={self._setup_done})"
        )

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
                "InputLayerCheckpointer.aput requires "
                "config['configurable']['thread_id']. "
                "Pass config={'configurable': {'thread_id': 'your-thread-id'}}"
                " to ainvoke()."
            ) from None
        checkpoint_id = checkpoint["id"]
        parent_id = config["configurable"].get("checkpoint_id")
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        packed_blob = _pack(self.serde, checkpoint)
        packed_meta = _pack(self.serde, metadata)

        await self._exec(
            f'+graph_checkpoint("{escape_iql(thread_id)}", '
            f'"{escape_iql(checkpoint_ns)}", '
            f'"{escape_iql(checkpoint_id)}", '
            f'"{escape_iql(parent_id or "")}", '
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
        task_path: str = "",
    ) -> None:
        """Persist intermediate writes for a checkpoint.

        Deletes existing writes for (thread, ns, checkpoint, task) before
        inserting to prevent duplicates on retry. All writes are submitted
        concurrently; errors are collected and raised together.

        Warning: the delete-then-insert is NOT atomic. If the process
        crashes between delete and insert, writes for this task will be
        lost. The alternative (insert first) would create duplicates that
        are harder to recover from.
        """
        await self.setup()

        try:
            thread_id = config["configurable"]["thread_id"]
            checkpoint_id = config["configurable"]["checkpoint_id"]
        except KeyError as exc:
            raise KeyError(
                "InputLayerCheckpointer.aput_writes requires "
                f"config['configurable'][{exc}]. "
                "Ensure your graph was compiled with this checkpointer and "
                "that config includes thread_id and checkpoint_id."
            ) from None
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        if not writes:
            return

        logger.debug(
            "InputLayerCheckpointer.aput_writes: deleting then inserting "
            "%d writes for thread=%r ns=%r checkpoint=%r task=%r "
            "(non-atomic: crash between delete and insert loses writes)",
            len(writes), thread_id, checkpoint_ns, checkpoint_id, task_id,
        )

        await self._exec(
            f"-graph_write(ThreadId, Ns, CkptId, TaskId, TaskPath, "
            f"Idx, Channel, Blob) <- "
            f'ThreadId = "{escape_iql(thread_id)}", '
            f'Ns = "{escape_iql(checkpoint_ns)}", '
            f'CkptId = "{escape_iql(checkpoint_id)}", '
            f'TaskId = "{escape_iql(task_id)}"'
        )

        results = await asyncio.gather(
            *(
                self._exec(
                    f'+graph_write("{escape_iql(thread_id)}", '
                    f'"{escape_iql(checkpoint_ns)}", '
                    f'"{escape_iql(checkpoint_id)}", '
                    f'"{escape_iql(task_id)}", '
                    f'"{escape_iql(task_path)}", '
                    f"{idx}, "
                    f'"{escape_iql(channel)}", '
                    f'"{escape_iql(_pack(self.serde, value))}")'
                )
                for idx, (channel, value) in enumerate(writes)
            ),
            return_exceptions=True,
        )

        errors = [r for r in results if isinstance(r, BaseException)]
        if errors:
            failed = len(errors)
            total = len(writes)
            logger.error(
                "InputLayerCheckpointer.aput_writes: %d/%d writes failed "
                "for thread=%r checkpoint=%r task=%r",
                failed, total, thread_id, checkpoint_id, task_id,
            )
            raise RuntimeError(
                f"InputLayerCheckpointer.aput_writes: "
                f"{failed}/{total} writes failed. "
                f"First error: {errors[0]}"
            ) from errors[0]

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
                "InputLayerCheckpointer.aget_tuple requires "
                "config['configurable']['thread_id']. "
                "Pass config={'configurable': {'thread_id': 'your-id'}}."
            ) from None
        checkpoint_id = config["configurable"].get("checkpoint_id")
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        if checkpoint_id:
            r = await self._exec(
                f'?graph_checkpoint("{escape_iql(thread_id)}", '
                f'"{escape_iql(checkpoint_ns)}", '
                f'"{escape_iql(checkpoint_id)}", '
                f"ParentId, Blob, Metadata, Ts)"
            )
        else:
            r = await self._exec(
                f'?graph_checkpoint("{escape_iql(thread_id)}", '
                f'"{escape_iql(checkpoint_ns)}", '
                f"CheckpointId, ParentId, Blob, Metadata, Ts)"
            )

        if not r.rows:
            return None

        min_cols = _MIN_CKPT_ROW_LEN if checkpoint_id else _MIN_CKPT_ROW_LEN_WITH_ID
        for row in r.rows:
            validate_row_length(row, min_cols, "graph_checkpoint", "aget_tuple")
        row = max(r.rows, key=lambda row: int(row[CKPT_TS]))
        parent_id = str(row[CKPT_PARENT_ID])
        actual_id = (
            checkpoint_id if checkpoint_id is not None else str(row[CKPT_ID])
        )

        checkpoint = _unpack(self.serde, str(row[CKPT_BLOB]))
        metadata = _unpack(self.serde, str(row[CKPT_METADATA]))

        r_writes = await self._exec(
            f'?graph_write("{escape_iql(thread_id)}", '
            f'"{escape_iql(checkpoint_ns)}", '
            f'"{escape_iql(actual_id)}", TaskId, TaskPath, Idx, Channel, Blob)'
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

    async def adelete_thread(self, thread_id: str) -> None:
        """Delete all checkpoints and writes for a thread."""
        await self.setup()
        await self._exec(
            f"-graph_checkpoint(ThreadId, Ns, CkptId, P, B, M, T) <- "
            f'ThreadId = "{escape_iql(thread_id)}"'
        )
        await self._exec(
            f"-graph_write(ThreadId, Ns, CkptId, TaskId, TaskPath, "
            f"Idx, Channel, Blob) <- "
            f'ThreadId = "{escape_iql(thread_id)}"'
        )

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """List checkpoints for a thread, newest first."""
        await self.setup()

        if config is None:
            return

        try:
            thread_id = config["configurable"]["thread_id"]
        except KeyError:
            raise KeyError(
                "InputLayerCheckpointer.alist requires "
                "config['configurable']['thread_id']. "
                "Pass config={'configurable': {'thread_id': 'your-id'}}."
            ) from None
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        r = await self._exec(
            f'?graph_checkpoint("{escape_iql(thread_id)}", '
            f'"{escape_iql(checkpoint_ns)}", '
            f"CheckpointId, ParentId, Blob, Metadata, Ts)"
        )

        for row in r.rows:
            validate_row_length(row, _MIN_CKPT_ROW_LEN_WITH_ID, "graph_checkpoint", "alist")
        sorted_rows = sorted(
            r.rows, key=lambda row: int(row[CKPT_TS]), reverse=True,
        )

        if before is not None:
            before_cfg = before.get("configurable", {}) if isinstance(before, dict) else {}
            before_id = before_cfg.get("checkpoint_id")
            if before_id:
                cutoff_ts = None
                for row in sorted_rows:
                    if str(row[CKPT_ID]) == before_id:
                        cutoff_ts = int(row[CKPT_TS])
                        break
                if cutoff_ts is not None:
                    sorted_rows = [
                        row for row in sorted_rows
                        if int(row[CKPT_TS]) < cutoff_ts
                    ]

        r_all_writes = await self._exec(
            f'?graph_write("{escape_iql(thread_id)}", '
            f'"{escape_iql(checkpoint_ns)}", '
            f"CheckpointId, TaskId, TaskPath, Idx, Channel, Blob)"
        )
        writes_by_ckpt: dict[str, list[Any]] = {}
        for w_row in r_all_writes.rows:
            validate_row_length(
                w_row, _MIN_WRITE_ROW_LEN_ALIST, "graph_write", "alist",
            )
            ckpt_id = str(w_row[_WRITE_CKPT_ID])
            writes_by_ckpt.setdefault(ckpt_id, []).append(w_row)

        count = 0
        for row in sorted_rows:
            checkpoint_id = str(row[CKPT_ID])
            parent_id = str(row[CKPT_PARENT_ID])

            checkpoint = _unpack(self.serde, str(row[CKPT_BLOB]))
            metadata = _unpack(self.serde, str(row[CKPT_METADATA]))

            if filter and not all(
                metadata.get(k) == v for k, v in filter.items()
            ):
                continue

            pending_writes = _parse_writes(
                self.serde, writes_by_ckpt.get(checkpoint_id, []),
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
