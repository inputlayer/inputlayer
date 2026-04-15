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
)
from inputlayer.integrations.langgraph._checkpoint_serde import pack as _pack
from inputlayer.integrations.langgraph._checkpoint_serde import parse_writes as _parse_writes
from inputlayer.integrations.langgraph._checkpoint_serde import unpack as _unpack
from inputlayer.integrations.langgraph._checkpointer_mixin import _SyncAndMaintenanceMixin
from inputlayer.integrations.langgraph._utils import (
    DEFAULT_KG_TIMEOUT,
    escape_iql,
    validate_row_length,
)

if TYPE_CHECKING:
    from langgraph.checkpoint.serde.base import SerializerProtocol

logger = logging.getLogger(__name__)

# 4 columns when checkpoint_id is bound (ParentId, Blob, Metadata, Ts).
# 5 columns when checkpoint_id is unbound (adds CheckpointId).
_MIN_CKPT_ROW_LEN = 4
_MIN_CKPT_ROW_LEN_WITH_ID = 5

# graph_write column indices for alist (6 unbound columns)
_WRITE_CKPT_ID = -6
_MIN_WRITE_ROW_LEN_ALIST = 6


def _apply_before_filter(
    sorted_rows: list[Sequence[Any]],
    before: RunnableConfig | None,
) -> list[Sequence[Any]]:
    """Filter rows to only those with timestamps before the cutoff checkpoint."""
    if before is None:
        return sorted_rows
    before_cfg = before.get("configurable", {}) if isinstance(before, dict) else {}
    before_id = before_cfg.get("checkpoint_id")
    if not before_id:
        return sorted_rows
    cutoff_ts = None
    for row in sorted_rows:
        if str(row[CKPT_ID]) == before_id:
            cutoff_ts = int(row[CKPT_TS])
            break
    if cutoff_ts is None:
        return sorted_rows
    return [row for row in sorted_rows if int(row[CKPT_TS]) < cutoff_ts]


def _group_writes_by_checkpoint(
    rows: list[Sequence[Any]],
) -> dict[str, list[Any]]:
    """Group write rows by their checkpoint ID."""
    by_ckpt: dict[str, list[Any]] = {}
    for w_row in rows:
        validate_row_length(
            w_row, _MIN_WRITE_ROW_LEN_ALIST, "graph_write", "alist",
        )
        ckpt_id = str(w_row[_WRITE_CKPT_ID])
        by_ckpt.setdefault(ckpt_id, []).append(w_row)
    return by_ckpt


def _build_checkpoint_tuple(
    serde: Any,
    row: Sequence[Any],
    thread_id: str,
    checkpoint_ns: str,
    writes_by_ckpt: dict[str, list[Any]],
) -> CheckpointTuple | None:
    """Build a CheckpointTuple from a checkpoint row and its writes."""
    checkpoint_id = str(row[CKPT_ID])
    parent_id = str(row[CKPT_PARENT_ID])

    checkpoint = _unpack(serde, str(row[CKPT_BLOB]))
    metadata = _unpack(serde, str(row[CKPT_METADATA]))

    pending_writes = _parse_writes(
        serde, writes_by_ckpt.get(checkpoint_id, []),
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

    return CheckpointTuple(
        config=ckpt_config,
        checkpoint=checkpoint,
        metadata=metadata,
        parent_config=parent_config,
        pending_writes=pending_writes,
    )


class InputLayerCheckpointer(_SyncAndMaintenanceMixin, BaseCheckpointSaver[str]):  # type: ignore[misc]
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
        kg_timeout: float = DEFAULT_KG_TIMEOUT,
    ) -> None:
        super().__init__(serde=serde or JsonPlusSerializer())
        self.kg = kg
        self._kg_timeout = kg_timeout
        self._setup_done = False
        self._setup_lock_guard = threading.Lock()
        self._setup_lock: asyncio.Lock | None = None

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
        except KeyError as exc:
            raise KeyError(
                "InputLayerCheckpointer.aput requires "
                "config['configurable']['thread_id']. "
                "Pass config={'configurable': {'thread_id': 'your-thread-id'}}"
                " to ainvoke()."
            ) from exc
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
            ) from exc
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
        except KeyError as exc:
            raise KeyError(
                "InputLayerCheckpointer.aget_tuple requires "
                "config['configurable']['thread_id']. "
                "Pass config={'configurable': {'thread_id': 'your-id'}}."
            ) from exc
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
        except KeyError as exc:
            raise KeyError(
                "InputLayerCheckpointer.alist requires "
                "config['configurable']['thread_id']. "
                "Pass config={'configurable': {'thread_id': 'your-id'}}."
            ) from exc
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        r = await self._exec(
            f'?graph_checkpoint("{escape_iql(thread_id)}", '
            f'"{escape_iql(checkpoint_ns)}", '
            f"CheckpointId, ParentId, Blob, Metadata, Ts)"
        )
        for row in r.rows:
            validate_row_length(row, _MIN_CKPT_ROW_LEN_WITH_ID, "graph_checkpoint", "alist")

        sorted_rows = sorted(r.rows, key=lambda row: int(row[CKPT_TS]), reverse=True)
        sorted_rows = _apply_before_filter(sorted_rows, before)

        r_all_writes = await self._exec(
            f'?graph_write("{escape_iql(thread_id)}", '
            f'"{escape_iql(checkpoint_ns)}", '
            f"CheckpointId, TaskId, TaskPath, Idx, Channel, Blob)"
        )
        writes_by_ckpt = _group_writes_by_checkpoint(r_all_writes.rows)

        count = 0
        for row in sorted_rows:
            tup = _build_checkpoint_tuple(
                self.serde, row, thread_id, checkpoint_ns, writes_by_ckpt,
            )
            if tup is None:
                continue
            if filter and not all(tup.metadata.get(k) == v for k, v in filter.items()):
                continue

            yield tup

            count += 1
            if limit is not None and count >= limit:
                return
