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
import logging
import threading
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
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from inputlayer._sync import run_sync
from inputlayer.integrations.langgraph._checkpoint_serde import (
    pack as _pack,
    parse_writes as _parse_writes,
    unpack as _unpack,
)
from inputlayer.integrations.langgraph._utils import escape_iql

logger = logging.getLogger(__name__)

# Default timeout for KG operations (seconds). Prevents indefinite hangs
# when the KG is unreachable or under heavy load.
_DEFAULT_KG_TIMEOUT = 30.0

# ── Column indices for graph_checkpoint query results ───────────────
# Query: ?graph_checkpoint(ThreadId, Ns, CheckpointId, ParentId, Blob, Metadata, Ts)
# When thread_id/ns are bound, remaining columns shift left. We use
# negative indices so they work regardless of how many columns are bound.
_CKPT_TS = -1          # Ts (last column, always present)
_CKPT_METADATA = -2    # Metadata
_CKPT_BLOB = -3        # Blob
_CKPT_PARENT_ID = -4   # ParentId
_CKPT_ID = -5          # CheckpointId

# ── Column indices for graph_write query results ────────────────────
# Query: ?graph_write(ThreadId, CheckpointId, TaskId, Idx, Channel, Blob)
_WRITE_CKPT_ID = -5    # CheckpointId (when ThreadId is bound)


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
        return await asyncio.wait_for(
            self.kg.execute(iql),
            timeout=self._kg_timeout,
        )

    def _get_setup_lock(self) -> asyncio.Lock:
        """Get or create the setup lock, guarded by a threading.Lock for safety."""
        with self._setup_lock_guard:
            if self._setup_lock is None:
                self._setup_lock = asyncio.Lock()
            return self._setup_lock

    async def setup(self) -> None:
        """Create the checkpoint relations if they don't exist.

        Idempotent and concurrency-safe. The first caller runs the DDL;
        simultaneous callers wait and return once it completes.
        """
        if self._setup_done:
            return

        async with self._get_setup_lock():
            if self._setup_done:
                return

            logger.debug("InputLayerCheckpointer: creating checkpoint relations")

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
            logger.debug("InputLayerCheckpointer: setup complete")

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
        task_path: str = "",  # required by BaseCheckpointSaver protocol, unused here
    ) -> None:
        """Persist intermediate writes for a checkpoint.

        Deletes any existing writes for this (thread, checkpoint, task)
        before inserting so that retries don't accumulate duplicate rows.

        All writes are submitted concurrently. If any fail, the error is
        raised after all operations complete, preserving as many writes
        as possible rather than cancelling on first failure.
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

        # Submit all writes concurrently with return_exceptions=True so that
        # a single failed write doesn't cancel the rest. This preserves as
        # many writes as possible and reports all errors together.
        results = await asyncio.gather(
            *(
                self._exec(
                    f'+graph_write("{escape_iql(thread_id)}", '
                    f'"{escape_iql(checkpoint_id)}", '
                    f'"{escape_iql(task_id)}", '
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
                "InputLayerCheckpointer.aput_writes: %d/%d writes failed for "
                "thread=%r checkpoint=%r task=%r",
                failed, total, thread_id, checkpoint_id, task_id,
            )
            raise RuntimeError(
                f"aput_writes: {failed}/{total} writes failed. "
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

        # Pick the latest by timestamp
        row = max(r.rows, key=lambda row: int(row[_CKPT_TS]))
        parent_id = str(row[_CKPT_PARENT_ID])
        actual_id = checkpoint_id if checkpoint_id is not None else str(row[_CKPT_ID])

        checkpoint = _unpack(self.serde, str(row[_CKPT_BLOB]))
        metadata = _unpack(self.serde, str(row[_CKPT_METADATA]))

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
        sorted_rows = sorted(r.rows, key=lambda row: int(row[_CKPT_TS]), reverse=True)

        # Resolve the 'before' timestamp cutoff from the already-fetched rows
        if before is not None:
            before_id = before["configurable"].get("checkpoint_id")
            if before_id:
                cutoff_ts = None
                for row in sorted_rows:
                    if str(row[_CKPT_ID]) == before_id:
                        cutoff_ts = int(row[_CKPT_TS])
                        break
                if cutoff_ts is not None:
                    sorted_rows = [row for row in sorted_rows if int(row[_CKPT_TS]) < cutoff_ts]

        # Fetch all writes for this thread at once (one query, not N)
        r_all_writes = await self._exec(
            f'?graph_write("{escape_iql(thread_id)}", '
            f"CheckpointId, TaskId, Idx, Channel, Blob)"
        )
        writes_by_ckpt: dict[str, list[Any]] = {}
        for w_row in r_all_writes.rows:
            ckpt_id = str(w_row[_WRITE_CKPT_ID])
            writes_by_ckpt.setdefault(ckpt_id, []).append(w_row)

        count = 0
        for row in sorted_rows:
            checkpoint_id = str(row[_CKPT_ID])
            parent_id = str(row[_CKPT_PARENT_ID])

            checkpoint = _unpack(self.serde, str(row[_CKPT_BLOB]))
            metadata = _unpack(self.serde, str(row[_CKPT_METADATA]))

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

    # ── Maintenance ──────────────────────────────────────────────────

    async def aprune(
        self,
        thread_id: str,
        *,
        checkpoint_ns: str = "",
        keep_last: int = 10,
    ) -> int:
        """Remove old checkpoints and their writes, keeping the most recent ones.

        Checkpoints accumulate indefinitely. Call this periodically for
        long-running threads to prevent unbounded storage growth.

        Args:
            thread_id: The thread to prune.
            checkpoint_ns: Namespace to prune within (default ``""`` for the
                parent graph). Subgraph checkpoints use a different namespace,
                so pruning is scoped per-namespace to avoid accidentally
                deleting subgraph state.
            keep_last: Number of most recent checkpoints to keep (default 10).

        Returns:
            Number of checkpoints removed.
        """
        await self.setup()

        if keep_last < 1:
            raise ValueError("keep_last must be >= 1")

        r = await self._exec(
            f'?graph_checkpoint("{escape_iql(thread_id)}", '
            f'"{escape_iql(checkpoint_ns)}", '
            f"CheckpointId, ParentId, Blob, Metadata, Ts)"
        )

        if len(r.rows) <= keep_last:
            return 0

        # Sort newest-first, identify rows to prune
        sorted_rows = sorted(r.rows, key=lambda row: int(row[_CKPT_TS]), reverse=True)
        to_prune = sorted_rows[keep_last:]

        logger.info(
            "InputLayerCheckpointer: pruning %d checkpoints for thread=%r ns=%r",
            len(to_prune), thread_id, checkpoint_ns,
        )

        for row in to_prune:
            ckpt_id = str(row[_CKPT_ID])

            # Delete the checkpoint
            await self._exec(
                f'-graph_checkpoint(ThreadId, Ns, CkptId, P, B, M, T) <- '
                f'ThreadId = "{escape_iql(thread_id)}", '
                f'Ns = "{escape_iql(checkpoint_ns)}", '
                f'CkptId = "{escape_iql(ckpt_id)}"'
            )

            # Delete associated writes
            await self._exec(
                f'-graph_write(ThreadId, CkptId, TaskId, Idx, Channel, Blob) <- '
                f'ThreadId = "{escape_iql(thread_id)}", '
                f'CkptId = "{escape_iql(ckpt_id)}"'
            )

        return len(to_prune)

    def prune(
        self,
        thread_id: str,
        *,
        checkpoint_ns: str = "",
        keep_last: int = 10,
    ) -> int:
        return run_sync(self.aprune(
            thread_id, checkpoint_ns=checkpoint_ns, keep_last=keep_last,
        ))

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
        # Collect in the background loop and yield one at a time.
        # We must collect because the async iterator is tied to the
        # background event loop and can't be driven from the calling thread.
        results: list[CheckpointTuple] = run_sync(self._alist_collect(
            config, filter=filter, before=before, limit=limit,
        ))
        yield from results

    async def _alist_collect(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> list[CheckpointTuple]:
        """Collect alist results into a list (used by sync list())."""
        return [tup async for tup in self.alist(config, filter=filter, before=before, limit=limit)]
