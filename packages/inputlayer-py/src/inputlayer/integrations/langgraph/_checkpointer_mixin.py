"""Sync wrappers, setup, and maintenance for InputLayerCheckpointer.

Separated from checkpointer.py to keep individual files under 500 lines.
Methods are mixed into InputLayerCheckpointer via inheritance.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)

from inputlayer._sync import run_sync
from inputlayer.integrations.langgraph._checkpoint_serde import CKPT_ID, CKPT_TS
from inputlayer.integrations.langgraph._utils import (
    check_error_response,
    escape_iql,
    validate_row_length,
)

logger = logging.getLogger(__name__)


class _SyncAndMaintenanceMixin:
    """Setup, sync wrappers, and maintenance ops for InputLayerCheckpointer.

    Attributes and async methods below are declared for mypy; concrete
    values are set by ``InputLayerCheckpointer.__init__`` and its
    method definitions.
    """

    kg: Any
    _kg_timeout: float
    _setup_done: bool
    _setup_lock_guard: threading.Lock
    _setup_lock: asyncio.Lock | None

    # Forward declarations for methods defined on the concrete subclass.
    # The concrete implementations live in checkpointer.py; these stubs
    # let mypy verify the sync wrappers below.
    async def aput(  # type: ignore[empty-body]
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig: ...

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None: ...

    async def aget_tuple(
        self,
        config: RunnableConfig,
    ) -> CheckpointTuple | None: ...

    def alist(  # type: ignore[empty-body]
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]: ...

    async def adelete_thread(self, thread_id: str) -> None: ...

    # ── Setup & infrastructure ──────────────────────────────────────

    async def _exec(self, iql: str) -> Any:
        """Execute IQL against the KG with a timeout."""
        try:
            result = await asyncio.wait_for(
                self.kg.execute(iql),
                timeout=self._kg_timeout,
            )
        except (asyncio.TimeoutError, TimeoutError):
            raise TimeoutError(
                f"KG operation timed out after {self._kg_timeout}s. "
                f"Query: {iql[:100]}{'...' if len(iql) > 100 else ''}"
            ) from None
        check_error_response(result, "InputLayerCheckpointer", iql)
        return result

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

    # ── Sync wrappers ───────────────────────────────────────────────

    def setup_sync(self) -> None:
        """Sync wrapper for setup()."""
        run_sync(self.setup())

    # ── Sync put / get / writes ──────────────────────────────────────

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Persist a checkpoint (blocking). See ``aput`` for details."""
        return run_sync(self.aput(config, checkpoint, metadata, new_versions))

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Persist intermediate writes (blocking). See ``aput_writes`` for details."""
        run_sync(self.aput_writes(config, writes, task_id, task_path))

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Retrieve a checkpoint by config (blocking). See ``aget_tuple`` for details."""
        return run_sync(self.aget_tuple(config))

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints for a thread (blocking). See ``alist`` for details."""
        collected: Sequence[CheckpointTuple] = run_sync(
            self._alist_collect(
                config,
                filter=filter,
                before=before,
                limit=limit,
            )
        )
        yield from collected

    async def _alist_collect(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Sequence[CheckpointTuple]:
        """Collect alist results into a list (used by sync list())."""
        return [
            tup
            async for tup in self.alist(
                config,
                filter=filter,
                before=before,
                limit=limit,
            )
        ]

    # ── Maintenance ──────────────────────────────────────────────────

    async def prune_thread(
        self,
        thread_id: str,
        *,
        checkpoint_ns: str = "",
        keep_last: int = 10,
    ) -> int:
        """Remove old checkpoints and their writes, keeping the most recent.

        Deletes are batched: one DELETE per checkpoint ID is issued
        concurrently for both checkpoints and writes to avoid N+1
        round-trips.

        Args:
            thread_id: The thread to prune.
            checkpoint_ns: Namespace (default ``""`` for parent graph).
            keep_last: Number of most recent checkpoints to keep (default 10).

        Returns:
            Number of checkpoints removed.
        """
        if not thread_id:
            raise ValueError(
                "InputLayerCheckpointer.prune_thread: thread_id must be a "
                "non-empty string."
            )
        await self.setup()

        if keep_last < 1:
            raise ValueError(
                f"InputLayerCheckpointer.prune_thread: keep_last must be >= 1, "
                f"got {keep_last}."
            )

        r = await self._exec(
            f'?graph_checkpoint("{escape_iql(thread_id)}", '
            f'"{escape_iql(checkpoint_ns)}", '
            f"CheckpointId, ParentId, Blob, Metadata, Ts)"
        )

        if len(r.rows) <= keep_last:
            return 0

        for row in r.rows:
            validate_row_length(row, 5, "graph_checkpoint", "prune_thread")
        sorted_rows = sorted(r.rows, key=lambda row: int(row[CKPT_TS]), reverse=True)
        to_prune = sorted_rows[keep_last:]

        logger.info(
            "InputLayerCheckpointer: pruning %d checkpoints for thread=%r ns=%r",
            len(to_prune),
            thread_id,
            checkpoint_ns,
        )

        await self._batch_delete_checkpoints(thread_id, checkpoint_ns, to_prune)
        return len(to_prune)

    async def _batch_delete_checkpoints(
        self,
        thread_id: str,
        checkpoint_ns: str,
        rows: Sequence[Any],
    ) -> None:
        """Delete checkpoint and write rows concurrently."""
        coros: Any = []
        esc_tid = escape_iql(thread_id)
        esc_ns = escape_iql(checkpoint_ns)
        for row in rows:
            ckpt_id = escape_iql(str(row[CKPT_ID]))
            coros.append(
                self._exec(
                    f"-graph_checkpoint(ThreadId, Ns, CkptId, P, B, M, T) <- "
                    f'ThreadId = "{esc_tid}", Ns = "{esc_ns}", CkptId = "{ckpt_id}"'
                )
            )
            coros.append(
                self._exec(
                    f"-graph_write(ThreadId, Ns, CkptId, TaskId, TaskPath, "
                    f"Idx, Channel, Blob) <- "
                    f'ThreadId = "{esc_tid}", Ns = "{esc_ns}", CkptId = "{ckpt_id}"'
                )
            )
        results = await asyncio.gather(*coros, return_exceptions=True)
        errors = [r for r in results if isinstance(r, BaseException)]
        if errors:
            logger.error(
                "InputLayerCheckpointer._batch_delete_checkpoints: "
                "%d/%d deletes failed for thread=%r ns=%r",
                len(errors),
                len(coros),
                thread_id,
                checkpoint_ns,
            )
            raise RuntimeError(
                f"_batch_delete_checkpoints: {len(errors)}/{len(coros)} "
                f"deletes failed. First error: {errors[0]}"
            ) from errors[0]

    def prune_thread_sync(
        self,
        thread_id: str,
        *,
        checkpoint_ns: str = "",
        keep_last: int = 10,
    ) -> int:
        """Sync wrapper for ``prune_thread()``.

        Named ``prune_thread_sync`` to avoid clashing with
        ``BaseCheckpointSaver.prune(thread_ids, *, strategy)``, which has
        a different signature reserved for future LangGraph runtime use.
        """
        return run_sync(
            self.prune_thread(
                thread_id,
                checkpoint_ns=checkpoint_ns,
                keep_last=keep_last,
            )
        )

    def delete_thread(self, thread_id: str) -> None:
        """Delete all checkpoints and writes for a thread (blocking).

        See ``adelete_thread`` for details.
        """
        run_sync(self.adelete_thread(thread_id))
