"""Sync wrappers and maintenance for InputLayerCheckpointer.

Separated from checkpointer.py to keep individual files under 500 lines.
Methods are mixed into InputLayerCheckpointer via inheritance.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Sequence
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)

from inputlayer._sync import run_sync
from inputlayer.integrations.langgraph._utils import escape_iql

logger = logging.getLogger(__name__)

# Column indices needed by aprune (see checkpointer.py for full set).
_CKPT_TS = -1
_CKPT_ID = -5


class _SyncAndMaintenanceMixin:
    """Sync wrappers and maintenance ops for InputLayerCheckpointer."""

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
        return [
            tup async for tup in self.alist(
                config, filter=filter, before=before, limit=limit,
            )
        ]

    # ── Maintenance ──────────────────────────────────────────────────

    async def aprune(
        self,
        thread_id: str,
        *,
        checkpoint_ns: str = "",
        keep_last: int = 10,
    ) -> int:
        """Remove old checkpoints and their writes, keeping the most recent.

        Args:
            thread_id: The thread to prune.
            checkpoint_ns: Namespace (default ``""`` for parent graph).
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

        sorted_rows = sorted(
            r.rows, key=lambda row: int(row[_CKPT_TS]), reverse=True,
        )
        to_prune = sorted_rows[keep_last:]

        logger.info(
            "InputLayerCheckpointer: pruning %d checkpoints for thread=%r ns=%r",
            len(to_prune), thread_id, checkpoint_ns,
        )

        for row in to_prune:
            ckpt_id = str(row[_CKPT_ID])
            await self._exec(
                f"-graph_checkpoint(ThreadId, Ns, CkptId, P, B, M, T) <- "
                f'ThreadId = "{escape_iql(thread_id)}", '
                f'Ns = "{escape_iql(checkpoint_ns)}", '
                f'CkptId = "{escape_iql(ckpt_id)}"'
            )
            await self._exec(
                f"-graph_write(ThreadId, CkptId, TaskId, Idx, Channel, Blob) <- "
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
        """Sync wrapper for aprune()."""
        return run_sync(self.aprune(
            thread_id, checkpoint_ns=checkpoint_ns, keep_last=keep_last,
        ))
