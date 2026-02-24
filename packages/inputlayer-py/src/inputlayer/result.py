"""ResultSet - typed, iterable query results."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Iterator


@dataclass
class ResultSet:
    """Container for query results.

    Supports iteration, indexing, and conversion to dicts/tuples/DataFrames.
    """

    columns: list[str]
    rows: list[list[Any]]
    row_count: int = 0
    total_count: int = 0
    truncated: bool = False
    execution_time_ms: int = 0
    row_provenance: list[str] | None = None
    has_ephemeral: bool = False
    ephemeral_sources: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    # Optional Relation class for typed iteration
    _relation_cls: type | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.row_count == 0:
            self.row_count = len(self.rows)
        if self.total_count == 0:
            self.total_count = self.row_count

    def __len__(self) -> int:
        return self.row_count

    def __bool__(self) -> bool:
        return self.row_count > 0

    def __iter__(self) -> Iterator[Any]:
        for row in self.rows:
            yield self._row_to_obj(row)

    def __getitem__(self, idx: int) -> Any:
        return self._row_to_obj(self.rows[idx])

    def first(self) -> Any | None:
        """Return the first row as an object, or None if empty."""
        if not self.rows:
            return None
        return self._row_to_obj(self.rows[0])

    def scalar(self) -> Any:
        """Return the single value from a 1×1 result."""
        if not self.rows or not self.rows[0]:
            raise ValueError("No results to extract scalar from")
        return self.rows[0][0]

    def to_dicts(self) -> list[dict[str, Any]]:
        """Convert all rows to list of dicts."""
        return [dict(zip(self.columns, row)) for row in self.rows]

    def to_tuples(self) -> list[tuple[Any, ...]]:
        """Convert all rows to list of tuples."""
        return [tuple(row) for row in self.rows]

    def to_df(self) -> Any:
        """Convert to a pandas DataFrame. Requires pandas."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for to_df(). "
                "Install with: pip install inputlayer[pandas]"
            )
        return pd.DataFrame(self.rows, columns=self.columns)

    def _row_to_obj(self, row: list[Any]) -> Any:
        """Convert a row to a typed object or SimpleNamespace."""
        if self._relation_cls is not None:
            try:
                kwargs = dict(zip(self.columns, row))
                return self._relation_cls(**kwargs)
            except Exception:
                pass
        return SimpleNamespace(**dict(zip(self.columns, row)))
