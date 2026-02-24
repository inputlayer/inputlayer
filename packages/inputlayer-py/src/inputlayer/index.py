"""HNSW index definition and compilation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from inputlayer.relation import Relation


@dataclass(frozen=True)
class HnswIndex:
    """HNSW vector index configuration.

    Compiles to::

        .index create <name> on <relation>(<column>) type hnsw
            metric <metric> m <m> ef_construction <ef_c> ef_search <ef_s>
    """

    name: str
    relation: type[Relation]
    column: str
    metric: str = "cosine"
    m: int = 16
    ef_construction: int = 100
    ef_search: int = 50

    def to_datalog(self) -> str:
        """Compile this index definition to a Datalog meta command."""
        from inputlayer.relation import Relation

        rel_name = Relation._resolve_name(self.relation)
        return (
            f".index create {self.name} on {rel_name}({self.column}) "
            f"type hnsw metric {self.metric} "
            f"m {self.m} ef_construction {self.ef_construction} "
            f"ef_search {self.ef_search}"
        )
