"""ModelState - snapshot of the full schema for diffing and embedding in migrations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from inputlayer.derived import Derived
    from inputlayer.index import HnswIndex
    from inputlayer.relation import Relation


@dataclass
class ModelState:
    """Snapshot of all relations, rules, and indexes at a point in time."""

    relations: dict[str, list[tuple[str, str]]] = field(default_factory=dict)
    rules: dict[str, list[str]] = field(default_factory=dict)
    indexes: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_models(
        cls,
        relations: list[type[Relation]] | None = None,
        derived: list[type[Derived]] | None = None,
        indexes: list[HnswIndex] | None = None,
    ) -> ModelState:
        """Build state by introspecting Python model classes."""
        from inputlayer.compiler import compile_rule
        from inputlayer.relation import Relation as RelBase
        from inputlayer.types import python_type_to_datalog

        state = cls()

        # Process plain relations
        for rel_cls in relations or []:
            name = RelBase._resolve_name(rel_cls)
            cols = RelBase._get_columns(rel_cls)
            col_types = RelBase._get_column_types(rel_cls)
            state.relations[name] = [
                (col, python_type_to_datalog(col_types[col])) for col in cols
            ]

        # Process derived relations (schema + rules)
        for der_cls in derived or []:
            name = RelBase._resolve_name(der_cls)
            cols = RelBase._get_columns(der_cls)
            col_types = RelBase._get_column_types(der_cls)
            state.relations[name] = [
                (col, python_type_to_datalog(col_types[col])) for col in cols
            ]

            # Compile each rule clause to Datalog
            compiled_clauses = []
            head_columns = cols
            for clause in der_cls.rules:
                datalog = compile_rule(
                    name,
                    head_columns,
                    clause.select_map,
                    clause.relations,
                    clause.condition,
                    persistent=True,
                )
                compiled_clauses.append(datalog)
            state.rules[name] = compiled_clauses

        # Process indexes
        for idx in indexes or []:
            rel_name = RelBase._resolve_name(idx.relation)
            state.indexes[idx.name] = {
                "relation": rel_name,
                "column": idx.column,
                "metric": idx.metric,
                "m": idx.m,
                "ef_construction": idx.ef_construction,
                "ef_search": idx.ef_search,
            }

        return state

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict for embedding in migration files."""
        return {
            "relations": {
                name: [list(c) for c in cols]
                for name, cols in self.relations.items()
            },
            "rules": dict(self.rules),
            "indexes": dict(self.indexes),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ModelState:
        """Deserialize from a dict."""
        return cls(
            relations={
                name: [tuple(c) for c in cols]
                for name, cols in d.get("relations", {}).items()
            },
            rules=d.get("rules", {}),
            indexes=d.get("indexes", {}),
        )

    def is_empty(self) -> bool:
        return not self.relations and not self.rules and not self.indexes
