"""Migration operations - atomic schema/rule changes with forward and backward commands."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class CreateRelation:
    """Create a new relation with a typed schema."""

    name: str
    columns: list[tuple[str, str]]  # [(col_name, datalog_type), ...]

    def forward_commands(self) -> list[str]:
        parts = ", ".join(f"{col}: {tp}" for col, tp in self.columns)
        return [f"+{self.name}({parts})"]

    def backward_commands(self) -> list[str]:
        return [f".rel drop {self.name}"]

    def describe(self) -> str:
        return f"Create relation {self.name}"

    def to_dict(self) -> dict[str, Any]:
        return {"type": "CreateRelation", "name": self.name, "columns": self.columns}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> CreateRelation:
        return cls(name=d["name"], columns=[tuple(c) for c in d["columns"]])


@dataclass(frozen=True)
class DropRelation:
    """Drop an existing relation (stores columns for reversibility)."""

    name: str
    columns: list[tuple[str, str]]

    def forward_commands(self) -> list[str]:
        return [f".rel drop {self.name}"]

    def backward_commands(self) -> list[str]:
        parts = ", ".join(f"{col}: {tp}" for col, tp in self.columns)
        return [f"+{self.name}({parts})"]

    def describe(self) -> str:
        return f"Drop relation {self.name}"

    def to_dict(self) -> dict[str, Any]:
        return {"type": "DropRelation", "name": self.name, "columns": self.columns}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> DropRelation:
        return cls(name=d["name"], columns=[tuple(c) for c in d["columns"]])


@dataclass(frozen=True)
class CreateRule:
    """Create a new rule with one or more clauses."""

    name: str
    clauses: list[str]  # Compiled Datalog strings

    def forward_commands(self) -> list[str]:
        return list(self.clauses)

    def backward_commands(self) -> list[str]:
        return [f".rule drop {self.name}"]

    def describe(self) -> str:
        n = len(self.clauses)
        return f"Create rule {self.name} ({n} clause{'s' if n != 1 else ''})"

    def to_dict(self) -> dict[str, Any]:
        return {"type": "CreateRule", "name": self.name, "clauses": self.clauses}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> CreateRule:
        return cls(name=d["name"], clauses=d["clauses"])


@dataclass(frozen=True)
class DropRule:
    """Drop an existing rule (stores clauses for reversibility)."""

    name: str
    clauses: list[str]

    def forward_commands(self) -> list[str]:
        return [f".rule drop {self.name}"]

    def backward_commands(self) -> list[str]:
        return list(self.clauses)

    def describe(self) -> str:
        return f"Drop rule {self.name}"

    def to_dict(self) -> dict[str, Any]:
        return {"type": "DropRule", "name": self.name, "clauses": self.clauses}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> DropRule:
        return cls(name=d["name"], clauses=d["clauses"])


@dataclass(frozen=True)
class ReplaceRule:
    """Replace a rule's clauses (drop + recreate)."""

    name: str
    old_clauses: list[str]
    new_clauses: list[str]

    def forward_commands(self) -> list[str]:
        return [f".rule drop {self.name}"] + list(self.new_clauses)

    def backward_commands(self) -> list[str]:
        return [f".rule drop {self.name}"] + list(self.old_clauses)

    def describe(self) -> str:
        return f"Replace rule {self.name}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "ReplaceRule",
            "name": self.name,
            "old_clauses": self.old_clauses,
            "new_clauses": self.new_clauses,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ReplaceRule:
        return cls(
            name=d["name"],
            old_clauses=d["old_clauses"],
            new_clauses=d["new_clauses"],
        )


@dataclass(frozen=True)
class CreateIndex:
    """Create an HNSW vector index."""

    name: str
    relation: str
    column: str
    metric: str = "cosine"
    m: int = 16
    ef_construction: int = 100
    ef_search: int = 50

    def forward_commands(self) -> list[str]:
        return [
            f".index create {self.name} on {self.relation}({self.column}) "
            f"type hnsw metric {self.metric} "
            f"m {self.m} ef_construction {self.ef_construction} "
            f"ef_search {self.ef_search}"
        ]

    def backward_commands(self) -> list[str]:
        return [f".index drop {self.name}"]

    def describe(self) -> str:
        return f"Create index {self.name} on {self.relation}({self.column})"

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "CreateIndex",
            "name": self.name,
            "relation": self.relation,
            "column": self.column,
            "metric": self.metric,
            "m": self.m,
            "ef_construction": self.ef_construction,
            "ef_search": self.ef_search,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> CreateIndex:
        return cls(
            name=d["name"],
            relation=d["relation"],
            column=d["column"],
            metric=d.get("metric", "cosine"),
            m=d.get("m", 16),
            ef_construction=d.get("ef_construction", 100),
            ef_search=d.get("ef_search", 50),
        )


@dataclass(frozen=True)
class DropIndex:
    """Drop an HNSW vector index (stores params for reversibility)."""

    name: str
    relation: str
    column: str
    metric: str = "cosine"
    m: int = 16
    ef_construction: int = 100
    ef_search: int = 50

    def forward_commands(self) -> list[str]:
        return [f".index drop {self.name}"]

    def backward_commands(self) -> list[str]:
        return [
            f".index create {self.name} on {self.relation}({self.column}) "
            f"type hnsw metric {self.metric} "
            f"m {self.m} ef_construction {self.ef_construction} "
            f"ef_search {self.ef_search}"
        ]

    def describe(self) -> str:
        return f"Drop index {self.name}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "DropIndex",
            "name": self.name,
            "relation": self.relation,
            "column": self.column,
            "metric": self.metric,
            "m": self.m,
            "ef_construction": self.ef_construction,
            "ef_search": self.ef_search,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> DropIndex:
        return cls(
            name=d["name"],
            relation=d["relation"],
            column=d["column"],
            metric=d.get("metric", "cosine"),
            m=d.get("m", 16),
            ef_construction=d.get("ef_construction", 100),
            ef_search=d.get("ef_search", 50),
        )


@dataclass(frozen=True)
class RunDatalog:
    """Execute arbitrary Datalog commands (escape hatch)."""

    forward: list[str]
    backward: list[str]

    def forward_commands(self) -> list[str]:
        return list(self.forward)

    def backward_commands(self) -> list[str]:
        return list(self.backward)

    def describe(self) -> str:
        n = len(self.forward)
        return f"Run {n} custom Datalog command{'s' if n != 1 else ''}"

    def to_dict(self) -> dict[str, Any]:
        return {"type": "RunDatalog", "forward": self.forward, "backward": self.backward}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> RunDatalog:
        return cls(forward=d["forward"], backward=d["backward"])


# Union type for all operations
Operation = (
    CreateRelation
    | DropRelation
    | CreateRule
    | DropRule
    | ReplaceRule
    | CreateIndex
    | DropIndex
    | RunDatalog
)

_OPERATION_REGISTRY: dict[str, type] = {
    "CreateRelation": CreateRelation,
    "DropRelation": DropRelation,
    "CreateRule": CreateRule,
    "DropRule": DropRule,
    "ReplaceRule": ReplaceRule,
    "CreateIndex": CreateIndex,
    "DropIndex": DropIndex,
    "RunDatalog": RunDatalog,
}


def operation_from_dict(d: dict[str, Any]) -> Operation:
    """Deserialize an operation from a dict."""
    cls = _OPERATION_REGISTRY.get(d["type"])
    if cls is None:
        raise ValueError(f"Unknown operation type: {d['type']}")
    return cls.from_dict(d)
