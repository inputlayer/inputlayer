"""Example: Django-style migration system for InputLayer.

This example demonstrates how to use the migration system to manage
schema changes in a production-safe, version-controlled way.

The migration system provides:
- `makemigrations` - Generate numbered migration files from model diffs
- `migrate` - Apply pending migrations to a server
- `revert` - Roll back to a previous migration
- `showmigrations` - Show which migrations are applied

Usage (CLI):
    # 1. Define your models (this file or your own models.py)
    # 2. Generate migration:
    inputlayer-migrate makemigrations --models examples.10_migrations
    # 3. Apply to server:
    inputlayer-migrate migrate --url ws://localhost:8080/ws --kg production
    # 4. Show status:
    inputlayer-migrate showmigrations --url ws://localhost:8080/ws --kg production
    # 5. Revert if needed:
    inputlayer-migrate revert --url ws://localhost:8080/ws --kg production 0001_initial
"""

from typing import ClassVar

from inputlayer import Derived, From, HnswIndex, Relation, Timestamp, Vector


# ── Relations ────────────────────────────────────────────────────────


class Employee(Relation):
    id: int
    name: str
    department: str
    salary: float
    active: bool


class Department(Relation):
    name: str
    budget: float
    location: str


class Document(Relation):
    id: int
    title: str
    content: str
    embedding: Vector[384]
    created_at: Timestamp


# ── Derived Relations (Rules) ────────────────────────────────────────


class HighEarner(Derived):
    name: str
    salary: float

    rules: ClassVar[list] = []


HighEarner.rules = [
    From(Employee)
        .where(lambda e: e.salary > 100000)
        .select(name=Employee.name, salary=Employee.salary),
]


class DepartmentBudgetOk(Derived):
    dept: str

    rules: ClassVar[list] = []


DepartmentBudgetOk.rules = [
    From(Department)
        .where(lambda d: d.budget > 0)
        .select(dept=Department.name),
]


# ── Indexes ──────────────────────────────────────────────────────────


doc_search_index = HnswIndex(
    name="doc_search",
    relation=Document,
    column="embedding",
    metric="cosine",
    m=16,
    ef_construction=100,
    ef_search=50,
)


# ── Programmatic usage ───────────────────────────────────────────────

if __name__ == "__main__":
    from inputlayer.migrations.autodetector import detect_changes
    from inputlayer.migrations.state import ModelState
    from inputlayer.migrations.writer import generate_migration

    # Build state from current models
    state = ModelState.from_models(
        relations=[Employee, Department, Document],
        derived=[HighEarner, DepartmentBudgetOk],
        indexes=[doc_search_index],
    )

    print("Current model state:")
    print(f"  Relations: {list(state.relations.keys())}")
    print(f"  Rules: {list(state.rules.keys())}")
    print(f"  Indexes: {list(state.indexes.keys())}")

    # Generate an initial migration
    empty = ModelState()
    ops = detect_changes(empty, state)

    print(f"\nDetected {len(ops)} operations:")
    for op in ops:
        print(f"  - {op.describe()}")

    filename, content = generate_migration(1, ops, state.to_dict(), [])
    print(f"\nGenerated migration file: {filename}")
    print("--- content preview ---")
    for line in content.split("\n")[:20]:
        print(f"  {line}")
    print("  ...")
