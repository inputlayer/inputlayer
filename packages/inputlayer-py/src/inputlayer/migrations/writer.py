"""Migration file writer - generates Python source for migration files."""

from __future__ import annotations

from typing import Any

from inputlayer.migrations.operations import (
    CreateIndex,
    CreateRelation,
    CreateRule,
    DropIndex,
    DropRelation,
    DropRule,
    Operation,
    ReplaceRule,
    RunDatalog,
)


def _repr_str_list(items: list[str], indent: str = "            ") -> str:
    """Render a list of strings as a formatted Python list."""
    if not items:
        return "[]"
    if len(items) == 1:
        return f"[{items[0]!r}]"
    lines = [f"{indent}{item!r}," for item in items]
    return "[\n" + "\n".join(lines) + f"\n{indent[:-4]}]"


def _repr_columns(cols: list[tuple[str, str]], indent: str = "            ") -> str:
    """Render column list as Python source."""
    if not cols:
        return "[]"
    lines = [f'{indent}("{c}", "{t}"),' for c, t in cols]
    return "[\n" + "\n".join(lines) + f"\n{indent[:-4]}]"


def _render_operation(op: Operation) -> str:
    """Render a single operation as a Python constructor call."""
    if isinstance(op, CreateRelation):
        cols = _repr_columns(op.columns)
        return f'ops.CreateRelation(\n            name="{op.name}",\n            columns={cols},\n        )'
    if isinstance(op, DropRelation):
        cols = _repr_columns(op.columns)
        return f'ops.DropRelation(\n            name="{op.name}",\n            columns={cols},\n        )'
    if isinstance(op, CreateRule):
        clauses = _repr_str_list(op.clauses)
        return f'ops.CreateRule(\n            name="{op.name}",\n            clauses={clauses},\n        )'
    if isinstance(op, DropRule):
        clauses = _repr_str_list(op.clauses)
        return f'ops.DropRule(\n            name="{op.name}",\n            clauses={clauses},\n        )'
    if isinstance(op, ReplaceRule):
        old = _repr_str_list(op.old_clauses)
        new = _repr_str_list(op.new_clauses)
        return (
            f'ops.ReplaceRule(\n'
            f'            name="{op.name}",\n'
            f'            old_clauses={old},\n'
            f'            new_clauses={new},\n'
            f'        )'
        )
    if isinstance(op, CreateIndex):
        return (
            f'ops.CreateIndex(\n'
            f'            name="{op.name}",\n'
            f'            relation="{op.relation}",\n'
            f'            column="{op.column}",\n'
            f'            metric="{op.metric}",\n'
            f'            m={op.m},\n'
            f'            ef_construction={op.ef_construction},\n'
            f'            ef_search={op.ef_search},\n'
            f'        )'
        )
    if isinstance(op, DropIndex):
        return (
            f'ops.DropIndex(\n'
            f'            name="{op.name}",\n'
            f'            relation="{op.relation}",\n'
            f'            column="{op.column}",\n'
            f'            metric="{op.metric}",\n'
            f'            m={op.m},\n'
            f'            ef_construction={op.ef_construction},\n'
            f'            ef_search={op.ef_search},\n'
            f'        )'
        )
    if isinstance(op, RunDatalog):
        fwd = _repr_str_list(op.forward)
        bwd = _repr_str_list(op.backward)
        return f'ops.RunDatalog(\n            forward={fwd},\n            backward={bwd},\n        )'
    raise TypeError(f"Unknown operation type: {type(op).__name__}")


def _render_state(state: dict[str, Any]) -> str:
    """Render state dict as formatted Python source."""
    lines = ["    state = {"]

    # Relations
    lines.append('        "relations": {')
    for name, cols in sorted(state.get("relations", {}).items()):
        col_strs = [f'("{c}", "{t}")' for c, t in cols]
        lines.append(f'            "{name}": [{", ".join(col_strs)}],')
    lines.append("        },")

    # Rules
    lines.append('        "rules": {')
    for name, clauses in sorted(state.get("rules", {}).items()):
        if len(clauses) == 1:
            lines.append(f'            "{name}": [{clauses[0]!r}],')
        else:
            lines.append(f'            "{name}": [')
            for c in clauses:
                lines.append(f"                {c!r},")
            lines.append("            ],")
    lines.append("        },")

    # Indexes
    lines.append('        "indexes": {')
    for name, info in sorted(state.get("indexes", {}).items()):
        lines.append(f'            "{name}": {{')
        for k, v in info.items():
            lines.append(f'                "{k}": {v!r},')
        lines.append("            },")
    lines.append("        },")

    lines.append("    }")
    return "\n".join(lines)


def generate_migration(
    number: int,
    operations: list[Operation],
    state: dict[str, Any],
    dependencies: list[str],
    *,
    name_suffix: str | None = None,
) -> tuple[str, str]:
    """Generate a migration file.

    Returns (filename, content).
    """
    if number == 1 and name_suffix is None:
        name_suffix = "initial"
    elif name_suffix is None:
        name_suffix = "auto"

    filename = f"{number:04d}_{name_suffix}.py"

    # Build content
    lines = [
        f"# Migration: {filename}",
        "# Auto-generated by inputlayer-migrate",
        "",
        "from inputlayer.migrations import Migration",
        "from inputlayer.migrations import operations as ops",
        "",
        "",
        "class M(Migration):",
    ]

    # Dependencies
    if dependencies:
        deps_str = ", ".join(f'"{d}"' for d in dependencies)
        lines.append(f"    dependencies = [{deps_str}]")
    else:
        lines.append("    dependencies = []")
    lines.append("")

    # Operations
    if operations:
        lines.append("    operations = [")
        for op in operations:
            rendered = _render_operation(op)
            lines.append(f"        {rendered},")
        lines.append("    ]")
    else:
        lines.append("    operations = []")
    lines.append("")

    # State
    lines.append(_render_state(state))
    lines.append("")

    return filename, "\n".join(lines)
