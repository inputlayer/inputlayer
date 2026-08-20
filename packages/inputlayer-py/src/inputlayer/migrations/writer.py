"""Migration file writer - generates language-neutral JSON migration files.

A migration is data, not code: typed operations (each with enough
structure to derive both apply and revert IQL) plus a state snapshot.
Any SDK language - and the il CLI natively - can read and apply them.
Legacy .py migrations remain loadable (see loader.py) but are no longer
generated.
"""

from __future__ import annotations

import json
from typing import Any

from inputlayer.migrations.operations import Operation

MIGRATION_FORMAT = 1


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

    filename = f"{number:04d}_{name_suffix}.json"

    document = {
        "format": MIGRATION_FORMAT,
        "dependencies": dependencies,
        "operations": [op.to_dict() for op in operations],
        "state": state,
    }
    return filename, json.dumps(document, indent=2, sort_keys=False) + "\n"
