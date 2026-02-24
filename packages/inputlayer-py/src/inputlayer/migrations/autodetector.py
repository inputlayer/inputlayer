"""Autodetector - diff two ModelStates to produce a list of Operations."""

from __future__ import annotations

from inputlayer.migrations.operations import (
    CreateIndex,
    CreateRelation,
    CreateRule,
    DropIndex,
    DropRelation,
    DropRule,
    Operation,
    ReplaceRule,
)
from inputlayer.migrations.state import ModelState


def detect_changes(old: ModelState, new: ModelState) -> list[Operation]:
    """Diff two states and return an ordered list of operations.

    Ordering:
    1. Create new relations (needed before rules can reference them)
    2. Drop old rules (before relations they depend on are dropped)
    3. Replace modified rules
    4. Create new rules
    5. Drop removed relations
    6. Handle indexes (drop removed, create new)
    """
    ops: list[Operation] = []

    old_rels = set(old.relations)
    new_rels = set(new.relations)
    old_rules = set(old.rules)
    new_rules = set(new.rules)
    old_idxs = set(old.indexes)
    new_idxs = set(new.indexes)

    # 1. Create new relations
    for name in sorted(new_rels - old_rels):
        ops.append(CreateRelation(name=name, columns=new.relations[name]))

    # 2. Modified relations (columns changed) → drop + recreate
    #    InputLayer can't ALTER, so this is destructive
    for name in sorted(old_rels & new_rels):
        if old.relations[name] != new.relations[name]:
            ops.append(DropRelation(name=name, columns=old.relations[name]))
            ops.append(CreateRelation(name=name, columns=new.relations[name]))

    # 3. Drop rules that no longer exist
    for name in sorted(old_rules - new_rules):
        ops.append(DropRule(name=name, clauses=old.rules[name]))

    # 4. Replace modified rules
    for name in sorted(old_rules & new_rules):
        if old.rules[name] != new.rules[name]:
            ops.append(ReplaceRule(
                name=name,
                old_clauses=old.rules[name],
                new_clauses=new.rules[name],
            ))

    # 5. Create new rules
    for name in sorted(new_rules - old_rules):
        ops.append(CreateRule(name=name, clauses=new.rules[name]))

    # 6. Drop removed relations (after their rules are gone)
    for name in sorted(old_rels - new_rels):
        ops.append(DropRelation(name=name, columns=old.relations[name]))

    # 7. Drop removed indexes
    for name in sorted(old_idxs - new_idxs):
        info = old.indexes[name]
        ops.append(DropIndex(
            name=name,
            relation=info["relation"],
            column=info["column"],
            metric=info.get("metric", "cosine"),
            m=info.get("m", 16),
            ef_construction=info.get("ef_construction", 100),
            ef_search=info.get("ef_search", 50),
        ))

    # 8. Modified indexes → drop + recreate
    for name in sorted(old_idxs & new_idxs):
        if old.indexes[name] != new.indexes[name]:
            old_info = old.indexes[name]
            ops.append(DropIndex(
                name=name,
                relation=old_info["relation"],
                column=old_info["column"],
                metric=old_info.get("metric", "cosine"),
                m=old_info.get("m", 16),
                ef_construction=old_info.get("ef_construction", 100),
                ef_search=old_info.get("ef_search", 50),
            ))
            new_info = new.indexes[name]
            ops.append(CreateIndex(
                name=name,
                relation=new_info["relation"],
                column=new_info["column"],
                metric=new_info.get("metric", "cosine"),
                m=new_info.get("m", 16),
                ef_construction=new_info.get("ef_construction", 100),
                ef_search=new_info.get("ef_search", 50),
            ))

    # 9. Create new indexes
    for name in sorted(new_idxs - old_idxs):
        info = new.indexes[name]
        ops.append(CreateIndex(
            name=name,
            relation=info["relation"],
            column=info["column"],
            metric=info.get("metric", "cosine"),
            m=info.get("m", 16),
            ef_construction=info.get("ef_construction", 100),
            ef_search=info.get("ef_search", 50),
        ))

    return ops
