"""Meta commands the SDK sends, in one table (R-META).

The table is pinned by the shared fixture
``packages/conformance/meta-commands.json``, which the JS SDK renders its own
table against and a live test runs against the engine, so a command spelling
cannot be wrong in one SDK only (``.session list``, ``.session remove`` and
``.rule show`` were). The reply parsers read the grammar the fixture records.
"""

from __future__ import annotations

import re
from typing import Any


def session_list() -> str:
    """List session facts and rules. The reply is parsed by :func:`session_rules`."""
    return ".session"


def session_drop(name_or_position: str | int) -> str:
    """Drop a session rule by name (every clause), or by 1-based position among all of them."""
    return f".session drop {name_or_position}"


def session_clear() -> str:
    """Clear every session fact and rule."""
    return ".session clear"


def rule_list() -> str:
    """List persistent rules. The reply is parsed by :func:`rule_infos`."""
    return ".rule list"


def rule_def(name: str) -> str:
    """Show a persistent rule's clauses. The reply is parsed by :func:`rule_clauses`."""
    return f".rule def {name}"


def rule_drop(name: str) -> str:
    """Drop every clause of a persistent rule."""
    return f".rule drop {name}"


def rule_drop_prefix(prefix: str) -> str:
    """Drop every persistent rule whose name starts with *prefix*."""
    return f".rule drop prefix {prefix}"


def rule_remove(name: str, index: int) -> str:
    """Remove one clause of a persistent rule, by 1-based index within that rule."""
    return f".rule remove {name} {index}"


def rule_clear(name: str) -> str:
    """Clear a persistent rule's clauses."""
    return f".rule clear {name}"


def subscribe(subscription_id: str, query: str) -> str:
    """Open a standing query."""
    return f".subscribe {subscription_id} {query}"


def unsubscribe(subscription_id: str) -> str:
    """Close a standing query."""
    return f".unsubscribe {subscription_id}"


def why(statement: str, full: bool = False) -> str:
    """Proof trees for a rule's derived rows."""
    return f".why full {statement}" if full else f".why {statement}"


def why_not(atom: str) -> str:
    """Why a fact was not derived."""
    return f".why_not {atom}"


#: Every command of the table by its fixture name.
COMMANDS = {
    fn.__name__: fn
    for fn in (
        session_list,
        session_drop,
        session_clear,
        rule_list,
        rule_def,
        rule_drop,
        rule_drop_prefix,
        rule_remove,
        rule_clear,
        subscribe,
        unsubscribe,
        why,
        why_not,
    )
}

_NUMBERED = re.compile(r"^\s+\d+\. (.*)$")
_RULE_INFO = re.compile(r"^\s+(\S+) \((\d+) clause\(s\)\)$")


def session_rules(rows: list[list[Any]]) -> list[str]:
    """Rule clauses in a ``.session`` reply, in definition order.

    The engine answers with message lines: ``No session data defined.``, or
    an optional ``Session facts (n):`` section, then ``Session rules (n):``
    followed by one ``  <position>. <clause>`` line per rule.
    """
    lines = [str(row[0]) for row in rows if row]
    start = next((i for i, line in enumerate(lines) if line.startswith("Session rules (")), None)
    if start is None:
        return []
    return [m.group(1) for line in lines[start + 1 :] if (m := _NUMBERED.match(line))]


def rule_infos(rows: list[list[Any]]) -> list[tuple[str, int]]:
    """``(name, clause count)`` per rule in a ``.rule list`` reply: a ``Rules:``
    line, then one ``  <name> (<n> clause(s))`` line per rule."""
    return [
        (m.group(1), int(m.group(2)))
        for row in rows
        if row and (m := _RULE_INFO.match(str(row[0])))
    ]


def rule_clauses(rows: list[list[Any]]) -> list[str]:
    """Clauses in a ``.rule def`` reply, in order. The engine answers with one
    message: ``Rule: <name>``, ``Clauses:``, then one ``  <i>. <clause>`` line each."""
    return [
        m.group(1)
        for row in rows
        if row
        for line in str(row[0]).split("\n")
        if (m := _NUMBERED.match(line))
    ]
