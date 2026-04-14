"""Safe parameter binding for InputLayer Query Language (IQL) queries.

The integration accepts queries with named ``:param`` placeholders
and a ``params`` dict, e.g.::

    bind_params(
        "?docs(T, C), search(:q, T, C), score(C) > :min",
        {"q": "machine learning", "min": 0.5},
    )

becomes::

    '?docs(T, C), search("machine learning", T, C), score(C) > 0.5'

Strings are quoted and escaped, so user input is never interpolated
as raw IQL. This is the IQL equivalent of parameterized SQL.

Two regions are protected from substitution:

- string literals (``"..."``)
- line comments (``// ...`` to end of line)

In both cases the original text is preserved verbatim.
"""

from __future__ import annotations

import math
import re
from typing import Any

# A placeholder is ``:`` followed by an identifier.
_PLACEHOLDER_RE = re.compile(r":([A-Za-z_][A-Za-z_0-9]*)")
# Match either a string literal or a // line comment to end of line.
# Whichever appears first in the unprotected text is the next "skip" span.
_PROTECTED_RE = re.compile(
    r'"(?:[^"\\]|\\.)*"'  # double-quoted string with backslash escapes
    r"|"
    r"//[^\n]*"  # // line comment
)


def iql_literal(value: Any) -> str:
    """Render a Python value as an IQL literal.

    Supported:
        - str  -> "..."  (quotes + backslashes escaped)
        - bool -> true / false
        - int / float -> bare number
        - list / tuple of numbers -> [1.0, 2.0, 3.0]
        - None -> raises (IQL has no NULL placeholder)
    """
    if value is None:
        raise ValueError(
            "Cannot bind None as an IQL literal - omit the parameter "
            "or use a sentinel value of the appropriate type."
        )
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            raise ValueError(
                f"Cannot bind {value!r} as an IQL literal - "
                "IQL does not support infinity or NaN."
            )
        return repr(value)
    if isinstance(value, str):
        escaped = (
            value
            .replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
            .replace("\x00", "\\0")
        )
        return f'"{escaped}"'
    if isinstance(value, (list, tuple)):
        if not all(
            isinstance(v, (int, float)) and not isinstance(v, bool) for v in value
        ):
            raise ValueError(
                f"List parameters must contain only numbers, got {value!r}"
            )
        for v in value:
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                raise ValueError(
                    f"Cannot bind {v!r} in list as an IQL literal - "
                    "IQL does not support infinity or NaN."
                )
        return "[" + ", ".join(repr(float(v)) for v in value) + "]"
    raise TypeError(
        f"Cannot bind {type(value).__name__} as an IQL literal: {value!r}"
    )


def bind_params(query: str, params: dict[str, Any] | None) -> str:
    """Substitute ``:name`` placeholders in ``query`` with values from ``params``.

    Placeholders inside string literals or ``//`` line comments are left
    untouched. Unknown placeholders raise ``KeyError``; unused params are
    ignored (so the same param dict can be reused across queries).

    ``params=None`` and ``params={}`` are both accepted; either way, if
    the query contains a placeholder outside of string literals or
    comments, that's a programming error and raises ``KeyError``.
    """
    if params is None:
        params = {}

    out: list[str] = []
    i = 0
    repl = _make_repl(params)
    for m in _PROTECTED_RE.finditer(query):
        # Substitute in the unprotected gap before this protected region.
        out.append(_PLACEHOLDER_RE.sub(repl, query[i : m.start()]))
        out.append(m.group(0))
        i = m.end()
    out.append(_PLACEHOLDER_RE.sub(repl, query[i:]))
    return "".join(out)


def _make_repl(params: dict[str, Any]):  # type: ignore[no-untyped-def]
    def repl(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in params:
            raise KeyError(f"Missing query parameter: :{name}")
        return iql_literal(params[name])

    return repl
