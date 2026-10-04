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

import re
from typing import Any

from inputlayer._literal import encode as encode_literal

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
        - str  -> "..."  (the engine's escapes: backslash, quote, \\n, \\t, \\r)
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
    if isinstance(value, (str, bool, int, float, list, tuple)):
        # The SDK's one literal encoder (R-LIT); its CompileError is a ValueError.
        return encode_literal(value)
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
