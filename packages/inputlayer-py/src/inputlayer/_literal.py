"""The one place program text is built from values (R-LIT).

Every value the SDK writes into IQL (an inserted fact, a condition's
constant, a function argument, an integration's bound parameter) goes
through :func:`encode`. It produces exactly the literal the engine's parser
reads back as the same value, and refuses what IQL cannot express instead
of sending text the engine would misread:

- strings are quoted with the engine's five escapes (``\\\\``, ``\\"``,
  ``\\n``, ``\\t``, ``\\r``; the parser keeps any other backslash pair
  verbatim, so nothing else is escaped); a lone surrogate is refused;
- ``bool`` is ``true``/``false`` (checked before ``int``);
- ``int`` must fit the engine's 64-bit integers;
- ``float`` uses Python's shortest round-trip form; NaN and the infinities
  are refused, IQL has no literal for them;
- ``datetime`` is Unix milliseconds (R-TYPE; a naive datetime is UTC);
- a list or tuple of numbers is a vector;
- ``None`` is refused: IQL has no null.

A fuzz test (``tests/compile_rules_live.py``) round-trips random values
through a live engine: encode, insert, read back, compare.

Writes send their values out of band instead (protocol version 4): inside
:func:`collect_params`, :func:`encode` returns a ``$pN`` reference and
collects the value as a parameter, which the engine binds to the parsed
program without parsing it. The literal is still computed, so every
refusal above holds, and it names the value: equal literals share one
parameter, so compiled text compares as before.
"""

from __future__ import annotations

import math
import numbers
import re
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from typing import Any

from inputlayer.exceptions import CompileError

I64_MIN = -(2**63)
I64_MAX = 2**63 - 1

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MS = timedelta(milliseconds=1)

_ESCAPES = str.maketrans({"\\": "\\\\", '"': '\\"', "\n": "\\n", "\t": "\\t", "\r": "\\r"})


def encode(value: Any) -> str:
    """The IQL for *value*: its literal, or inside :func:`collect_params` a
    ``$pN`` reference to it. Raises :class:`CompileError` when IQL has no
    literal for it."""
    text = literal(value)
    sink = _ACTIVE.get()
    return text if sink is None else sink.reference(text, value)


class _ParamSink:
    """A program's parameter values, each distinct value once."""

    def __init__(self) -> None:
        self.params: dict[str, Any] = {}
        self._names: dict[str, str] = {}

    def reference(self, text: str, value: Any) -> str:
        name = self._names.get(text)
        if name is None:
            name = f"p{len(self._names)}"
            self.params[name] = wire_value(value)
            self._names[text] = name
        return f"${name}"


_ACTIVE: ContextVar[_ParamSink | None] = ContextVar("inputlayer_params", default=None)


@contextmanager
def collect_params() -> Iterator[dict[str, Any]]:
    """Compile with values out of band: within the block, :func:`encode`
    writes ``$pN`` references, and the yielded dict fills with their values,
    the request's ``params``::

        with collect_params() as params:
            iql = compile_insert(fact)
        await conn.execute(iql, params=params)
    """
    sink = _ParamSink()
    token = _ACTIVE.set(sink)
    try:
        yield sink.params
    finally:
        _ACTIVE.reset(token)


_REFERENCE = re.compile(r"\$(p[0-9]+)\b")


def params_of(iql: str, params: dict[str, Any]) -> dict[str, Any]:
    """The parameters of *params* that *iql*, one statement of a program
    compiled in one :func:`collect_params` block, references: for sending the
    statements as separate requests (the engine refuses an unused parameter)."""
    return {name: params[name] for name in dict.fromkeys(_REFERENCE.findall(iql)) if name in params}


def wire_value(value: Any) -> Any:
    """The ``params`` value for *value*, whose literal compiled: the type its
    literal denotes, exactly. A float JSON would read back as ambiguous (an
    integral value of 2^63 or more) takes the explicit ``{"float": x}`` form."""
    if isinstance(value, (str, bool)):
        return value
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        f = float(value)
        return {"float": f} if f.is_integer() and abs(f) >= 2.0**63 else f
    if isinstance(value, datetime):
        return datetime_to_ms(value)
    if isinstance(value, (list, tuple)):
        return [float(v) for v in value]
    raise CompileError(f"Cannot send a {type(value).__name__} value: {value!r}")


def literal(value: Any) -> str:
    """The IQL literal for *value*, never a parameter: for the places IQL takes
    no parameter (an aggregate's settings, ``limit``, meta commands)."""
    if isinstance(value, str):
        return encode_string(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, numbers.Integral):
        return _encode_int(int(value))
    if isinstance(value, numbers.Real):
        return _encode_float(float(value))
    if isinstance(value, datetime):
        return _encode_int(datetime_to_ms(value))
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_encode_vector_item(v) for v in value) + "]"
    if value is None:
        raise CompileError(
            "IQL has no null literal",
            hint="pass a value of the column's type, or leave the column out of the condition",
        )
    raise CompileError(
        f"Cannot write a {type(value).__name__} value into IQL: {value!r}",
        hint="use str, int, float, bool, datetime or a list of numbers",
    )


def encode_string(value: str) -> str:
    """The quoted IQL string literal for *value*."""
    return f'"{escape_string(value)}"'


def escape_string(value: str) -> str:
    """The body of the IQL string literal for *value* (without the quotes)."""
    if not isinstance(value, str):
        raise CompileError(f"Expected a str, got {type(value).__name__}: {value!r}")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as err:
        raise CompileError(
            f"String {value!r} holds a lone surrogate, which is not valid text",
            hint="decode the bytes it came from with errors='replace'",
        ) from err
    return value.translate(_ESCAPES)


def datetime_to_ms(value: datetime) -> int:
    """Unix milliseconds for *value*; a naive datetime is taken as UTC."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return (value - _EPOCH) // _MS


def ms_to_datetime(ms: int) -> datetime:
    """The UTC datetime *ms* Unix milliseconds stand for."""
    return _EPOCH + timedelta(milliseconds=ms)


def _encode_int(value: int) -> str:
    if not I64_MIN <= value <= I64_MAX:
        raise CompileError(
            f"Integer {value} does not fit the engine's 64-bit integers",
            hint=f"keep integers within [{I64_MIN}, {I64_MAX}], or store it as a string",
        )
    return str(value)


def _encode_float(value: float) -> str:
    if not math.isfinite(value):
        raise CompileError(
            f"IQL has no literal for {value!r}: it does not support infinity or NaN",
            hint="use a finite float, or leave the value out",
        )
    return repr(value)


def _encode_vector_item(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise CompileError(
            f"A vector holds numbers only, got {type(value).__name__}: {value!r}",
            hint="pass a list of floats",
        )
    return _encode_float(float(value))
