"""InputLayer type system - Python types that map to Datalog storage types."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, ClassVar

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema


class _VectorMeta(type):
    """Metaclass for Vector that supports Vector[N] syntax."""

    _dim: int | None = None
    _cache: ClassVar[dict[int, type]] = {}

    def __getitem__(cls, dim: int) -> type:
        if not isinstance(dim, int) or dim <= 0:
            raise TypeError(f"Vector dimension must be a positive integer, got {dim!r}")
        if dim not in cls._cache:
            ns = type.__new__(_VectorMeta, f"Vector[{dim}]", (cls,), {"_dim": dim})
            cls._cache[dim] = ns
        return cls._cache[dim]

    def __instancecheck__(cls, instance: object) -> bool:
        if not isinstance(instance, list):
            return False
        if cls._dim is not None and len(instance) != cls._dim:
            return False
        return all(isinstance(v, (int, float)) for v in instance)

    def __repr__(cls) -> str:
        if cls._dim is not None:
            return f"Vector[{cls._dim}]"
        return "Vector"


class Vector(list, metaclass=_VectorMeta):
    """Float32 vector type. Use Vector[N] for a fixed-dimensionality vector."""

    _dim: ClassVar[int | None] = None

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        dim = getattr(source_type, "_dim", None)

        def validate(v: Any) -> list:
            if isinstance(v, (list, tuple)):
                v = list(v)
            if not isinstance(v, list):
                raise ValueError(f"Expected list for Vector, got {type(v).__name__}")
            if dim is not None and len(v) != dim:
                raise ValueError(f"Expected vector of dimension {dim}, got {len(v)}")
            return [float(x) for x in v]

        return core_schema.no_info_plain_validator_function(
            validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: v, info_arg=False
            ),
        )


class _VectorInt8Meta(type):
    """Metaclass for VectorInt8 that supports VectorInt8[N] syntax."""

    _dim: int | None = None
    _cache: ClassVar[dict[int, type]] = {}

    def __getitem__(cls, dim: int) -> type:
        if not isinstance(dim, int) or dim <= 0:
            raise TypeError(
                f"VectorInt8 dimension must be a positive integer, got {dim!r}"
            )
        if dim not in cls._cache:
            ns = type.__new__(
                _VectorInt8Meta, f"VectorInt8[{dim}]", (cls,), {"_dim": dim}
            )
            cls._cache[dim] = ns
        return cls._cache[dim]

    def __instancecheck__(cls, instance: object) -> bool:
        if not isinstance(instance, list):
            return False
        if cls._dim is not None and len(instance) != cls._dim:
            return False
        return all(isinstance(v, int) and -128 <= v <= 127 for v in instance)

    def __repr__(cls) -> str:
        if cls._dim is not None:
            return f"VectorInt8[{cls._dim}]"
        return "VectorInt8"


class VectorInt8(list, metaclass=_VectorInt8Meta):
    """Int8 quantized vector type. Use VectorInt8[N] for fixed dimensionality."""

    _dim: ClassVar[int | None] = None

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        dim = getattr(source_type, "_dim", None)

        def validate(v: Any) -> list:
            if isinstance(v, (list, tuple)):
                v = list(v)
            if not isinstance(v, list):
                raise ValueError(f"Expected list for VectorInt8, got {type(v).__name__}")
            if dim is not None and len(v) != dim:
                raise ValueError(f"Expected vector of dimension {dim}, got {len(v)}")
            for x in v:
                if not isinstance(x, int) or not (-128 <= x <= 127):
                    raise ValueError(f"VectorInt8 values must be int in [-128, 127], got {x}")
            return [int(x) for x in v]

        return core_schema.no_info_plain_validator_function(
            validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: v, info_arg=False
            ),
        )


class Timestamp(int):
    """Timestamp as Unix milliseconds since epoch."""

    @classmethod
    def now(cls) -> Timestamp:
        """Current time as a Timestamp."""
        return cls(int(time.time() * 1000))

    @classmethod
    def from_datetime(cls, dt: datetime) -> Timestamp:
        """Convert a datetime to a Timestamp (Unix ms)."""
        return cls(int(dt.timestamp() * 1000))

    def to_datetime(self) -> datetime:
        """Convert to a timezone-aware UTC datetime."""
        return datetime.fromtimestamp(int(self) / 1000.0, tz=timezone.utc)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            lambda v: Timestamp(int(v)),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: int(v), info_arg=False
            ),
        )


# Map Python types to InputLayer Datalog type names
# Order matters: more specific types first (bool before int, Timestamp before int)
TYPE_MAP: dict[type, str] = {
    bool: "bool",
    Timestamp: "timestamp",
    int: "int",
    float: "float",
    str: "string",
    Vector: "vector",
    VectorInt8: "vector_int8",
}


def python_type_to_datalog(tp: type) -> str:
    """Convert a Python type annotation to its InputLayer Datalog type string.

    Handles Vector[N], VectorInt8[N], and plain types.
    """
    # Check for dimensioned vectors
    if isinstance(tp, _VectorMeta) and tp._dim is not None:
        return f"vector[{tp._dim}]"
    if isinstance(tp, _VectorInt8Meta) and tp._dim is not None:
        return f"vector_int8[{tp._dim}]"
    # Check base types
    for base_type, name in TYPE_MAP.items():
        if tp is base_type:
            return name
        # Handle subclasses (e.g., Vector without dim)
        try:
            if issubclass(tp, base_type) and tp is not base_type:
                return name
        except TypeError:
            pass
    raise TypeError(f"Unsupported type for InputLayer schema: {tp!r}")
