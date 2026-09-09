"""Conversion between Python/Pydantic types and DuckDB columns and values."""

from __future__ import annotations

import datetime
import enum
import json
import typing
import uuid
from typing import Any, Dict, List, get_args, get_origin

from pydantic import BaseModel

# Python → DuckDB type mapping
_TYPE_MAP: dict[type, str] = {
    int: "BIGINT",
    float: "DOUBLE",
    str: "VARCHAR",
    bool: "BOOLEAN",
    bytes: "BLOB",
    datetime.date: "DATE",
    datetime.datetime: "TIMESTAMP",
    datetime.time: "TIME",
    uuid.UUID: "UUID",
}


def python_type_to_duckdb(py_type: Any) -> str:
    """Convert a Python / Pydantic type annotation to a DuckDB column type."""
    # Handle Optional[X]
    origin = get_origin(py_type)
    if origin is type(None):
        return "VARCHAR"

    # Optional[X] shows up as Union[X, None]
    args = get_args(py_type)
    if args:
        # typing.Annotated — the first arg is the actual type
        if origin is getattr(typing, "Annotated", None):
            return python_type_to_duckdb(args[0])

        # Union types (Optional)
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            return python_type_to_duckdb(non_none[0])

    # List/dict → JSON-like storage
    if origin in (list, List, dict, Dict):
        return "JSON"

    # Direct lookup
    if py_type in _TYPE_MAP:
        return _TYPE_MAP[py_type]

    # Enum
    if isinstance(py_type, type) and issubclass(py_type, enum.Enum):
        return "VARCHAR"

    # Nested Pydantic model → JSON
    if isinstance(py_type, type) and issubclass(py_type, BaseModel):
        return "JSON"

    return "VARCHAR"


def python_value_to_duckdb(value: Any) -> Any:
    """Convert a Python value for DuckDB insertion."""
    if value is None:
        return None
    if isinstance(value, (BaseModel,)):
        return value.model_dump_json()
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, enum.Enum):
        return value.value
    return value


def duckdb_value_to_python(value: Any, py_type: Any) -> Any:
    """Convert a DuckDB value back to the expected Python type."""
    if value is None:
        return None

    origin = get_origin(py_type)
    args = get_args(py_type)

    # Handle Annotated
    if origin is getattr(typing, "Annotated", None) and args:
        py_type = args[0]
        origin = get_origin(py_type)
        args = get_args(py_type)

    # Handle Optional
    if args:
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            py_type = non_none[0]
            origin = get_origin(py_type)
            args = get_args(py_type)

    # Nested Pydantic model
    if isinstance(py_type, type) and issubclass(py_type, BaseModel):
        if isinstance(value, str):
            return py_type.model_validate_json(value)
        if isinstance(value, dict):
            return py_type.model_validate(value)

    # List / Dict from JSON
    if origin in (list, List, dict, Dict):
        if isinstance(value, str):
            return json.loads(value)
        return value

    # UUID
    if py_type is uuid.UUID:
        if isinstance(value, str):
            return uuid.UUID(value)
        return value

    # Enum
    if isinstance(py_type, type) and issubclass(py_type, enum.Enum):
        return py_type(value)

    return value
