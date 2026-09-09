"""Index specification and the `Indexed()` annotation helper."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any


@dataclass
class IndexSpec:
    """Describes an index on a column."""

    unique: bool = False
    index_type: str = "default"  # default, hash, art


def Indexed(
    field_type: type = None,
    *,
    unique: bool = False,
    index_type: str = "default",
    **kwargs,
):
    """
    Mark a field as indexed, similar to Beanie's Indexed().

    Usage:
        class User(Document):
            email: Indexed(str, unique=True)
            age: Indexed(int)
    """
    if field_type is None:
        field_type = Any

    return Annotated[field_type, IndexSpec(unique=unique, index_type=index_type)]
