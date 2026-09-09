"""FieldProxy — enables `User.name == "Alice"` style query expressions."""

from __future__ import annotations

from typing import Any

from ..expressions import (
    BetweenExpression,
    ComparisonExpression,
    InExpression,
    LikeExpression,
)
from .sort import SortDirection


class FieldProxy:
    """
    A descriptor proxy returned when accessing a field on the Document *class*.
    Supports comparison operators that produce Expression objects for queries.

    The proxy is built with the field's *column* name, so every expression it
    produces addresses the right identifier even when the two differ.
    """

    def __init__(self, field_name: str, field_type: type = Any) -> None:
        self.field_name = field_name
        self.field_type = field_type

    # Comparison operators → Expression
    def __eq__(self, other: Any) -> ComparisonExpression:  # type: ignore[override]
        return ComparisonExpression(self.field_name, "=", other)

    def __ne__(self, other: Any) -> ComparisonExpression:  # type: ignore[override]
        return ComparisonExpression(self.field_name, "!=", other)

    def __gt__(self, other: Any) -> ComparisonExpression:
        return ComparisonExpression(self.field_name, ">", other)

    def __ge__(self, other: Any) -> ComparisonExpression:
        return ComparisonExpression(self.field_name, ">=", other)

    def __lt__(self, other: Any) -> ComparisonExpression:
        return ComparisonExpression(self.field_name, "<", other)

    def __le__(self, other: Any) -> ComparisonExpression:
        return ComparisonExpression(self.field_name, "<=", other)

    # Extra query helpers
    def is_in(self, values: list) -> InExpression:
        """Field IN (values...)"""
        return InExpression(self.field_name, values)

    def not_in(self, values: list) -> InExpression:
        """Field NOT IN (values...)"""
        return InExpression(self.field_name, values, negate=True)

    def between(self, low: Any, high: Any) -> BetweenExpression:
        """Field BETWEEN low AND high"""
        return BetweenExpression(self.field_name, low, high)

    def like(self, pattern: str) -> LikeExpression:
        """Field LIKE pattern"""
        return LikeExpression(self.field_name, pattern)

    def ilike(self, pattern: str) -> LikeExpression:
        """Field ILIKE pattern (case-insensitive)"""
        return LikeExpression(self.field_name, pattern, case_insensitive=True)

    def startswith(self, prefix: str) -> LikeExpression:
        return LikeExpression(self.field_name, f"{prefix}%")

    def endswith(self, suffix: str) -> LikeExpression:
        return LikeExpression(self.field_name, f"%{suffix}")

    def contains(self, substring: str) -> LikeExpression:
        return LikeExpression(self.field_name, f"%{substring}%")

    # Sort helpers
    def asc(self) -> tuple[str, SortDirection]:
        return (self.field_name, SortDirection.ASCENDING)

    def desc(self) -> tuple[str, SortDirection]:
        return (self.field_name, SortDirection.DESCENDING)

    def __pos__(self) -> tuple[str, SortDirection]:
        """Unary + for ascending sort: +User.name"""
        return self.asc()

    def __neg__(self) -> tuple[str, SortDirection]:
        """Unary - for descending sort: -User.name"""
        return self.desc()

    def __repr__(self) -> str:
        return f"FieldProxy({self.field_name!r})"
