"""Field types, indexes, and query expression proxies for Duckling."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Generic, Optional, TypeVar, get_args, get_origin

from pydantic import Field as PydanticField


# ──────────────────────────────────────────────
# Sort direction
# ──────────────────────────────────────────────
class SortDirection(enum.IntEnum):
    ASCENDING = 1
    DESCENDING = -1


# ──────────────────────────────────────────────
# Index specification
# ──────────────────────────────────────────────
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

    # Store index metadata in Pydantic's json_schema_extra
    metadata = {
        "_duckling_indexed": True,
        "_duckling_unique": unique,
        "_duckling_index_type": index_type,
    }

    # Return an Annotated type with our metadata
    from typing import Annotated

    return Annotated[field_type, IndexSpec(unique=unique, index_type=index_type)]


# ──────────────────────────────────────────────
# Query Expressions
# ──────────────────────────────────────────────
class Expression:
    """Base class for SQL expressions used in query building."""

    def __and__(self, other: Expression) -> AndExpression:
        return AndExpression(self, other)

    def __or__(self, other: Expression) -> OrExpression:
        return OrExpression(self, other)

    def __invert__(self) -> NotExpression:
        return NotExpression(self)

    def to_sql(self) -> tuple[str, list]:
        """Return (sql_fragment, params) tuple."""
        raise NotImplementedError


class ComparisonExpression(Expression):
    """A comparison like `field_name > value`."""

    def __init__(self, field_name: str, op: str, value: Any) -> None:
        self.field_name = field_name
        self.op = op
        self.value = value

    def to_sql(self) -> tuple[str, list]:
        if self.value is None:
            if self.op == "=":
                return f'"{self.field_name}" IS NULL', []
            elif self.op in ("!=", "<>"):
                return f'"{self.field_name}" IS NOT NULL', []
        return f'"{self.field_name}" {self.op} ?', [self.value]

    def __repr__(self) -> str:
        return f"ComparisonExpression({self.field_name!r} {self.op} {self.value!r})"


class InExpression(Expression):
    """An `IN (...)` expression."""

    def __init__(self, field_name: str, values: list, negate: bool = False) -> None:
        self.field_name = field_name
        self.values = values
        self.negate = negate

    def to_sql(self) -> tuple[str, list]:
        placeholders = ", ".join("?" for _ in self.values)
        op = "NOT IN" if self.negate else "IN"
        return f'"{self.field_name}" {op} ({placeholders})', list(self.values)


class BetweenExpression(Expression):
    """A `BETWEEN` expression."""

    def __init__(self, field_name: str, low: Any, high: Any) -> None:
        self.field_name = field_name
        self.low = low
        self.high = high

    def to_sql(self) -> tuple[str, list]:
        return f'"{self.field_name}" BETWEEN ? AND ?', [self.low, self.high]


class LikeExpression(Expression):
    """A `LIKE` / `ILIKE` expression."""

    def __init__(self, field_name: str, pattern: str, case_insensitive: bool = False) -> None:
        self.field_name = field_name
        self.pattern = pattern
        self.case_insensitive = case_insensitive

    def to_sql(self) -> tuple[str, list]:
        op = "ILIKE" if self.case_insensitive else "LIKE"
        return f'"{self.field_name}" {op} ?', [self.pattern]


class RawExpression(Expression):
    """A raw SQL expression."""

    def __init__(self, sql: str, params: Optional[list] = None) -> None:
        self.sql = sql
        self.params = params or []

    def to_sql(self) -> tuple[str, list]:
        return self.sql, self.params


class AndExpression(Expression):
    def __init__(self, left: Expression, right: Expression) -> None:
        self.left = left
        self.right = right

    def to_sql(self) -> tuple[str, list]:
        left_sql, left_params = self.left.to_sql()
        right_sql, right_params = self.right.to_sql()
        return f"({left_sql} AND {right_sql})", left_params + right_params


class OrExpression(Expression):
    def __init__(self, left: Expression, right: Expression) -> None:
        self.left = left
        self.right = right

    def to_sql(self) -> tuple[str, list]:
        left_sql, left_params = self.left.to_sql()
        right_sql, right_params = self.right.to_sql()
        return f"({left_sql} OR {right_sql})", left_params + right_params


class NotExpression(Expression):
    def __init__(self, expr: Expression) -> None:
        self.expr = expr

    def to_sql(self) -> tuple[str, list]:
        sql, params = self.expr.to_sql()
        return f"NOT ({sql})", params


# ──────────────────────────────────────────────
# Field Proxy — enables `User.name == "Alice"`
# ──────────────────────────────────────────────
class FieldProxy:
    """
    A descriptor proxy returned when accessing a field on the Document *class*.
    Supports comparison operators that produce Expression objects for queries.
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
