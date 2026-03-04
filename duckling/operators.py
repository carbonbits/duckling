"""
Query operators for Duckling — providing both Beanie-style and SQL-friendly syntax.

Usage:
    from duckling.operators import In, Between, Like, And, Or, Not, Raw

    # Beanie-style operator usage
    await User.find(In(User.age, [25, 30, 35])).to_list()
    await User.find(Between(User.age, 18, 65)).to_list()
    await User.find(Like(User.name, "A%")).to_list()

    # Combine with boolean operators
    await User.find(
        And(
            User.age >= 18,
            Or(User.city == "NYC", User.city == "LA")
        )
    ).to_list()
"""

from __future__ import annotations

from typing import Any

from .fields import (
    AndExpression,
    BetweenExpression,
    Expression,
    InExpression,
    LikeExpression,
    NotExpression,
    OrExpression,
    RawExpression,
    ComparisonExpression,
    FieldProxy,
)


# ──────────────────────────────────────────────
# Functional operator constructors
# ──────────────────────────────────────────────

def In(field: FieldProxy | str, values: list) -> InExpression:
    """Create an IN expression: `field IN (v1, v2, ...)`."""
    name = field.field_name if isinstance(field, FieldProxy) else field
    return InExpression(name, values)


def NotIn(field: FieldProxy | str, values: list) -> InExpression:
    """Create a NOT IN expression: `field NOT IN (v1, v2, ...)`."""
    name = field.field_name if isinstance(field, FieldProxy) else field
    return InExpression(name, values, negate=True)


def Between(field: FieldProxy | str, low: Any, high: Any) -> BetweenExpression:
    """Create a BETWEEN expression: `field BETWEEN low AND high`."""
    name = field.field_name if isinstance(field, FieldProxy) else field
    return BetweenExpression(name, low, high)


def Like(field: FieldProxy | str, pattern: str) -> LikeExpression:
    """Create a LIKE expression: `field LIKE pattern`."""
    name = field.field_name if isinstance(field, FieldProxy) else field
    return LikeExpression(name, pattern)


def ILike(field: FieldProxy | str, pattern: str) -> LikeExpression:
    """Create an ILIKE expression: `field ILIKE pattern` (case-insensitive)."""
    name = field.field_name if isinstance(field, FieldProxy) else field
    return LikeExpression(name, pattern, case_insensitive=True)


def And(*expressions: Expression) -> Expression:
    """Combine expressions with AND."""
    if not expressions:
        raise ValueError("And() requires at least one expression")
    result = expressions[0]
    for expr in expressions[1:]:
        result = AndExpression(result, expr)
    return result


def Or(*expressions: Expression) -> Expression:
    """Combine expressions with OR."""
    if not expressions:
        raise ValueError("Or() requires at least one expression")
    result = expressions[0]
    for expr in expressions[1:]:
        result = OrExpression(result, expr)
    return result


def Not(expression: Expression) -> NotExpression:
    """Negate an expression with NOT."""
    return NotExpression(expression)


def Raw(sql: str, params: list | None = None) -> RawExpression:
    """Create a raw SQL expression."""
    return RawExpression(sql, params)


def Eq(field: FieldProxy | str, value: Any) -> ComparisonExpression:
    """Equality: `field = value`."""
    name = field.field_name if isinstance(field, FieldProxy) else field
    return ComparisonExpression(name, "=", value)


def Ne(field: FieldProxy | str, value: Any) -> ComparisonExpression:
    """Not equal: `field != value`."""
    name = field.field_name if isinstance(field, FieldProxy) else field
    return ComparisonExpression(name, "!=", value)


def Gt(field: FieldProxy | str, value: Any) -> ComparisonExpression:
    """Greater than: `field > value`."""
    name = field.field_name if isinstance(field, FieldProxy) else field
    return ComparisonExpression(name, ">", value)


def Gte(field: FieldProxy | str, value: Any) -> ComparisonExpression:
    """Greater than or equal: `field >= value`."""
    name = field.field_name if isinstance(field, FieldProxy) else field
    return ComparisonExpression(name, ">=", value)


def Lt(field: FieldProxy | str, value: Any) -> ComparisonExpression:
    """Less than: `field < value`."""
    name = field.field_name if isinstance(field, FieldProxy) else field
    return ComparisonExpression(name, "<", value)


def Lte(field: FieldProxy | str, value: Any) -> ComparisonExpression:
    """Less than or equal: `field <= value`."""
    name = field.field_name if isinstance(field, FieldProxy) else field
    return ComparisonExpression(name, "<=", value)


def IsNull(field: FieldProxy | str) -> ComparisonExpression:
    """IS NULL check."""
    name = field.field_name if isinstance(field, FieldProxy) else field
    return ComparisonExpression(name, "=", None)


def IsNotNull(field: FieldProxy | str) -> ComparisonExpression:
    """IS NOT NULL check."""
    name = field.field_name if isinstance(field, FieldProxy) else field
    return ComparisonExpression(name, "!=", None)
