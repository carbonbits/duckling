"""The Expression base class."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .conjunction import AndExpression
    from .disjunction import OrExpression
    from .negation import NotExpression


class Expression:
    """Base class for SQL expressions used in query building."""

    def __and__(self, other: Expression) -> AndExpression:
        from .conjunction import AndExpression

        return AndExpression(self, other)

    def __or__(self, other: Expression) -> OrExpression:
        from .disjunction import OrExpression

        return OrExpression(self, other)

    def __invert__(self) -> NotExpression:
        from .negation import NotExpression

        return NotExpression(self)

    def to_sql(self) -> tuple[str, list]:
        """Return (sql_fragment, params) tuple."""
        raise NotImplementedError
