"""`BETWEEN` expressions."""

from __future__ import annotations

from typing import Any

from .base import Expression


class BetweenExpression(Expression):
    """A `BETWEEN` expression."""

    def __init__(self, field_name: str, low: Any, high: Any) -> None:
        self.field_name = field_name
        self.low = low
        self.high = high

    def to_sql(self) -> tuple[str, list]:
        return f'"{self.field_name}" BETWEEN ? AND ?', [self.low, self.high]
