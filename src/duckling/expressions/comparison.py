"""Comparison expressions: `=`, `!=`, `>`, `>=`, `<`, `<=`."""

from __future__ import annotations

from typing import Any

from .base import Expression


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
