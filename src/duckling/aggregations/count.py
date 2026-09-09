"""COUNT aggregation."""

from __future__ import annotations

from typing import Callable, Optional

from .base import AggFunc


class Count(AggFunc):
    """`COUNT(field)`, or `COUNT(*)` when no field is given."""

    func = "COUNT"

    def __init__(self, field: str = "*"):
        super().__init__(field)

    def to_sql(self, resolve: Optional[Callable[[str], str]] = None) -> str:
        if self.field == "*":
            return "COUNT(*)"
        return super().to_sql(resolve)
