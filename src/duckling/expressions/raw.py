"""Raw SQL expressions — the escape hatch."""

from __future__ import annotations

from typing import Optional

from .base import Expression


class RawExpression(Expression):
    """A raw SQL expression."""

    def __init__(self, sql: str, params: Optional[list] = None) -> None:
        self.sql = sql
        self.params = params or []

    def to_sql(self) -> tuple[str, list]:
        return self.sql, self.params
