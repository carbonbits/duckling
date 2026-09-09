"""COUNT DISTINCT aggregation."""

from __future__ import annotations

from typing import Callable, Optional

from .base import AggFunc


class CountDistinct(AggFunc):
    """`COUNT(DISTINCT field)`."""

    func = "COUNT"

    def to_sql(self, resolve: Optional[Callable[[str], str]] = None) -> str:
        return f'COUNT(DISTINCT "{self._column(resolve)}")'
