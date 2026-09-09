"""The AggFunc base class."""

from __future__ import annotations

from typing import Callable, Optional


class AggFunc:
    """
    Base aggregation function.

    Subclasses set `func` to the SQL function name. `to_sql` takes an optional
    `resolve` callable mapping a field name to its column name, so aggregations
    over aliased fields address the right column.
    """

    func: str = ""

    def __init__(self, field: str):
        self.field = field

    def _column(self, resolve: Optional[Callable[[str], str]]) -> str:
        return resolve(self.field) if resolve else self.field

    def to_sql(self, resolve: Optional[Callable[[str], str]] = None) -> str:
        if not self.func:
            raise NotImplementedError
        return f'{self.func}("{self._column(resolve)}")'
