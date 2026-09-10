"""COUNT DISTINCT aggregation."""

from typing import Callable, Optional

from duckling.aggregations.base import AggFunc


class CountDistinct(AggFunc):
    """`COUNT(DISTINCT field)`."""

    func = "COUNT"

    def to_sql(self, resolve: Optional[Callable[[str], str]] = None) -> str:
        return f'COUNT(DISTINCT "{self._column(resolve)}")'
