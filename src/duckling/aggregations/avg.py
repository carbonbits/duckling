"""AVG aggregation."""

from __future__ import annotations

from .base import AggFunc


class Avg(AggFunc):
    """`AVG(field)`."""

    func = "AVG"
