"""SUM aggregation."""

from __future__ import annotations

from .base import AggFunc


class Sum(AggFunc):
    """`SUM(field)`."""

    func = "SUM"
