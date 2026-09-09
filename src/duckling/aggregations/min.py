"""MIN aggregation."""

from __future__ import annotations

from .base import AggFunc


class Min(AggFunc):
    """`MIN(field)`."""

    func = "MIN"
