"""MAX aggregation."""

from __future__ import annotations

from .base import AggFunc


class Max(AggFunc):
    """`MAX(field)`."""

    func = "MAX"
