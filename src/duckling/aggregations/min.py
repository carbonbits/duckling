"""MIN aggregation."""

from duckling.aggregations.base import AggFunc


class Min(AggFunc):
    """`MIN(field)`."""

    func = "MIN"
