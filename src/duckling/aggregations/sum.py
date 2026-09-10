"""SUM aggregation."""

from duckling.aggregations.base import AggFunc


class Sum(AggFunc):
    """`SUM(field)`."""

    func = "SUM"
