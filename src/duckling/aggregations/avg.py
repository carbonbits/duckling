"""AVG aggregation."""

from duckling.aggregations.base import AggFunc


class Avg(AggFunc):
    """`AVG(field)`."""

    func = "AVG"
