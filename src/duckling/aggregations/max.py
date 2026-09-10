"""MAX aggregation."""

from duckling.aggregations.base import AggFunc


class Max(AggFunc):
    """`MAX(field)`."""

    func = "MAX"
