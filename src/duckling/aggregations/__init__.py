"""
Aggregation functions — one class per module.

Passed to `FindQuery.aggregate()`:

    stats = await User.find().aggregate(avg_age=Avg("age"), total=Count())
"""

from duckling.aggregations.avg import Avg
from duckling.aggregations.base import AggFunc
from duckling.aggregations.count import Count
from duckling.aggregations.count_distinct import CountDistinct
from duckling.aggregations.max import Max
from duckling.aggregations.min import Min
from duckling.aggregations.sum import Sum

__all__ = [
    "AggFunc",
    "Avg",
    "Count",
    "CountDistinct",
    "Max",
    "Min",
    "Sum",
]
