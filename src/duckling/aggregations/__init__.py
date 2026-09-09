"""
Aggregation functions — one class per module.

Passed to `FindQuery.aggregate()`:

    stats = await User.find().aggregate(avg_age=Avg("age"), total=Count())
"""

from .avg import Avg
from .base import AggFunc
from .count import Count
from .count_distinct import CountDistinct
from .max import Max
from .min import Min
from .sum import Sum

__all__ = [
    "AggFunc",
    "Avg",
    "Count",
    "CountDistinct",
    "Max",
    "Min",
    "Sum",
]
