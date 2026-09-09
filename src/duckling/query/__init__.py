"""
Query builder for Duckling — fluent, chainable queries inspired by Beanie.

Usage:
    # Chain methods to build queries
    users = await User.find(User.age > 25).sort("+name").limit(10).to_list()
    user  = await User.find_one(User.name == "Alice")
    count = await User.find(User.active == True).count()

    # Projection (select specific fields)
    names = await User.find().project(name=1, email=1).to_list()

    # Aggregation
    stats = await User.find().aggregate(avg_age=Avg("age"), total=Count())

Aggregation functions now live in `duckling.aggregations` and are re-exported
here so that `from duckling.query import Avg, Count, ...` keeps working.
"""

from ..aggregations import AggFunc, Avg, Count, CountDistinct, Max, Min, Sum
from .find_query import FindQuery
from .iterator import FindQueryIterator

__all__ = [
    "FindQuery",
    "FindQueryIterator",
    # Re-exported from duckling.aggregations
    "AggFunc",
    "Avg",
    "Count",
    "CountDistinct",
    "Max",
    "Min",
    "Sum",
]
