"""
Field types, indexes, and query expression proxies for Duckling.

Expression classes now live in `duckling.expressions` and are re-exported here
so that `from duckling.fields import Expression, ...` keeps working.
"""

from duckling.expressions import (
    AndExpression,
    BetweenExpression,
    ComparisonExpression,
    Expression,
    InExpression,
    LikeExpression,
    NotExpression,
    OrExpression,
    RawExpression,
)
from duckling.fields.index_spec import Indexed, IndexSpec
from duckling.fields.proxy import FieldProxy
from duckling.fields.sort import SortDirection

__all__ = [
    "FieldProxy",
    "Indexed",
    "IndexSpec",
    "SortDirection",
    # Re-exported from duckling.expressions
    "Expression",
    "AndExpression",
    "BetweenExpression",
    "ComparisonExpression",
    "InExpression",
    "LikeExpression",
    "NotExpression",
    "OrExpression",
    "RawExpression",
]
