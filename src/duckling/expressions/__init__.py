"""
SQL expression objects — one class per module.

Every expression renders itself to a `(sql_fragment, params)` pair and composes
with the others through `&`, `|` and `~`.
"""

from duckling.expressions.base import Expression
from duckling.expressions.between import BetweenExpression
from duckling.expressions.comparison import ComparisonExpression
from duckling.expressions.conjunction import AndExpression
from duckling.expressions.disjunction import OrExpression
from duckling.expressions.inclusion import InExpression
from duckling.expressions.like import LikeExpression
from duckling.expressions.negation import NotExpression
from duckling.expressions.raw import RawExpression

__all__ = [
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
