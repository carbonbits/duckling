"""
Pins the public import paths across the per-class module split.

Classes moved into `duckling.expressions` and `duckling.aggregations`, but the
old paths are re-exported, so anything importing from `duckling.fields` or
`duckling.query` keeps working.
"""

import duckling


class TestTopLevelExports:
    def test_all_exports_resolve(self):
        missing = [name for name in duckling.__all__ if not hasattr(duckling, name)]

        assert missing == []


class TestLegacyImportPaths:
    def test_expressions_are_importable_from_fields(self):
        from duckling.expressions import Expression as FromExpressions
        from duckling.fields import Expression as FromFields

        assert FromFields is FromExpressions

    def test_aggregations_are_importable_from_query(self):
        from duckling.aggregations import Avg as FromAggregations
        from duckling.query import Avg as FromQuery

        assert FromQuery is FromAggregations

    def test_document_is_importable_from_its_package(self):
        from duckling.document import Document, DocumentMeta

        assert type(Document) is DocumentMeta

    def test_index_spec_is_importable_from_fields(self):
        from duckling.fields import IndexSpec

        assert IndexSpec is duckling.IndexSpec


class TestOneClassPerModule:
    def test_expression_classes_live_in_their_own_modules(self):
        from duckling.expressions.between import BetweenExpression
        from duckling.expressions.comparison import ComparisonExpression
        from duckling.expressions.conjunction import AndExpression
        from duckling.expressions.disjunction import OrExpression
        from duckling.expressions.inclusion import InExpression
        from duckling.expressions.like import LikeExpression
        from duckling.expressions.negation import NotExpression
        from duckling.expressions.raw import RawExpression

        assert {
            AndExpression,
            BetweenExpression,
            ComparisonExpression,
            InExpression,
            LikeExpression,
            NotExpression,
            OrExpression,
            RawExpression,
        } == set(
            getattr(duckling.expressions, name)
            for name in duckling.expressions.__all__
            if name != "Expression"
        )

    def test_aggregation_classes_live_in_their_own_modules(self):
        from duckling.aggregations.avg import Avg
        from duckling.aggregations.count import Count
        from duckling.aggregations.count_distinct import CountDistinct
        from duckling.aggregations.max import Max
        from duckling.aggregations.min import Min
        from duckling.aggregations.sum import Sum

        assert [c.func for c in (Avg, Count, CountDistinct, Max, Min, Sum)] == [
            "AVG",
            "COUNT",
            "COUNT",
            "MAX",
            "MIN",
            "SUM",
        ]
