"""`AND` expressions."""

from duckling.expressions.base import Expression


class AndExpression(Expression):
    def __init__(self, left: Expression, right: Expression) -> None:
        self.left = left
        self.right = right

    def to_sql(self) -> tuple[str, list]:
        left_sql, left_params = self.left.to_sql()
        right_sql, right_params = self.right.to_sql()
        return f"({left_sql} AND {right_sql})", left_params + right_params
