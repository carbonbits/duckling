"""`NOT` expressions."""

from duckling.expressions.base import Expression


class NotExpression(Expression):
    def __init__(self, expr: Expression) -> None:
        self.expr = expr

    def to_sql(self) -> tuple[str, list]:
        sql, params = self.expr.to_sql()
        return f"NOT ({sql})", params
