"""`LIKE` / `ILIKE` expressions."""

from duckling.expressions.base import Expression


class LikeExpression(Expression):
    """A `LIKE` / `ILIKE` expression."""

    def __init__(self, field_name: str, pattern: str, case_insensitive: bool = False) -> None:
        self.field_name = field_name
        self.pattern = pattern
        self.case_insensitive = case_insensitive

    def to_sql(self) -> tuple[str, list]:
        op = "ILIKE" if self.case_insensitive else "LIKE"
        return f'"{self.field_name}" {op} ?', [self.pattern]
