"""`IN` / `NOT IN` expressions."""

from duckling.expressions.base import Expression


class InExpression(Expression):
    """An `IN (...)` expression."""

    def __init__(self, field_name: str, values: list, negate: bool = False) -> None:
        self.field_name = field_name
        self.values = values
        self.negate = negate

    def to_sql(self) -> tuple[str, list]:
        placeholders = ", ".join("?" for _ in self.values)
        op = "NOT IN" if self.negate else "IN"
        return f'"{self.field_name}" {op} ({placeholders})', list(self.values)
