"""
Duckling — A Beanie-inspired ORM for DuckDB.

    from duckling import Document, init_duckling, Indexed

    class User(Document):
        name: str
        email: Indexed(str, unique=True)
        age: int = 0

        class Settings:
            table_name = "users"

    await init_duckling(database=":memory:", document_models=[User])

    user = User(name="Alice", email="alice@example.com", age=30)
    await user.insert()

    users = await User.find(User.age > 25).sort("+name").limit(10).to_list()
"""

from duckling.aggregations import (
    AggFunc,
    Avg,
    Count,
    CountDistinct,
    Max,
    Min,
    Sum,
)
from duckling.connection import ConnectionFactory, DucklingSession, get_session
from duckling.document import Document
from duckling.exceptions import (
    CollectionNotFound,
    ConnectionError,
    DocumentAlreadyExists,
    DocumentNotFound,
    DucklingError,
    InvalidQueryError,
    NotInitializedError,
    ValidationError,
)
from duckling.expressions import Expression
from duckling.fields import (
    FieldProxy,
    Indexed,
    IndexSpec,
    SortDirection,
)
from duckling.ids import generate_ulid
from duckling.init import init_duckling, init_duckling_sync
from duckling.operators import (
    And,
    Between,
    Eq,
    Gt,
    Gte,
    ILike,
    In,
    IsNotNull,
    IsNull,
    Like,
    Lt,
    Lte,
    Ne,
    Not,
    NotIn,
    Or,
    Raw,
)
from duckling.query import FindQuery

__all__ = [
    # Core
    "Document",
    "init_duckling",
    "init_duckling_sync",
    # Session
    "ConnectionFactory",
    "DucklingSession",
    "get_session",
    # Fields
    "Indexed",
    "IndexSpec",
    "SortDirection",
    "FieldProxy",
    "Expression",
    # IDs
    "generate_ulid",
    # Query
    "FindQuery",
    # Operators
    "And",
    "Or",
    "Not",
    "In",
    "NotIn",
    "Between",
    "Like",
    "ILike",
    "Eq",
    "Ne",
    "Gt",
    "Gte",
    "Lt",
    "Lte",
    "IsNull",
    "IsNotNull",
    "Raw",
    # Aggregation
    "AggFunc",
    "Count",
    "CountDistinct",
    "Sum",
    "Avg",
    "Min",
    "Max",
    # Exceptions
    "DucklingError",
    "DocumentNotFound",
    "DocumentAlreadyExists",
    "NotInitializedError",
    "CollectionNotFound",
    "InvalidQueryError",
    "ValidationError",
    "ConnectionError",
]
