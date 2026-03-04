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

__version__ = "0.1.0"

from .connection import DucklingSession, get_session
from .document import Document
from .exceptions import (
    CollectionNotFound,
    ConnectionError,
    DocumentAlreadyExists,
    DocumentNotFound,
    DucklingError,
    InvalidQueryError,
    NotInitializedError,
    ValidationError,
)
from .fields import (
    Expression,
    FieldProxy,
    Indexed,
    IndexSpec,
    SortDirection,
)
from .init import init_duckling, init_duckling_sync
from .operators import (
    And,
    Avg,
    Between,
    Count,
    CountDistinct,
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
    Max,
    Min,
    Ne,
    Not,
    NotIn,
    Or,
    Raw,
    Sum,
)
from .query import FindQuery

__all__ = [
    # Core
    "Document",
    "init_duckling",
    "init_duckling_sync",
    # Session
    "DucklingSession",
    "get_session",
    # Fields
    "Indexed",
    "IndexSpec",
    "SortDirection",
    "FieldProxy",
    "Expression",
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
