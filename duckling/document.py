"""
Document base class for Duckling — the core of the ORM.

Inherits from Pydantic BaseModel and adds DuckDB persistence, just like
Beanie's Document wraps MongoDB documents.

Usage:
    from duckling import Document, Indexed

    class User(Document):
        name: str
        email: Indexed(str, unique=True)
        age: int = 0

        class Settings:
            table_name = "users"

    # CRUD
    user = User(name="Alice", email="alice@example.com", age=30)
    await user.insert()
    await user.save()       # upsert
    await user.delete()

    # Queries
    users = await User.find(User.age > 25).to_list()
    user  = await User.find_one(User.email == "alice@example.com")
    count = await User.find_all().count()
"""

from __future__ import annotations

import asyncio
import datetime
import uuid
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import BaseModel, ConfigDict

from .connection import DucklingSession, get_session
from .exceptions import DocumentNotFound, InvalidQueryError
from .fields import Expression, FieldProxy, IndexSpec, SortDirection
from .query import FindQuery

T = TypeVar("T", bound="Document")

# Python → DuckDB type mapping
_TYPE_MAP: dict[type, str] = {
    int: "BIGINT",
    float: "DOUBLE",
    str: "VARCHAR",
    bool: "BOOLEAN",
    bytes: "BLOB",
    datetime.date: "DATE",
    datetime.datetime: "TIMESTAMP",
    datetime.time: "TIME",
    uuid.UUID: "UUID",
}


def _python_type_to_duckdb(py_type: Any) -> str:
    """Convert a Python / Pydantic type annotation to a DuckDB column type."""
    # Handle Optional[X]
    origin = get_origin(py_type)
    if origin is type(None):
        return "VARCHAR"

    # Optional[X] shows up as Union[X, None]
    args = get_args(py_type)
    if args:
        # typing.Annotated — check for IndexSpec
        import typing
        if origin is getattr(typing, "Annotated", None):
            # First arg is the actual type
            return _python_type_to_duckdb(args[0])

        # Union types (Optional)
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            return _python_type_to_duckdb(non_none[0])

    # List/dict → JSON-like storage
    if origin in (list, List, dict, Dict):
        return "JSON"

    # Direct lookup
    if py_type in _TYPE_MAP:
        return _TYPE_MAP[py_type]

    # Enum
    import enum
    if isinstance(py_type, type) and issubclass(py_type, enum.Enum):
        return "VARCHAR"

    # Nested Pydantic model → JSON
    if isinstance(py_type, type) and issubclass(py_type, BaseModel):
        return "JSON"

    return "VARCHAR"


def _python_value_to_duckdb(value: Any) -> Any:
    """Convert a Python value for DuckDB insertion."""
    if value is None:
        return None
    if isinstance(value, (BaseModel,)):
        return value.model_dump_json()
    if isinstance(value, (dict, list)):
        import json
        return json.dumps(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    import enum
    if isinstance(value, enum.Enum):
        return value.value
    return value


def _duckdb_value_to_python(value: Any, py_type: Any) -> Any:
    """Convert a DuckDB value back to the expected Python type."""
    if value is None:
        return None

    origin = get_origin(py_type)
    args = get_args(py_type)

    # Handle Annotated
    import typing
    if origin is getattr(typing, "Annotated", None) and args:
        py_type = args[0]
        origin = get_origin(py_type)
        args = get_args(py_type)

    # Handle Optional
    if args:
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            py_type = non_none[0]
            origin = get_origin(py_type)
            args = get_args(py_type)

    # Nested Pydantic model
    if isinstance(py_type, type) and issubclass(py_type, BaseModel):
        import json
        if isinstance(value, str):
            return py_type.model_validate_json(value)
        if isinstance(value, dict):
            return py_type.model_validate(value)

    # List / Dict from JSON
    if origin in (list, List, dict, Dict):
        import json
        if isinstance(value, str):
            return json.loads(value)
        return value

    # UUID
    if py_type is uuid.UUID:
        if isinstance(value, str):
            return uuid.UUID(value)
        return value

    # Enum
    import enum
    if isinstance(py_type, type) and issubclass(py_type, enum.Enum):
        return py_type(value)

    return value


class DocumentMeta(type(BaseModel)):
    """
    Metaclass for Document that installs FieldProxy descriptors on the class,
    enabling `User.name == "Alice"` style query expressions.
    """

    def __new__(mcs, name: str, bases: tuple, namespace: dict, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        # Skip the base Document class itself
        if name == "Document" and not any(
            hasattr(b, "_is_duckling_document") for b in bases
        ):
            cls._is_duckling_document = True
            return cls

        # For every model field, create a FieldProxy accessible on the class
        cls._field_proxies = {}
        for field_name, field_info in cls.model_fields.items():
            proxy = FieldProxy(field_name, field_info.annotation)
            cls._field_proxies[field_name] = proxy

        return cls

    def __getattr__(cls, name: str):
        # Return FieldProxy for query building when accessing fields on the class
        if name.startswith("_") or name == "model_fields":
            raise AttributeError(name)
        proxies = cls.__dict__.get("_field_proxies", {})
        if name in proxies:
            return proxies[name]
        raise AttributeError(
            f"type object {cls.__name__!r} has no attribute {name!r}"
        )


class Document(BaseModel, metaclass=DocumentMeta):
    """
    Base document class for Duckling ORM.

    Subclass this and define your fields using standard Pydantic syntax.
    Use the inner `Settings` class for table configuration.

    Example:
        class Product(Document):
            name: str
            price: float
            in_stock: bool = True

            class Settings:
                table_name = "products"
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
    )

    # Auto-generated primary key
    id: Optional[int] = None

    # ── Inner Settings class ──────────────────

    class Settings:
        table_name: Optional[str] = None
        indexes: list = []

    # ── Table name resolution ─────────────────

    @classmethod
    def _get_table_name(cls) -> str:
        if hasattr(cls, "Settings") and hasattr(cls.Settings, "table_name") and cls.Settings.table_name:
            return cls.Settings.table_name
        # Auto-generate from class name: UserProfile → user_profile
        import re
        name = cls.__name__
        return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()

    @classmethod
    def _get_column_names(cls) -> list[str]:
        return list(cls.model_fields.keys())

    @classmethod
    def _get_column_types(cls) -> dict[str, str]:
        """Map field names to DuckDB column types."""
        import typing
        hints = get_type_hints(cls, include_extras=True)
        result = {}
        for name in cls.model_fields:
            py_type = hints.get(name, str)
            result[name] = _python_type_to_duckdb(py_type)
        return result

    @classmethod
    def _get_indexed_fields(cls) -> list[tuple[str, IndexSpec]]:
        """Return fields that have Indexed() annotations."""
        import typing
        hints = get_type_hints(cls, include_extras=True)
        indexed = []
        for name in cls.model_fields:
            py_type = hints.get(name)
            if py_type and get_origin(py_type) is getattr(typing, "Annotated", None):
                args = get_args(py_type)
                for arg in args[1:]:
                    if isinstance(arg, IndexSpec):
                        indexed.append((name, arg))
        return indexed

    # ── Table creation ────────────────────────

    @classmethod
    def _build_create_table_sql(cls) -> str:
        """Generate CREATE TABLE IF NOT EXISTS SQL."""
        table = cls._get_table_name()
        col_types = cls._get_column_types()
        indexed = dict(cls._get_indexed_fields())

        columns = []
        for col_name, col_type in col_types.items():
            parts = [f'"{col_name}"', col_type]
            if col_name == "id":
                parts = ['"id"', "INTEGER PRIMARY KEY DEFAULT(nextval('seq_" + table + "_id'))"]
                continue  # handled separately
            if col_name in indexed and indexed[col_name].unique:
                parts.append("UNIQUE")
            columns.append(" ".join(parts))

        # id column first
        col_defs = [f'"id" INTEGER PRIMARY KEY DEFAULT(nextval(\'seq_{table}_id\'))']
        col_defs.extend(columns)

        return f'CREATE TABLE IF NOT EXISTS "{table}" (\n  ' + ",\n  ".join(col_defs) + "\n)"

    @classmethod
    def _build_sequence_sql(cls) -> str:
        table = cls._get_table_name()
        return f"CREATE SEQUENCE IF NOT EXISTS seq_{table}_id START 1"

    @classmethod
    def _create_table_sync(cls) -> None:
        """Create the table synchronously."""
        session = get_session()
        session.execute(cls._build_sequence_sql())
        session.execute(cls._build_create_table_sql())

        # Create indexes
        table = cls._get_table_name()
        for field_name, spec in cls._get_indexed_fields():
            idx_name = f"idx_{table}_{field_name}"
            unique = "UNIQUE " if spec.unique else ""
            try:
                session.execute(
                    f'CREATE {unique}INDEX IF NOT EXISTS "{idx_name}" ON "{table}" ("{field_name}")'
                )
            except Exception:
                pass  # Index may already exist

    @classmethod
    async def _create_table(cls) -> None:
        """Create the table asynchronously."""
        await asyncio.to_thread(cls._create_table_sync)

    # ── Row serialization ─────────────────────

    def _to_row_dict(self) -> dict[str, Any]:
        """Convert this document to a dict of column → value for DuckDB."""
        data = {}
        for name in self.model_fields:
            val = getattr(self, name)
            data[name] = _python_value_to_duckdb(val)
        return data

    @classmethod
    def _from_row(cls: Type[T], row: tuple, columns: list[str]) -> T:
        """Create a document instance from a database row."""
        import typing
        hints = get_type_hints(cls, include_extras=True)
        data = {}
        for col_name, value in zip(columns, row):
            py_type = hints.get(col_name, str)
            data[col_name] = _duckdb_value_to_python(value, py_type)
        return cls.model_validate(data)

    # ── CRUD: Insert ──────────────────────────

    async def insert(self: T) -> T:
        """Insert this document into the database."""
        session = get_session()
        table = self._get_table_name()
        data = self._to_row_dict()

        # Remove id if None (auto-generated)
        if data.get("id") is None:
            data.pop("id", None)

        columns = list(data.keys())
        placeholders = ", ".join("?" for _ in columns)
        col_str = ", ".join(f'"{c}"' for c in columns)
        values = [data[c] for c in columns]

        sql = f'INSERT INTO "{table}" ({col_str}) VALUES ({placeholders}) RETURNING "id"'

        row = await session.async_fetchone(sql, values)
        if row:
            self.id = row[0]
        return self

    def insert_sync(self: T) -> T:
        """Insert this document synchronously."""
        session = get_session()
        table = self._get_table_name()
        data = self._to_row_dict()

        if data.get("id") is None:
            data.pop("id", None)

        columns = list(data.keys())
        placeholders = ", ".join("?" for _ in columns)
        col_str = ", ".join(f'"{c}"' for c in columns)
        values = [data[c] for c in columns]

        sql = f'INSERT INTO "{table}" ({col_str}) VALUES ({placeholders}) RETURNING "id"'
        row = session.fetchone(sql, values)
        if row:
            self.id = row[0]
        return self

    # ── CRUD: Insert Many ─────────────────────

    @classmethod
    async def insert_many(cls: Type[T], documents: Sequence[T]) -> list[T]:
        """Bulk insert multiple documents."""
        if not documents:
            return []

        session = get_session()
        table = cls._get_table_name()

        results = []
        for doc in documents:
            inserted = await doc.insert()
            results.append(inserted)
        return results

    @classmethod
    def insert_many_sync(cls: Type[T], documents: Sequence[T]) -> list[T]:
        """Bulk insert multiple documents synchronously."""
        return [doc.insert_sync() for doc in documents]

    # ── CRUD: Save (Upsert) ───────────────────

    async def save(self: T) -> T:
        """
        Save (upsert) this document.
        If the document has an id and exists → UPDATE.
        Otherwise → INSERT.
        """
        if self.id is not None:
            session = get_session()
            table = self._get_table_name()
            data = self._to_row_dict()
            data.pop("id")

            if not data:
                return self

            set_parts = [f'"{col}" = ?' for col in data]
            values = list(data.values()) + [self.id]

            sql = f'UPDATE "{table}" SET {", ".join(set_parts)} WHERE "id" = ?'
            await session.async_execute(sql, values)
            return self
        else:
            return await self.insert()

    def save_sync(self: T) -> T:
        """Save (upsert) this document synchronously."""
        if self.id is not None:
            session = get_session()
            table = self._get_table_name()
            data = self._to_row_dict()
            data.pop("id")

            if not data:
                return self

            set_parts = [f'"{col}" = ?' for col in data]
            values = list(data.values()) + [self.id]

            sql = f'UPDATE "{table}" SET {", ".join(set_parts)} WHERE "id" = ?'
            session.execute(sql, values)
            return self
        else:
            return self.insert_sync()

    # ── CRUD: Delete ──────────────────────────

    async def delete(self) -> None:
        """Delete this document from the database."""
        if self.id is None:
            raise InvalidQueryError("Cannot delete a document without an id")

        session = get_session()
        table = self._get_table_name()
        await session.async_execute(f'DELETE FROM "{table}" WHERE "id" = ?', [self.id])

    def delete_sync(self) -> None:
        """Delete this document synchronously."""
        if self.id is None:
            raise InvalidQueryError("Cannot delete a document without an id")

        session = get_session()
        table = self._get_table_name()
        session.execute(f'DELETE FROM "{table}" WHERE "id" = ?', [self.id])

    # ── CRUD: Delete All ──────────────────────

    @classmethod
    async def delete_all(cls) -> None:
        """Delete all documents in the table."""
        session = get_session()
        table = cls._get_table_name()
        await session.async_execute(f'DELETE FROM "{table}"')

    @classmethod
    def delete_all_sync(cls) -> None:
        """Delete all documents synchronously."""
        session = get_session()
        table = cls._get_table_name()
        session.execute(f'DELETE FROM "{table}"')

    # ── Query: find / find_one / find_all ─────

    @classmethod
    def find(cls: Type[T], *conditions: Expression) -> FindQuery[T]:
        """
        Create a query builder with optional filter conditions.

        Usage:
            users = await User.find(User.age > 25).to_list()
            users = await User.find(User.name == "Alice", User.active == True).to_list()
        """
        return FindQuery(cls, *conditions)

    @classmethod
    def find_all(cls: Type[T]) -> FindQuery[T]:
        """Return a query for all documents (no filter)."""
        return FindQuery(cls)

    @classmethod
    async def find_one(cls: Type[T], *conditions: Expression) -> Optional[T]:
        """Find a single document matching the conditions."""
        return await FindQuery(cls, *conditions).first_or_none()

    @classmethod
    def find_one_sync(cls: Type[T], *conditions: Expression) -> Optional[T]:
        """Find a single document synchronously."""
        return FindQuery(cls, *conditions).first_or_none_sync()

    # ── Query: get by id ──────────────────────

    @classmethod
    async def get(cls: Type[T], doc_id: int) -> Optional[T]:
        """Get a document by its primary key id."""
        session = get_session()
        table = cls._get_table_name()
        row = await session.async_fetchone(
            f'SELECT * FROM "{table}" WHERE "id" = ?', [doc_id]
        )
        if row is None:
            return None
        return cls._from_row(row, cls._get_column_names())

    @classmethod
    def get_sync(cls: Type[T], doc_id: int) -> Optional[T]:
        """Get a document by id synchronously."""
        session = get_session()
        table = cls._get_table_name()
        row = session.fetchone(
            f'SELECT * FROM "{table}" WHERE "id" = ?', [doc_id]
        )
        if row is None:
            return None
        return cls._from_row(row, cls._get_column_names())

    # ── Query: count ──────────────────────────

    @classmethod
    async def count(cls) -> int:
        """Count all documents in the table."""
        return await cls.find_all().count()

    @classmethod
    def count_sync(cls) -> int:
        """Count all documents synchronously."""
        return cls.find_all().count_sync()

    # ── Refresh ───────────────────────────────

    async def refresh(self: T) -> T:
        """Reload this document's data from the database."""
        if self.id is None:
            raise InvalidQueryError("Cannot refresh a document without an id")

        fresh = await self.__class__.get(self.id)
        if fresh is None:
            raise DocumentNotFound(f"{self.__class__.__name__} with id={self.id} not found")

        for field_name in self.model_fields:
            setattr(self, field_name, getattr(fresh, field_name))
        return self

    # ── Repr ──────────────────────────────────

    def __repr__(self) -> str:
        fields = ", ".join(f"{k}={getattr(self, k)!r}" for k in self.model_fields)
        return f"{self.__class__.__name__}({fields})"
