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

import asyncio
import re
import typing
from typing import (
    Any,
    Optional,
    Sequence,
    Type,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)

import duckdb
from pydantic import BaseModel, ConfigDict

from duckling.connection import get_session
from duckling.document.meta import DocumentMeta
from duckling.document.types import (
    duckdb_value_to_python,
    python_type_to_duckdb,
    python_value_to_duckdb,
)
from duckling.exceptions import DocumentAlreadyExists, DocumentNotFound, InvalidQueryError
from duckling.expressions import Expression
from duckling.fields import IndexSpec
from duckling.identifiers import qualified_name, quote_ident
from duckling.query import FindQuery

T = TypeVar("T", bound="Document")


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
        schema_name: Optional[str] = None
        indexes: list = []

    # ── Table name resolution ─────────────────

    @classmethod
    def _get_table_name(cls) -> str:
        """
        The bare (unqualified) table name. Never includes a schema — see
        `_get_schema_name` for that, and `_get_qualified_table_name` for the
        quoted reference to use in SQL.
        """
        if hasattr(cls, "Settings") and hasattr(cls.Settings, "table_name") and cls.Settings.table_name:
            return cls.Settings.table_name
        # Auto-generate from class name: UserProfile → user_profile
        name = cls.__name__
        return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()

    @classmethod
    def _get_schema_name(cls) -> Optional[str]:
        """
        The schema holding this model's table, or None for the connection's
        default schema (`main`, unless the caller changed the search path).

            class Role(Document):
                class Settings:
                    schema_name = "v1"
                    table_name = "roles"
        """
        if hasattr(cls, "Settings"):
            return getattr(cls.Settings, "schema_name", None) or None
        return None

    @classmethod
    def _get_qualified_table_name(cls) -> str:
        """The quoted table reference for SQL: `"roles"` or `"v1"."roles"`."""
        return qualified_name(cls._get_schema_name(), cls._get_table_name())

    # ── Field ↔ column name mapping ───────────

    @classmethod
    def _get_column_name(cls, field_name: str) -> str:
        """
        The database column backing a model field.

        A field declared with `Field(alias=...)` is stored under its alias, so
        the Python attribute and the column can differ:

            class Tag(Document):
                id: str = Field(alias="key")   # self.id ↔ the "key" column

        Names that are not model fields pass through unchanged, which makes
        this safe to apply to strings that are already column names.
        """
        field = cls.model_fields.get(field_name)
        if field is not None and field.alias:
            return field.alias
        return field_name

    @classmethod
    def _get_pk_column(cls) -> str:
        """The column holding the primary key. `self.id` always maps to it."""
        return cls._get_column_name("id")

    @classmethod
    def _get_column_names(cls) -> list[str]:
        return [cls._get_column_name(name) for name in cls.model_fields]

    @classmethod
    def _select_columns_sql(cls) -> str:
        """
        An explicit, quoted column list for SELECT.

        Never use `SELECT *`: rows are mapped back onto fields positionally by
        `_from_row`, so the projection order must be the model's field order
        rather than whatever order the table happens to have on disk.
        """
        return ", ".join(quote_ident(c) for c in cls._get_column_names())

    @classmethod
    def _get_column_types(cls) -> dict[str, str]:
        """Map column names to DuckDB column types."""
        hints = get_type_hints(cls, include_extras=True)
        result = {}
        for name in cls.model_fields:
            py_type = hints.get(name, str)
            result[cls._get_column_name(name)] = python_type_to_duckdb(py_type)
        return result

    @classmethod
    def _get_indexed_fields(cls) -> list[tuple[str, IndexSpec]]:
        """Return (column_name, spec) for fields with Indexed() annotations."""
        hints = get_type_hints(cls, include_extras=True)
        indexed = []
        for name in cls.model_fields:
            py_type = hints.get(name)
            if py_type and get_origin(py_type) is getattr(typing, "Annotated", None):
                args = get_args(py_type)
                for arg in args[1:]:
                    if isinstance(arg, IndexSpec):
                        indexed.append((cls._get_column_name(name), arg))
        return indexed

    # ── Primary key strategy ──────────────────

    @classmethod
    def _is_auto_increment_id(cls) -> bool:
        """
        True if the `id` field resolves to `int` (the default primary key
        strategy: an auto-incrementing INTEGER backed by a DuckDB SEQUENCE).

        Any other resolved type (str, uuid.UUID, ...) is treated as a
        caller/default_factory-supplied primary key with no sequence.
        """
        hints = get_type_hints(cls, include_extras=True)
        py_type = hints.get("id", int)

        origin = get_origin(py_type)
        if origin is getattr(typing, "Annotated", None):
            py_type = get_args(py_type)[0]
            origin = get_origin(py_type)

        args = get_args(py_type)
        if args:
            non_none = [a for a in args if a is not type(None)]
            if non_none:
                py_type = non_none[0]

        return py_type is int

    # ── Table creation ────────────────────────

    @classmethod
    def _build_create_schema_sql(cls) -> Optional[str]:
        """Generate CREATE SCHEMA for the model's schema, or None if it uses the default."""
        schema = cls._get_schema_name()
        if not schema:
            return None
        return f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)}"

    @classmethod
    def _build_create_table_sql(cls) -> str:
        """Generate CREATE TABLE IF NOT EXISTS SQL."""
        table = cls._get_qualified_table_name()
        col_types = cls._get_column_types()
        indexed = dict(cls._get_indexed_fields())
        pk = cls._get_pk_column()

        if cls._is_auto_increment_id():
            # nextval() takes the sequence as a string literal, so the qualified
            # name goes inside single quotes with its own double quotes intact.
            seq = cls._get_qualified_sequence_name()
            id_def = f"{quote_ident(pk)} INTEGER PRIMARY KEY DEFAULT(nextval('{seq}'))"
        else:
            id_def = f"{quote_ident(pk)} {col_types[pk]} PRIMARY KEY"

        columns = []
        for col_name, col_type in col_types.items():
            if col_name == pk:
                continue
            parts = [quote_ident(col_name), col_type]
            if col_name in indexed and indexed[col_name].unique:
                parts.append("UNIQUE")
            columns.append(" ".join(parts))

        col_defs = [id_def] + columns

        return f"CREATE TABLE IF NOT EXISTS {table} (\n  " + ",\n  ".join(col_defs) + "\n)"

    @classmethod
    def _get_sequence_name(cls) -> str:
        """Bare name of the sequence backing an auto-increment primary key."""
        return f"seq_{cls._get_table_name()}_{cls._get_pk_column()}"

    @classmethod
    def _get_qualified_sequence_name(cls) -> str:
        """
        The quoted sequence reference for SQL.

        The sequence lives in the same schema as its table, so two schemas can
        each hold a `roles` table without their sequences colliding.
        """
        return qualified_name(cls._get_schema_name(), cls._get_sequence_name())

    @classmethod
    def _build_sequence_sql(cls) -> Optional[str]:
        if not cls._is_auto_increment_id():
            return None
        return f"CREATE SEQUENCE IF NOT EXISTS {cls._get_qualified_sequence_name()} START 1"

    @classmethod
    def _create_table_sync(cls) -> None:
        """Create the table synchronously."""
        session = get_session()
        schema_sql = cls._build_create_schema_sql()
        if schema_sql:
            session.execute(schema_sql)
        seq_sql = cls._build_sequence_sql()
        if seq_sql:
            session.execute(seq_sql)
        session.execute(cls._build_create_table_sql())

        # Create indexes. The index name is deliberately unqualified — DuckDB
        # places the index in the schema of the table it targets.
        table = cls._get_qualified_table_name()
        for field_name, spec in cls._get_indexed_fields():
            idx_name = f"idx_{cls._get_table_name()}_{field_name}"
            unique = "UNIQUE " if spec.unique else ""
            try:
                session.execute(
                    f"CREATE {unique}INDEX IF NOT EXISTS {quote_ident(idx_name)} "
                    f"ON {table} ({quote_ident(field_name)})"
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
        cls = type(self)
        data = {}
        for name in cls.model_fields:
            val = getattr(self, name)
            data[cls._get_column_name(name)] = python_value_to_duckdb(val)
        return data

    @classmethod
    def _get_field_names_by_column(cls) -> dict[str, str]:
        """Reverse of the field → column mapping."""
        return {cls._get_column_name(name): name for name in cls.model_fields}

    @classmethod
    def _from_row(cls: Type[T], row: tuple, columns: list[str]) -> T:
        """Create a document instance from a database row."""
        hints = get_type_hints(cls, include_extras=True)
        fields_by_column = cls._get_field_names_by_column()
        data = {}
        for col_name, value in zip(columns, row):
            # Values arrive keyed by column; convert using the field's declared
            # type and hand Pydantic the field name (populate_by_name is on).
            field_name = fields_by_column.get(col_name, col_name)
            py_type = hints.get(field_name, str)
            data[field_name] = duckdb_value_to_python(value, py_type)
        return cls.model_validate(data)

    # ── CRUD: Insert ──────────────────────────

    def _build_insert_sql(self) -> tuple[str, list, dict[str, Any]]:
        """Build the INSERT for this document, returning (sql, values, data)."""
        table = self._get_qualified_table_name()
        pk = self._get_pk_column()
        data = self._to_row_dict()

        if data.get(pk) is None:
            if self._is_auto_increment_id():
                # Drop it so the sequence default supplies the value.
                data.pop(pk, None)
            else:
                raise InvalidQueryError(
                    f"{type(self).__name__}.id has no value and no default. "
                    f"A non-integer primary key must be supplied by the caller "
                    f"or by a default_factory."
                )

        columns = list(data.keys())
        placeholders = ", ".join("?" for _ in columns)
        col_str = ", ".join(quote_ident(c) for c in columns)
        values = [data[c] for c in columns]

        sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders}) RETURNING {quote_ident(pk)}"
        return sql, values, data

    def _already_exists_error(self, data: dict[str, Any]) -> DocumentAlreadyExists:
        pk_value = data.get(self._get_pk_column())
        return DocumentAlreadyExists(
            f"{type(self).__name__} with id={pk_value!r} already exists"
        )

    async def insert(self: T) -> T:
        """Insert this document into the database."""
        session = get_session()
        sql, values, data = self._build_insert_sql()

        try:
            row = await session.async_fetchone(sql, values)
        except duckdb.ConstraintException as e:
            raise self._already_exists_error(data) from e
        if row:
            self.id = row[0]
        return self

    def insert_sync(self: T) -> T:
        """Insert this document synchronously."""
        session = get_session()
        sql, values, data = self._build_insert_sql()

        try:
            row = session.fetchone(sql, values)
        except duckdb.ConstraintException as e:
            raise self._already_exists_error(data) from e
        if row:
            self.id = row[0]
        return self

    # ── CRUD: Insert Many ─────────────────────

    @classmethod
    async def insert_many(cls: Type[T], documents: Sequence[T]) -> list[T]:
        """Bulk insert multiple documents."""
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
            sql, values = self._build_update_sql()
            if sql is None:
                return self
            await session.async_execute(sql, values)
            return self
        else:
            return await self.insert()

    def _build_update_sql(self) -> tuple[Optional[str], list]:
        """Build the UPDATE for this document, or (None, []) if it has no columns."""
        table = self._get_qualified_table_name()
        pk = self._get_pk_column()
        data = self._to_row_dict()
        data.pop(pk, None)

        if not data:
            return None, []

        set_parts = [f"{quote_ident(col)} = ?" for col in data]
        values = list(data.values()) + [self.id]
        return f'UPDATE {table} SET {", ".join(set_parts)} WHERE {quote_ident(pk)} = ?', values

    def save_sync(self: T) -> T:
        """Save (upsert) this document synchronously."""
        if self.id is not None:
            session = get_session()
            sql, values = self._build_update_sql()
            if sql is None:
                return self
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
        table = self._get_qualified_table_name()
        pk = self._get_pk_column()
        await session.async_execute(f"DELETE FROM {table} WHERE {quote_ident(pk)} = ?", [self.id])

    def delete_sync(self) -> None:
        """Delete this document synchronously."""
        if self.id is None:
            raise InvalidQueryError("Cannot delete a document without an id")

        session = get_session()
        table = self._get_qualified_table_name()
        pk = self._get_pk_column()
        session.execute(f"DELETE FROM {table} WHERE {quote_ident(pk)} = ?", [self.id])

    # ── CRUD: Delete All ──────────────────────

    @classmethod
    async def delete_all(cls) -> None:
        """Delete all documents in the table."""
        session = get_session()
        table = cls._get_qualified_table_name()
        await session.async_execute(f"DELETE FROM {table}")

    @classmethod
    def delete_all_sync(cls) -> None:
        """Delete all documents synchronously."""
        session = get_session()
        table = cls._get_qualified_table_name()
        session.execute(f"DELETE FROM {table}")

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
    async def get(cls: Type[T], doc_id: Any) -> Optional[T]:
        """Get a document by its primary key id."""
        session = get_session()
        table = cls._get_qualified_table_name()
        row = await session.async_fetchone(
            f"SELECT {cls._select_columns_sql()} FROM {table} "
            f"WHERE {quote_ident(cls._get_pk_column())} = ?",
            [doc_id],
        )
        if row is None:
            return None
        return cls._from_row(row, cls._get_column_names())

    @classmethod
    def get_sync(cls: Type[T], doc_id: Any) -> Optional[T]:
        """Get a document by id synchronously."""
        session = get_session()
        table = cls._get_qualified_table_name()
        row = session.fetchone(
            f"SELECT {cls._select_columns_sql()} FROM {table} "
            f"WHERE {quote_ident(cls._get_pk_column())} = ?",
            [doc_id],
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

        for field_name in type(self).model_fields:
            setattr(self, field_name, getattr(fresh, field_name))
        return self

    # ── Repr ──────────────────────────────────

    def __repr__(self) -> str:
        fields = ", ".join(f"{k}={getattr(self, k)!r}" for k in type(self).model_fields)
        return f"{self.__class__.__name__}({fields})"
