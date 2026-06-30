"""
Query builder for Duckling — fluent, chainable queries inspired by Beanie.

Usage:
    # Chain methods to build queries
    users = await User.find(User.age > 25).sort("+name").limit(10).to_list()
    user  = await User.find_one(User.name == "Alice")
    count = await User.find(User.active == True).count()

    # Projection (select specific fields)
    names = await User.find().project(name=1, email=1).to_list()

    # Aggregation
    stats = await User.find().aggregate(avg_age=Avg("age"), total=Count())
"""

from __future__ import annotations

import asyncio
from typing import Any, Generic, Optional, Type, TypeVar

from .connection import get_session
from .fields import Expression, FieldProxy, SortDirection

T = TypeVar("T")


# ──────────────────────────────────────────────
# Aggregation functions
# ──────────────────────────────────────────────
class AggFunc:
    """Base aggregation function."""

    def to_sql(self) -> str:
        raise NotImplementedError


class Count(AggFunc):
    def __init__(self, field: str = "*"):
        self.field = field

    def to_sql(self) -> str:
        if self.field == "*":
            return "COUNT(*)"
        return f'COUNT("{self.field}")'


class Sum(AggFunc):
    def __init__(self, field: str):
        self.field = field

    def to_sql(self) -> str:
        return f'SUM("{self.field}")'


class Avg(AggFunc):
    def __init__(self, field: str):
        self.field = field

    def to_sql(self) -> str:
        return f'AVG("{self.field}")'


class Min(AggFunc):
    def __init__(self, field: str):
        self.field = field

    def to_sql(self) -> str:
        return f'MIN("{self.field}")'


class Max(AggFunc):
    def __init__(self, field: str):
        self.field = field

    def to_sql(self) -> str:
        return f'MAX("{self.field}")'


class CountDistinct(AggFunc):
    def __init__(self, field: str):
        self.field = field

    def to_sql(self) -> str:
        return f'COUNT(DISTINCT "{self.field}")'


# ──────────────────────────────────────────────
# FindQuery — the main query builder
# ──────────────────────────────────────────────
class FindQuery(Generic[T]):
    """
    A fluent, chainable query builder for finding documents.

    Mirrors Beanie's query interface:
        results = await Model.find(condition).sort(...).skip(...).limit(...).to_list()
    """

    def __init__(
        self,
        document_class: Type[T],
        *conditions: Expression,
    ) -> None:
        self._document_class = document_class
        self._conditions: list[Expression] = list(conditions)
        self._sort_clauses: list[tuple[str, SortDirection]] = []
        self._limit_val: Optional[int] = None
        self._skip_val: int = 0
        self._projection: Optional[list[str]] = None
        self._group_by: Optional[list[str]] = None

    # ── Chainable methods ─────────────────────

    def find(self, *conditions: Expression) -> FindQuery[T]:
        """Add additional filter conditions (AND)."""
        self._conditions.extend(conditions)
        return self

    def sort(self, *keys: str | tuple[str, SortDirection] | tuple[str, int]) -> FindQuery[T]:
        """
        Add sort clauses. Accepts:
            - "+field" / "-field" strings
            - (field_name, SortDirection) tuples
            - FieldProxy.asc() / FieldProxy.desc() results
        """
        for key in keys:
            if isinstance(key, str):
                if key.startswith("-"):
                    self._sort_clauses.append((key[1:], SortDirection.DESCENDING))
                elif key.startswith("+"):
                    self._sort_clauses.append((key[1:], SortDirection.ASCENDING))
                else:
                    self._sort_clauses.append((key, SortDirection.ASCENDING))
            elif isinstance(key, tuple) and len(key) == 2:
                field_name, direction = key
                if isinstance(direction, int):
                    direction = SortDirection(direction)
                self._sort_clauses.append((field_name, direction))
        return self

    def limit(self, n: int) -> FindQuery[T]:
        """Limit the number of results."""
        self._limit_val = n
        return self

    def skip(self, n: int) -> FindQuery[T]:
        """Skip the first n results."""
        self._skip_val = n
        return self

    def project(self, *fields: str, **named_fields: int) -> FindQuery[T]:
        """
        Select specific fields (projection).

            .project("name", "email")
            .project(name=1, email=1)
        """
        proj = list(fields)
        for name, include in named_fields.items():
            if include:
                proj.append(name)
        self._projection = proj
        return self

    # ── SQL Generation ────────────────────────

    def _get_table_name(self) -> str:
        return self._document_class._get_table_name()

    def _build_where(self) -> tuple[str, list]:
        if not self._conditions:
            return "", []

        parts = []
        params = []
        for cond in self._conditions:
            sql, p = cond.to_sql()
            parts.append(sql)
            params.extend(p)

        return " AND ".join(parts), params

    def _build_select_sql(self) -> tuple[str, list]:
        table = self._get_table_name()

        # Columns
        if self._projection:
            cols = ", ".join(f'"{c}"' for c in self._projection)
        else:
            cols = "*"

        sql = f'SELECT {cols} FROM "{table}"'
        params: list = []

        # WHERE
        where_sql, where_params = self._build_where()
        if where_sql:
            sql += f" WHERE {where_sql}"
            params.extend(where_params)

        # ORDER BY
        if self._sort_clauses:
            order_parts = []
            for field_name, direction in self._sort_clauses:
                dir_str = "ASC" if direction == SortDirection.ASCENDING else "DESC"
                order_parts.append(f'"{field_name}" {dir_str}')
            sql += " ORDER BY " + ", ".join(order_parts)

        # LIMIT / OFFSET
        if self._limit_val is not None:
            sql += f" LIMIT {self._limit_val}"
        if self._skip_val:
            sql += f" OFFSET {self._skip_val}"

        return sql, params

    def _build_count_sql(self) -> tuple[str, list]:
        table = self._get_table_name()
        sql = f'SELECT COUNT(*) FROM "{table}"'
        params: list = []

        where_sql, where_params = self._build_where()
        if where_sql:
            sql += f" WHERE {where_sql}"
            params.extend(where_params)

        return sql, params

    def _build_delete_sql(self) -> tuple[str, list]:
        table = self._get_table_name()
        sql = f'DELETE FROM "{table}"'
        params: list = []

        where_sql, where_params = self._build_where()
        if where_sql:
            sql += f" WHERE {where_sql}"
            params.extend(where_params)

        return sql, params

    def _build_update_sql(self, updates: dict[str, Any]) -> tuple[str, list]:
        table = self._get_table_name()
        set_parts = []
        params: list = []

        for col, val in updates.items():
            set_parts.append(f'"{col}" = ?')
            params.append(val)

        sql = f'UPDATE "{table}" SET {", ".join(set_parts)}'

        where_sql, where_params = self._build_where()
        if where_sql:
            sql += f" WHERE {where_sql}"
            params.extend(where_params)

        return sql, params

    # ── Execution (async) ─────────────────────

    async def to_list(self) -> list[T]:
        """Execute query and return list of document instances."""
        sql, params = self._build_select_sql()
        session = get_session()
        rows = await session.async_fetchall(sql, params)

        if not rows:
            return []

        # Get column names
        col_names = self._projection or list(self._document_class.model_fields.keys())
        if not self._projection:
            col_names = self._document_class._get_column_names()

        return [self._document_class._from_row(row, col_names) for row in rows]

    async def first_or_none(self) -> Optional[T]:
        """Return the first result or None."""
        self._limit_val = 1
        results = await self.to_list()
        return results[0] if results else None

    async def count(self) -> int:
        """Return the count of matching documents."""
        sql, params = self._build_count_sql()
        session = get_session()
        row = await session.async_fetchone(sql, params)
        return row[0] if row else 0

    async def exists(self) -> bool:
        """Check if any matching documents exist."""
        return (await self.count()) > 0

    async def delete(self) -> int:
        """Delete all matching documents. Returns number of deleted rows."""
        sql, params = self._build_delete_sql()
        session = get_session()
        result = await session.async_execute(sql, params)
        return result.fetchone()[0] if result.description else 0

    async def update(self, updates: dict[str, Any]) -> int:
        """Update all matching documents with the given values."""
        sql, params = self._build_update_sql(updates)
        session = get_session()
        await session.async_execute(sql, params)
        return 0  # DuckDB doesn't return affected rows easily

    async def aggregate(self, **agg_funcs: AggFunc) -> dict[str, Any]:
        """
        Run aggregation functions on matching documents.

            result = await User.find().aggregate(
                avg_age=Avg("age"),
                total=Count(),
                max_age=Max("age"),
            )
        """
        table = self._get_table_name()
        agg_parts = []
        for alias, func in agg_funcs.items():
            agg_parts.append(f'{func.to_sql()} AS "{alias}"')

        sql = f'SELECT {", ".join(agg_parts)} FROM "{table}"'
        params: list = []

        where_sql, where_params = self._build_where()
        if where_sql:
            sql += f" WHERE {where_sql}"
            params.extend(where_params)

        session = get_session()
        row = await session.async_fetchone(sql, params)
        if row is None:
            return {alias: None for alias in agg_funcs}
        return dict(zip(agg_funcs.keys(), row))

    async def to_df(self):
        """Execute query and return a pandas DataFrame."""
        sql, params = self._build_select_sql()
        session = get_session()
        return await session.async_fetchdf(sql, params)

    # ── Execution (sync) ──────────────────────

    def to_list_sync(self) -> list[T]:
        """Synchronous version of to_list()."""
        sql, params = self._build_select_sql()
        session = get_session()
        rows = session.fetchall(sql, params)

        if not rows:
            return []

        col_names = self._projection or self._document_class._get_column_names()
        return [self._document_class._from_row(row, col_names) for row in rows]

    def first_or_none_sync(self) -> Optional[T]:
        """Synchronous version of first_or_none()."""
        self._limit_val = 1
        results = self.to_list_sync()
        return results[0] if results else None

    def count_sync(self) -> int:
        """Synchronous version of count()."""
        sql, params = self._build_count_sql()
        session = get_session()
        row = session.fetchone(sql, params)
        return row[0] if row else 0

    # ── Async iteration ───────────────────────

    def __aiter__(self):
        return FindQueryIterator(self)


class FindQueryIterator:
    """Async iterator for FindQuery results."""

    def __init__(self, query: FindQuery) -> None:
        self._query = query
        self._results: Optional[list] = None
        self._index = 0

    async def __anext__(self):
        if self._results is None:
            self._results = await self._query.to_list()
        if self._index >= len(self._results):
            raise StopAsyncIteration
        item = self._results[self._index]
        self._index += 1
        return item
