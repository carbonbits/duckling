"""DuckDB connection management for Duckling."""

from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any, Optional

import duckdb

from .exceptions import ConnectionError, NotInitializedError


class DucklingSession:
    """
    Manages DuckDB connections and provides both sync and async access.

    This is a singleton that holds the DuckDB connection used by all
    Document models registered with Duckling.
    """

    _instance: Optional[DucklingSession] = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
        self._database: Optional[str] = None
        self._config: dict[str, Any] = {}
        self._initialized = False

    @classmethod
    def get_instance(cls) -> DucklingSession:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton (useful for testing)."""
        if cls._instance and cls._instance._connection:
            try:
                cls._instance._connection.close()
            except Exception:
                pass
        cls._instance = None

    def connect(
        self,
        database: str = ":memory:",
        read_only: bool = False,
        config: Optional[dict[str, Any]] = None,
    ) -> duckdb.DuckDBPyConnection:
        """Establish a connection to DuckDB."""
        try:
            self._database = database
            self._config = config or {}
            self._connection = duckdb.connect(
                database=database,
                read_only=read_only,
                config=self._config,
            )
            self._initialized = True
            return self._connection
        except Exception as e:
            raise ConnectionError(f"Failed to connect to DuckDB: {e}") from e

    def use_connection(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Use an existing DuckDB connection."""
        self._connection = connection
        self._database = None
        self._config = {}
        self._initialized = True

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get the active DuckDB connection."""
        if not self._initialized or self._connection is None:
            raise NotInitializedError(
                "Duckling is not initialized. Call `await init_duckling(...)` first."
            )
        return self._connection

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    def execute(self, query: str, params: Optional[list] = None) -> duckdb.DuckDBPyConnection:
        """Execute a SQL query synchronously."""
        conn = self.connection
        if params:
            return conn.execute(query, params)
        return conn.execute(query)

    async def async_execute(self, query: str, params: Optional[list] = None) -> Any:
        """Execute a SQL query asynchronously via thread pool."""
        return await asyncio.to_thread(self.execute, query, params)

    def fetchall(self, query: str, params: Optional[list] = None) -> list[tuple]:
        """Execute and fetch all results synchronously."""
        result = self.execute(query, params)
        return result.fetchall()

    async def async_fetchall(self, query: str, params: Optional[list] = None) -> list[tuple]:
        """Execute and fetch all results asynchronously."""
        return await asyncio.to_thread(self.fetchall, query, params)

    def fetchone(self, query: str, params: Optional[list] = None) -> Optional[tuple]:
        """Execute and fetch one result synchronously."""
        result = self.execute(query, params)
        return result.fetchone()

    async def async_fetchone(self, query: str, params: Optional[list] = None) -> Optional[tuple]:
        """Execute and fetch one result asynchronously."""
        return await asyncio.to_thread(self.fetchone, query, params)

    def fetchdf(self, query: str, params: Optional[list] = None):
        """Execute and return results as a pandas DataFrame."""
        result = self.execute(query, params)
        return result.fetchdf()

    async def async_fetchdf(self, query: str, params: Optional[list] = None):
        """Execute and return results as a DataFrame asynchronously."""
        return await asyncio.to_thread(self.fetchdf, query, params)

    @contextmanager
    def transaction(self):
        """Synchronous transaction context manager."""
        conn = self.connection
        conn.execute("BEGIN TRANSACTION")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    @asynccontextmanager
    async def async_transaction(self):
        """Asynchronous transaction context manager."""
        conn = self.connection

        def begin():
            conn.execute("BEGIN TRANSACTION")

        def commit():
            conn.execute("COMMIT")

        def rollback():
            conn.execute("ROLLBACK")

        await asyncio.to_thread(begin)
        try:
            yield conn
            await asyncio.to_thread(commit)
        except Exception:
            await asyncio.to_thread(rollback)
            raise

    def close(self) -> None:
        """Close the connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            self._initialized = False


def get_session() -> DucklingSession:
    """Get the current Duckling session."""
    return DucklingSession.get_instance()
