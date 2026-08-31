"""DuckDB connection management for Duckling."""

from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any, Callable, Optional

import duckdb

from .exceptions import ConnectionError, NotInitializedError

# Returns the connection to use for the calling thread.
ConnectionFactory = Callable[[], duckdb.DuckDBPyConnection]


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
        self._connection_factory: Optional[ConnectionFactory] = None
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
        """Reset the singleton (useful for testing).

        Only closes a connection the session itself is holding. One produced by
        a connection factory belongs to the caller, so it is dropped rather than
        closed — closing it would take the host application's handle with it.
        """
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
        self._connection_factory = None
        self._database = None
        self._config = {}
        self._initialized = True

    def use_connection_factory(self, factory: ConnectionFactory) -> None:
        """Resolve a connection per call instead of holding one.

        A DuckDB connection object cannot be driven from two threads at once —
        it segfaults the interpreter rather than raising. Holding a single
        handle therefore confines Duckling to one thread, which is a problem for
        a host application that already hands out a connection per thread (via
        `conn.cursor()`, the pattern DuckDB documents for threaded use).

        Pass a callable and it is invoked on every access, so each thread gets
        its own handle:

            init_duckling_sync(connection_factory=DB.get_connection)

        The factory owns the lifecycle: `reset()` and `close()` will not close
        what it returns, since the session did not create it.
        """
        self._connection_factory = factory
        self._connection = None
        self._database = None
        self._config = {}
        self._initialized = True

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get the active DuckDB connection.

        Every query path goes through here, so a factory installed by
        `use_connection_factory` is consulted for each one.
        """
        if self._connection_factory is not None:
            connection = self._connection_factory()
            if connection is None:
                raise NotInitializedError("The configured connection factory returned None.")
            return connection

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
        """Close the connection.

        A factory-provided connection is released, not closed: the session did
        not open it and the caller may still be using it.
        """
        if self._connection:
            self._connection.close()
        self._connection = None
        self._connection_factory = None
        self._initialized = False


def get_session() -> DucklingSession:
    """Get the current Duckling session."""
    return DucklingSession.get_instance()
