"""
Initialization for the Duckling ORM.

Usage:
    from duckling import init_duckling, Document

    class User(Document):
        name: str

    # Async init
    await init_duckling(
        database=":memory:",
        document_models=[User],
    )

    # Sync init
    init_duckling_sync(
        database="my_data.db",
        document_models=[User],
    )
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional, Sequence, Type

from .connection import DucklingSession, get_session
from .document import Document


async def init_duckling(
    database: str = ":memory:",
    document_models: Optional[Sequence[Type[Document]]] = None,
    read_only: bool = False,
    config: Optional[dict[str, Any]] = None,
    recreate_tables: bool = False,
) -> DucklingSession:
    """
    Initialize the Duckling ORM — async version.

    This connects to DuckDB and creates tables for all registered document models.

    Args:
        database: Path to the DuckDB database file, or ":memory:" for in-memory.
        document_models: List of Document subclasses to register.
        read_only: Open database in read-only mode.
        config: Optional DuckDB configuration dict.
        recreate_tables: If True, drop and recreate all tables.

    Returns:
        The DucklingSession instance.

    Example:
        await init_duckling(
            database="app.db",
            document_models=[User, Product, Order],
        )
    """
    session = get_session()

    # Connect
    await asyncio.to_thread(
        session.connect,
        database=database,
        read_only=read_only,
        config=config,
    )

    # Register and create tables for each document model
    if document_models:
        for model in document_models:
            if recreate_tables:
                table = model._get_table_name()
                await session.async_execute(f'DROP TABLE IF EXISTS "{table}"')
                await session.async_execute(f"DROP SEQUENCE IF EXISTS seq_{table}_id")
            await model._create_table()

    return session


def init_duckling_sync(
    database: str = ":memory:",
    document_models: Optional[Sequence[Type[Document]]] = None,
    read_only: bool = False,
    config: Optional[dict[str, Any]] = None,
    recreate_tables: bool = False,
) -> DucklingSession:
    """
    Initialize the Duckling ORM — synchronous version.

    Same as init_duckling() but runs synchronously.
    """
    session = get_session()

    session.connect(
        database=database,
        read_only=read_only,
        config=config,
    )

    if document_models:
        for model in document_models:
            if recreate_tables:
                table = model._get_table_name()
                session.execute(f'DROP TABLE IF EXISTS "{table}"')
                session.execute(f"DROP SEQUENCE IF EXISTS seq_{table}_id")
            model._create_table_sync()

    return session
