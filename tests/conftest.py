"""Shared fixtures for the Duckling test suite.

Tests run against the *installed* package (`pip install -e .`), so there is no
`sys.path` manipulation here — that is the whole point of the src layout.
"""

import pytest
import pytest_asyncio

from duckling import DucklingSession, init_duckling, init_duckling_sync

from .models import ALL_MODELS


@pytest.fixture(autouse=True)
def reset_session():
    """Reset the singleton session before and after each test."""
    DucklingSession.reset()
    yield
    DucklingSession.reset()


@pytest.fixture
def sync_db():
    """Create a sync in-memory database."""
    return init_duckling_sync(database=":memory:", document_models=ALL_MODELS)


@pytest_asyncio.fixture
async def async_db():
    """Create an async in-memory database."""
    return await init_duckling(database=":memory:", document_models=ALL_MODELS)
