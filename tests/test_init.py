"""Tests for duckling.init — initialization and table creation."""

import pytest

from duckling import DucklingSession, get_session
from duckling.exceptions import NotInitializedError

from .models import AutoNamed, User


class TestInit:
    def test_init_creates_tables(self, sync_db):
        session = get_session()
        tables = session.fetchall("SHOW TABLES")
        table_names = [t[0] for t in tables]
        assert "users" in table_names
        assert "products" in table_names

    def test_auto_table_name(self, sync_db):
        assert AutoNamed._get_table_name() == "auto_named"

    @pytest.mark.asyncio
    async def test_not_initialized_error(self):
        """Operations should fail before init."""
        DucklingSession.reset()
        with pytest.raises(NotInitializedError):
            await User.find_all().to_list()
