"""Tests for duckling.connection — session and transaction management."""

import pytest

from duckling import get_session

from .models import User


class TestConnection:
    @pytest.mark.asyncio
    async def test_transaction(self, async_db):
        session = get_session()
        async with session.async_transaction():
            await User(name="TxUser", email="tx@test.com", age=50).insert()

        found = await User.find_one(User.name == "TxUser")
        assert found is not None
