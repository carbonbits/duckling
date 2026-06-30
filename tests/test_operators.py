"""Tests for duckling.operators — comparison and boolean operators."""

import pytest

from duckling.operators import And, Between, In, Like, Not, Or

from .models import User


class TestSyncOperators:
    def test_complex_query(self, sync_db):
        User(name="A", email="a@test.com", age=20, active=True).insert_sync()
        User(name="B", email="b@test.com", age=30, active=False).insert_sync()
        User(name="C", email="c@test.com", age=40, active=True).insert_sync()

        # AND condition
        results = User.find(
            (User.age > 20) & (User.active == True)
        ).to_list_sync()
        assert len(results) == 1
        assert results[0].name == "C"

        # OR condition
        results = User.find(
            (User.name == "A") | (User.name == "C")
        ).to_list_sync()
        assert len(results) == 2

    def test_in_and_between(self, sync_db):
        for i in range(5):
            User(name=f"U{i}", email=f"u{i}@test.com", age=i * 10).insert_sync()

        results = User.find(In(User.age, [10, 30])).to_list_sync()
        assert len(results) == 2

        results = User.find(Between(User.age, 10, 30)).to_list_sync()
        assert len(results) == 3  # 10, 20, 30

    def test_like(self, sync_db):
        User(name="Alice Smith", email="alice@test.com").insert_sync()
        User(name="Bob Jones", email="bob@test.com").insert_sync()
        User(name="Alicia Keys", email="alicia@test.com").insert_sync()

        results = User.find(Like(User.name, "Ali%")).to_list_sync()
        assert len(results) == 2


class TestAsyncOperators:
    @pytest.mark.asyncio
    async def test_boolean_operators(self, async_db):
        await User.insert_many([
            User(name="A", email="a@test.com", age=20, active=True),
            User(name="B", email="b@test.com", age=30, active=False),
            User(name="C", email="c@test.com", age=40, active=True),
        ])

        results = await User.find(
            And(User.active == True, User.age > 25)
        ).to_list()
        assert len(results) == 1
        assert results[0].name == "C"

        results = await User.find(
            Or(User.age == 20, User.age == 40)
        ).to_list()
        assert len(results) == 2

        results = await User.find(
            Not(User.active == True)
        ).to_list()
        assert len(results) == 1
        assert results[0].name == "B"
