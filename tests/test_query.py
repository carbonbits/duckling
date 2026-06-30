"""Tests for duckling.query — find, sort, pagination, counting, aggregation."""

import pytest

from duckling.query import Avg, Count, Max, Min, Sum

from .models import User


class TestSyncQuery:
    def test_find(self, sync_db):
        User(name="A", email="a@test.com", age=20).insert_sync()
        User(name="B", email="b@test.com", age=30).insert_sync()
        User(name="C", email="c@test.com", age=40).insert_sync()

        results = User.find(User.age > 25).to_list_sync()
        assert len(results) == 2
        names = {u.name for u in results}
        assert names == {"B", "C"}

    def test_find_one(self, sync_db):
        User(name="Eve", email="eve@test.com", age=28).insert_sync()

        found = User.find_one_sync(User.name == "Eve")
        assert found is not None
        assert found.email == "eve@test.com"

    def test_count(self, sync_db):
        User(name="A", email="a@test.com").insert_sync()
        User(name="B", email="b@test.com").insert_sync()

        assert User.count_sync() == 2

    def test_sort(self, sync_db):
        User(name="C", email="c@test.com", age=30).insert_sync()
        User(name="A", email="a@test.com", age=10).insert_sync()
        User(name="B", email="b@test.com", age=20).insert_sync()

        results = User.find_all().sort("+name").to_list_sync()
        assert [u.name for u in results] == ["A", "B", "C"]

        results = User.find_all().sort("-age").to_list_sync()
        assert [u.age for u in results] == [30, 20, 10]

    def test_limit_skip(self, sync_db):
        for i in range(10):
            User(name=f"U{i}", email=f"u{i}@test.com", age=i * 10).insert_sync()

        results = User.find_all().sort("+age").limit(3).to_list_sync()
        assert len(results) == 3

        results = User.find_all().sort("+age").skip(2).limit(3).to_list_sync()
        assert len(results) == 3
        assert results[0].age == 20


class TestAsyncQuery:
    @pytest.mark.asyncio
    async def test_find_with_conditions(self, async_db):
        await User.insert_many([
            User(name="A", email="a@test.com", age=20),
            User(name="B", email="b@test.com", age=30),
            User(name="C", email="c@test.com", age=40),
        ])

        results = await User.find(User.age >= 30).sort("-age").to_list()
        assert len(results) == 2
        assert results[0].name == "C"

    @pytest.mark.asyncio
    async def test_find_one(self, async_db):
        await User(name="Target", email="target@test.com", age=99).insert()

        found = await User.find_one(User.age == 99)
        assert found is not None
        assert found.name == "Target"

    @pytest.mark.asyncio
    async def test_count(self, async_db):
        await User.insert_many([
            User(name="A", email="a@test.com"),
            User(name="B", email="b@test.com"),
            User(name="C", email="c@test.com"),
        ])
        assert await User.count() == 3
        assert await User.find(User.name == "A").count() == 1

    @pytest.mark.asyncio
    async def test_exists(self, async_db):
        assert not await User.find(User.name == "Ghost").exists()

        await User(name="Ghost", email="ghost@test.com").insert()
        assert await User.find(User.name == "Ghost").exists()

    @pytest.mark.asyncio
    async def test_aggregation(self, async_db):
        await User.insert_many([
            User(name="A", email="a@test.com", age=20),
            User(name="B", email="b@test.com", age=30),
            User(name="C", email="c@test.com", age=40),
        ])

        stats = await User.find_all().aggregate(
            avg_age=Avg("age"),
            total=Count(),
            max_age=Max("age"),
            min_age=Min("age"),
            sum_age=Sum("age"),
        )
        assert stats["total"] == 3
        assert stats["avg_age"] == 30.0
        assert stats["max_age"] == 40
        assert stats["min_age"] == 20
        assert stats["sum_age"] == 90

    @pytest.mark.asyncio
    async def test_filtered_aggregation(self, async_db):
        await User.insert_many([
            User(name="A", email="a@test.com", age=20),
            User(name="B", email="b@test.com", age=30),
            User(name="C", email="c@test.com", age=40),
        ])

        stats = await User.find(User.age > 20).aggregate(
            avg_age=Avg("age"),
            total=Count(),
        )
        assert stats["total"] == 2
        assert stats["avg_age"] == 35.0

    @pytest.mark.asyncio
    async def test_chained_query(self, async_db):
        for i in range(20):
            await User(name=f"User{i:02d}", email=f"u{i}@test.com", age=i).insert()

        results = (
            await User.find(User.age >= 5)
            .find(User.age < 15)
            .sort("+age")
            .skip(2)
            .limit(5)
            .to_list()
        )
        assert len(results) == 5
        assert results[0].age == 7

    @pytest.mark.asyncio
    async def test_async_iteration(self, async_db):
        await User.insert_many([
            User(name="A", email="a@test.com", age=1),
            User(name="B", email="b@test.com", age=2),
            User(name="C", email="c@test.com", age=3),
        ])

        names = []
        async for user in User.find_all().sort("+name"):
            names.append(user.name)
        assert names == ["A", "B", "C"]
