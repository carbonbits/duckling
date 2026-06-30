"""Tests for duckling.document — CRUD operations on Document."""

import pytest

from duckling import get_session

from .models import User


class TestSyncCrud:
    def test_insert(self, sync_db):
        user = User(name="Alice", email="alice@test.com", age=30)
        user.insert_sync()
        assert user.id is not None
        assert user.id >= 1

    def test_get_by_id(self, sync_db):
        user = User(name="Bob", email="bob@test.com", age=25)
        user.insert_sync()

        found = User.get_sync(user.id)
        assert found is not None
        assert found.name == "Bob"
        assert found.email == "bob@test.com"
        assert found.age == 25

    def test_save_update(self, sync_db):
        user = User(name="Charlie", email="charlie@test.com", age=35)
        user.insert_sync()

        user.age = 36
        user.save_sync()

        found = User.get_sync(user.id)
        assert found.age == 36

    def test_delete(self, sync_db):
        user = User(name="Dave", email="dave@test.com")
        user.insert_sync()
        uid = user.id

        user.delete_sync()
        assert User.get_sync(uid) is None

    def test_insert_many(self, sync_db):
        users = [
            User(name="X", email="x@test.com", age=20),
            User(name="Y", email="y@test.com", age=25),
            User(name="Z", email="z@test.com", age=30),
        ]
        results = User.insert_many_sync(users)
        assert len(results) == 3
        assert all(u.id is not None for u in results)

    def test_delete_all(self, sync_db):
        User(name="A", email="a@test.com").insert_sync()
        User(name="B", email="b@test.com").insert_sync()
        assert User.count_sync() == 2

        User.delete_all_sync()
        assert User.count_sync() == 0


class TestAsyncCrud:
    @pytest.mark.asyncio
    async def test_insert_and_get(self, async_db):
        user = User(name="Async Alice", email="aa@test.com", age=30)
        await user.insert()
        assert user.id is not None

        found = await User.get(user.id)
        assert found.name == "Async Alice"

    @pytest.mark.asyncio
    async def test_save_upsert(self, async_db):
        user = User(name="Updatable", email="up@test.com", age=20)
        await user.save()  # should insert
        assert user.id is not None

        user.age = 21
        await user.save()  # should update

        found = await User.get(user.id)
        assert found.age == 21

    @pytest.mark.asyncio
    async def test_delete(self, async_db):
        user = User(name="Doomed", email="doom@test.com")
        await user.insert()
        uid = user.id

        await user.delete()
        assert await User.get(uid) is None

    @pytest.mark.asyncio
    async def test_delete_all(self, async_db):
        await User.insert_many([
            User(name="A", email="a@test.com"),
            User(name="B", email="b@test.com"),
        ])
        assert await User.count() == 2

        await User.delete_all()
        assert await User.count() == 0

    @pytest.mark.asyncio
    async def test_refresh(self, async_db):
        user = User(name="Original", email="orig@test.com", age=10)
        await user.insert()

        # Simulate external update
        session = get_session()
        await session.async_execute(
            'UPDATE "users" SET "age" = ? WHERE "id" = ?', [99, user.id]
        )

        await user.refresh()
        assert user.age == 99
