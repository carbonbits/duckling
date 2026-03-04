"""
Comprehensive tests for the Duckling ORM.
Run with: pytest tests/ -v
"""

import asyncio
import datetime
import uuid
from typing import Annotated, List, Optional

import pytest
import pytest_asyncio

# Import duckling
import sys
sys.path.insert(0, ".")

from duckling import (
    Document,
    Indexed,
    IndexSpec,
    SortDirection,
    init_duckling,
    init_duckling_sync,
    FindQuery,
    DucklingSession,
    get_session,
)
from duckling.operators import (
    And, Or, Not, In, NotIn, Between, Like, ILike,
    Eq, Ne, Gt, Gte, Lt, Lte, IsNull, IsNotNull, Raw,
)
from duckling.query import Count, Sum, Avg, Min, Max, CountDistinct
from duckling.exceptions import (
    DocumentNotFound, InvalidQueryError, NotInitializedError,
)


# ──────────────────────────────────────────────
# Test Models
# ──────────────────────────────────────────────

class User(Document):
    name: str
    email: Annotated[str, IndexSpec(unique=True)]
    age: int = 0
    active: bool = True

    class Settings:
        table_name = "users"


class Product(Document):
    name: str
    price: float
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    in_stock: bool = True

    class Settings:
        table_name = "products"


class Event(Document):
    title: str
    date: datetime.date
    created_at: Optional[datetime.datetime] = None


class AutoNamed(Document):
    """Table name should auto-generate as 'auto_named'."""
    value: str


# ──────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────

@pytest.fixture(autouse=True)
def reset_session():
    """Reset the singleton session before each test."""
    DucklingSession.reset()
    yield
    DucklingSession.reset()


@pytest.fixture
def sync_db():
    """Create a sync in-memory database."""
    session = init_duckling_sync(
        database=":memory:",
        document_models=[User, Product, Event, AutoNamed],
    )
    return session


@pytest_asyncio.fixture
async def async_db():
    """Create an async in-memory database."""
    session = await init_duckling(
        database=":memory:",
        document_models=[User, Product, Event, AutoNamed],
    )
    return session


# ──────────────────────────────────────────────
# Sync Tests
# ──────────────────────────────────────────────

class TestSync:
    def test_init_creates_tables(self, sync_db):
        session = get_session()
        tables = session.fetchall("SHOW TABLES")
        table_names = [t[0] for t in tables]
        assert "users" in table_names
        assert "products" in table_names

    def test_auto_table_name(self, sync_db):
        assert AutoNamed._get_table_name() == "auto_named"

    def test_insert_sync(self, sync_db):
        user = User(name="Alice", email="alice@test.com", age=30)
        user.insert_sync()
        assert user.id is not None
        assert user.id >= 1

    def test_get_by_id_sync(self, sync_db):
        user = User(name="Bob", email="bob@test.com", age=25)
        user.insert_sync()

        found = User.get_sync(user.id)
        assert found is not None
        assert found.name == "Bob"
        assert found.email == "bob@test.com"
        assert found.age == 25

    def test_save_update_sync(self, sync_db):
        user = User(name="Charlie", email="charlie@test.com", age=35)
        user.insert_sync()

        user.age = 36
        user.save_sync()

        found = User.get_sync(user.id)
        assert found.age == 36

    def test_delete_sync(self, sync_db):
        user = User(name="Dave", email="dave@test.com")
        user.insert_sync()
        uid = user.id

        user.delete_sync()
        assert User.get_sync(uid) is None

    def test_find_sync(self, sync_db):
        User(name="A", email="a@test.com", age=20).insert_sync()
        User(name="B", email="b@test.com", age=30).insert_sync()
        User(name="C", email="c@test.com", age=40).insert_sync()

        results = User.find(User.age > 25).to_list_sync()
        assert len(results) == 2
        names = {u.name for u in results}
        assert names == {"B", "C"}

    def test_find_one_sync(self, sync_db):
        User(name="Eve", email="eve@test.com", age=28).insert_sync()

        found = User.find_one_sync(User.name == "Eve")
        assert found is not None
        assert found.email == "eve@test.com"

    def test_count_sync(self, sync_db):
        User(name="A", email="a@test.com").insert_sync()
        User(name="B", email="b@test.com").insert_sync()

        assert User.count_sync() == 2

    def test_insert_many_sync(self, sync_db):
        users = [
            User(name="X", email="x@test.com", age=20),
            User(name="Y", email="y@test.com", age=25),
            User(name="Z", email="z@test.com", age=30),
        ]
        results = User.insert_many_sync(users)
        assert len(results) == 3
        assert all(u.id is not None for u in results)

    def test_sort_sync(self, sync_db):
        User(name="C", email="c@test.com", age=30).insert_sync()
        User(name="A", email="a@test.com", age=10).insert_sync()
        User(name="B", email="b@test.com", age=20).insert_sync()

        results = User.find_all().sort("+name").to_list_sync()
        assert [u.name for u in results] == ["A", "B", "C"]

        results = User.find_all().sort("-age").to_list_sync()
        assert [u.age for u in results] == [30, 20, 10]

    def test_limit_skip_sync(self, sync_db):
        for i in range(10):
            User(name=f"U{i}", email=f"u{i}@test.com", age=i * 10).insert_sync()

        results = User.find_all().sort("+age").limit(3).to_list_sync()
        assert len(results) == 3

        results = User.find_all().sort("+age").skip(2).limit(3).to_list_sync()
        assert len(results) == 3
        assert results[0].age == 20

    def test_optional_field(self, sync_db):
        p = Product(name="Widget", price=9.99)
        p.insert_sync()

        found = Product.get_sync(p.id)
        assert found.category is None
        assert found.in_stock is True

    def test_date_field(self, sync_db):
        e = Event(title="Launch", date=datetime.date(2025, 6, 15))
        e.insert_sync()

        found = Event.get_sync(e.id)
        assert found.date == datetime.date(2025, 6, 15)

    def test_complex_query_sync(self, sync_db):
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

    def test_operators_in_between(self, sync_db):
        for i in range(5):
            User(name=f"U{i}", email=f"u{i}@test.com", age=i * 10).insert_sync()

        results = User.find(In(User.age, [10, 30])).to_list_sync()
        assert len(results) == 2

        results = User.find(Between(User.age, 10, 30)).to_list_sync()
        assert len(results) == 3  # 10, 20, 30

    def test_like_operator(self, sync_db):
        User(name="Alice Smith", email="alice@test.com").insert_sync()
        User(name="Bob Jones", email="bob@test.com").insert_sync()
        User(name="Alicia Keys", email="alicia@test.com").insert_sync()

        results = User.find(Like(User.name, "Ali%")).to_list_sync()
        assert len(results) == 2

    def test_delete_all_sync(self, sync_db):
        User(name="A", email="a@test.com").insert_sync()
        User(name="B", email="b@test.com").insert_sync()
        assert User.count_sync() == 2

        User.delete_all_sync()
        assert User.count_sync() == 0


# ──────────────────────────────────────────────
# Async Tests
# ──────────────────────────────────────────────

class TestAsync:
    @pytest.mark.asyncio
    async def test_insert_and_get(self, async_db):
        user = User(name="Async Alice", email="aa@test.com", age=30)
        await user.insert()
        assert user.id is not None

        found = await User.get(user.id)
        assert found.name == "Async Alice"

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

    @pytest.mark.asyncio
    async def test_field_proxy_methods(self, async_db):
        await User.insert_many([
            User(name="Alice", email="alice@test.com", age=25),
            User(name="Bob", email="bob@test.com", age=30),
            User(name="Alicia", email="alicia@test.com", age=35),
        ])

        # startswith
        results = await User.find(User.name.startswith("Ali")).to_list()
        assert len(results) == 2

        # contains
        results = await User.find(User.name.contains("ob")).to_list()
        assert len(results) == 1
        assert results[0].name == "Bob"

        # is_in
        results = await User.find(User.age.is_in([25, 35])).to_list()
        assert len(results) == 2

        # between
        results = await User.find(User.age.between(26, 34)).to_list()
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_product_with_optional_fields(self, async_db):
        p1 = Product(name="Widget", price=9.99, category="tools")
        p2 = Product(name="Gadget", price=19.99, tags=["cool", "new"])
        await p1.insert()
        await p2.insert()

        found1 = await Product.get(p1.id)
        assert found1.category == "tools"
        assert found1.tags is None

        found2 = await Product.get(p2.id)
        assert found2.tags == ["cool", "new"]

    @pytest.mark.asyncio
    async def test_transaction(self, async_db):
        session = get_session()
        async with session.async_transaction():
            await User(name="TxUser", email="tx@test.com", age=50).insert()

        found = await User.find_one(User.name == "TxUser")
        assert found is not None

    @pytest.mark.asyncio
    async def test_not_initialized_error(self):
        """Test that operations fail before init."""
        DucklingSession.reset()
        with pytest.raises(NotInitializedError):
            await User.find_all().to_list()

    @pytest.mark.asyncio
    async def test_boolean_operators(self, async_db):
        await User.insert_many([
            User(name="A", email="a@test.com", age=20, active=True),
            User(name="B", email="b@test.com", age=30, active=False),
            User(name="C", email="c@test.com", age=40, active=True),
        ])

        # Using operator functions
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
