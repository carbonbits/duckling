"""Tests for duckling.fields — field types, optionals, and FieldProxy methods."""

import datetime

import pytest

from .models import Event, Product, User


class TestSyncFields:
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


class TestAsyncFields:
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
