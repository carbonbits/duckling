"""
Tests for fields whose database column differs from the Python attribute.

`Field(alias=...)` renames the column while the attribute keeps its name, so
`self.id` can address a primary key column called something else entirely.
"""

import pytest
from pydantic import Field

from duckling import Avg, Count, Document, InvalidQueryError, get_session

from .models import Machine, Ticket


def table_columns(table: str) -> list[str]:
    session = get_session()
    rows = session.fetchall(
        "SELECT column_name FROM duckdb_columns() WHERE table_name = ?", [table]
    )
    return [row[0] for row in rows]


class TestSchema:
    def test_pk_column_uses_the_alias(self, sync_db):
        columns = table_columns("ticket")

        assert "key" in columns
        assert "id" not in columns

    def test_regular_field_column_uses_the_alias(self, sync_db):
        columns = table_columns("ticket")

        assert "prio" in columns
        assert "priority" not in columns

    def test_pk_column_is_the_primary_key(self, sync_db):
        session = get_session()
        rows = session.fetchall(
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE table_name = 'ticket' AND constraint_type = 'PRIMARY KEY'"
        )
        assert rows == [(["key"],)]

    def test_sequence_is_named_after_the_pk_column(self, sync_db):
        session = get_session()
        rows = session.fetchall(
            "SELECT sequence_name FROM duckdb_sequences() WHERE sequence_name = ?",
            ["seq_machine_machine_id"],
        )
        assert rows == [("seq_machine_machine_id",)]

    def test_index_is_created_on_the_aliased_column(self, sync_db):
        session = get_session()
        rows = session.fetchall(
            "SELECT index_name, sql FROM duckdb_indexes() WHERE table_name = 'machine'"
        )

        assert [row[0] for row in rows] == ["idx_machine_host_name"]
        assert "(host_name)" in rows[0][1]

    def test_unique_constraint_uses_the_aliased_column(self, sync_db):
        session = get_session()
        rows = session.fetchall(
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE table_name = 'machine' AND constraint_type = 'UNIQUE'"
        )
        assert rows == [(["host_name"],)]


class TestConstruction:
    def test_populates_by_field_name(self, sync_db):
        ticket = Ticket(subject="Printer down", priority=2)

        assert ticket.subject == "Printer down"
        assert ticket.priority == 2

    def test_populates_by_alias(self, sync_db):
        ticket = Ticket(key="abc", subject="Printer down", prio=3)

        assert ticket.id == "abc"
        assert ticket.priority == 3


class TestCrud:
    def test_insert_and_get_by_id(self, sync_db):
        ticket = Ticket(subject="Printer down", priority=2)
        ticket.insert_sync()

        found = Ticket.get_sync(ticket.id)
        assert found is not None
        assert found.id == ticket.id
        assert found.subject == "Printer down"
        assert found.priority == 2

    def test_auto_increment_pk_is_assigned(self, sync_db):
        first = Machine(hostname="alpha")
        second = Machine(hostname="beta")
        first.insert_sync()
        second.insert_sync()

        assert first.id == 1
        assert second.id == 2
        assert Machine.get_sync(2).hostname == "beta"

    def test_save_updates_by_pk_column(self, sync_db):
        ticket = Ticket(subject="Printer down", priority=1)
        ticket.insert_sync()

        ticket.priority = 5
        ticket.save_sync()

        assert Ticket.get_sync(ticket.id).priority == 5

    def test_delete_by_pk_column(self, sync_db):
        ticket = Ticket(subject="Printer down")
        ticket.insert_sync()
        ticket.delete_sync()

        assert Ticket.get_sync(ticket.id) is None
        assert Ticket.count_sync() == 0

    @pytest.mark.asyncio
    async def test_async_round_trip(self, async_db):
        ticket = Ticket(subject="Async", priority=7)
        await ticket.insert()

        found = await Ticket.get(ticket.id)
        assert found.priority == 7

        found.priority = 8
        await found.save()
        await ticket.refresh()
        assert ticket.priority == 8

        await ticket.delete()
        assert await Ticket.get(ticket.id) is None


class TestQueries:
    @pytest.fixture
    def tickets(self, sync_db):
        Ticket(id="a", subject="Low", priority=1).insert_sync()
        Ticket(id="b", subject="High", priority=9).insert_sync()
        return sync_db

    def test_filter_on_aliased_field(self, tickets):
        results = Ticket.find(Ticket.priority > 5).to_list_sync()

        assert len(results) == 1
        assert results[0].subject == "High"

    def test_filter_on_aliased_pk(self, tickets):
        results = Ticket.find(Ticket.id == "a").to_list_sync()

        assert len(results) == 1
        assert results[0].subject == "Low"

    def test_sort_by_field_name(self, tickets):
        results = Ticket.find_all().sort("-priority").to_list_sync()

        assert [t.subject for t in results] == ["High", "Low"]

    def test_sort_by_column_name(self, tickets):
        results = Ticket.find_all().sort("-prio").to_list_sync()

        assert [t.subject for t in results] == ["High", "Low"]

    def test_sort_by_field_proxy(self, tickets):
        results = Ticket.find_all().sort(Ticket.priority.desc()).to_list_sync()

        assert [t.subject for t in results] == ["High", "Low"]

    def test_projection_by_field_name(self, tickets):
        results = (
            Ticket.find(Ticket.id == "a").project("id", "subject", "priority")
        ).to_list_sync()

        assert results[0].subject == "Low"
        assert results[0].id == "a"

    @pytest.mark.asyncio
    async def test_bulk_update_by_field_name(self, async_db):
        await Ticket(id="a", subject="Low", priority=1).insert()

        await Ticket.find(Ticket.id == "a").update({"priority": 4})

        assert (await Ticket.get("a")).priority == 4

    @pytest.mark.asyncio
    async def test_aggregate_over_aliased_field(self, async_db):
        await Ticket(id="a", subject="Low", priority=1).insert()
        await Ticket(id="b", subject="High", priority=9).insert()

        stats = await Ticket.find_all().aggregate(
            avg_priority=Avg("priority"), total=Count()
        )

        assert stats["avg_priority"] == 5
        assert stats["total"] == 2

    @pytest.mark.asyncio
    async def test_aggregate_by_column_name(self, async_db):
        await Ticket(id="a", subject="Low", priority=1).insert()

        stats = await Ticket.find_all().aggregate(avg_priority=Avg("prio"))

        assert stats["avg_priority"] == 1


class TestValidation:
    def test_alias_colliding_with_another_field_is_rejected(self):
        with pytest.raises(InvalidQueryError, match="both map to column 'name'"):

            class Colliding(Document):
                name: str
                label: str = Field(alias="name")

    def test_alias_colliding_with_another_alias_is_rejected(self):
        with pytest.raises(InvalidQueryError, match="both map to column 'slug'"):

            class AlsoColliding(Document):
                first: str = Field(alias="slug")
                second: str = Field(alias="slug")
