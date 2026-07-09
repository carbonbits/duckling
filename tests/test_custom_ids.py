"""Tests for custom (non auto-increment) primary keys — str, UUID, ULID."""

import uuid

import pytest

from duckling import DocumentAlreadyExists, get_session

from .models import ApiKey, Session, Tag


class TestSyncCustomIds:
    def test_ulid_id_is_generated_and_unique(self, sync_db):
        a = Session(user_email="a@test.com")
        b = Session(user_email="b@test.com")
        a.insert_sync()
        b.insert_sync()

        assert isinstance(a.id, str) and len(a.id) == 26
        assert a.id != b.id

    def test_uuid_id_round_trip(self, sync_db):
        key = ApiKey(label="prod")
        key.insert_sync()
        assert isinstance(key.id, uuid.UUID)

        found = ApiKey.get_sync(key.id)
        assert found is not None
        assert found.label == "prod"

    def test_caller_supplied_id_round_trip(self, sync_db):
        tag = Tag(id="billing", name="Billing")
        tag.insert_sync()

        found = Tag.get_sync("billing")
        assert found is not None
        assert found.name == "Billing"

    def test_duplicate_caller_supplied_id_raises(self, sync_db):
        Tag(id="dup", name="First").insert_sync()
        with pytest.raises(DocumentAlreadyExists):
            Tag(id="dup", name="Second").insert_sync()

    def test_no_sequence_created_for_string_id_model(self, sync_db):
        session = get_session()
        rows = session.fetchall(
            "SELECT sequence_name FROM duckdb_sequences() WHERE sequence_name = ?",
            ["seq_tags_id"],
        )
        assert rows == []


class TestAsyncCustomIds:
    @pytest.mark.asyncio
    async def test_ulid_id_is_generated_and_unique(self, async_db):
        a = Session(user_email="a@test.com")
        b = Session(user_email="b@test.com")
        await a.insert()
        await b.insert()

        assert isinstance(a.id, str) and len(a.id) == 26
        assert a.id != b.id

    @pytest.mark.asyncio
    async def test_duplicate_caller_supplied_id_raises(self, async_db):
        await Tag(id="dup", name="First").insert()
        with pytest.raises(DocumentAlreadyExists):
            await Tag(id="dup", name="Second").insert()

    @pytest.mark.asyncio
    async def test_recreate_tables_with_mixed_pk_types(self):
        from duckling import get_session, init_duckling

        from .models import ALL_MODELS

        session = await init_duckling(database=":memory:", document_models=ALL_MODELS)
        conn = session.connection

        # Reuse the same live connection so recreate_tables actually drops
        # and rebuilds tables/sequences that exist, instead of a fresh :memory: db.
        await init_duckling(
            connection=conn, document_models=ALL_MODELS, recreate_tables=True
        )
        assert get_session().connection is conn
