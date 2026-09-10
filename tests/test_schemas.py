"""
Tests for `Settings.schema_name` — placing a model's table in a named schema.

Every identifier Duckling renders is quoted independently, so the schema and
table are separate names rather than one string the caller has to pre-qualify.
"""

import pytest

from duckling import Document, InvalidQueryError, get_session

from .models import ArchivedRole, Permission, Role


class TestNameResolution:
    def test_schema_is_read_from_settings(self):
        assert Role._get_schema_name() == "v1"
        assert Role._get_table_name() == "roles"
        assert Role._get_qualified_table_name() == '"v1"."roles"'

    def test_table_name_still_auto_generates_inside_a_schema(self):
        assert Permission._get_table_name() == "permission"
        assert Permission._get_qualified_table_name() == '"v1"."permission"'

    def test_no_schema_yields_a_bare_quoted_table(self):
        class Plain(Document):
            value: str

        assert Plain._get_schema_name() is None
        assert Plain._get_qualified_table_name() == '"plain"'

    def test_sequence_is_qualified_alongside_its_table(self):
        assert Role._get_sequence_name() == "seq_roles_id"
        assert Role._get_qualified_sequence_name() == '"v1"."seq_roles_id"'

    def test_generated_sql_never_emits_an_unqualified_reference(self):
        assert '"v1"."roles"' in Role._build_create_table_sql()
        assert Role._build_create_schema_sql() == 'CREATE SCHEMA IF NOT EXISTS "v1"'
        assert '"v1"."seq_roles_id"' in Role._build_sequence_sql()

    def test_default_schema_needs_no_create_schema(self):
        class Plain(Document):
            value: str

        assert Plain._build_create_schema_sql() is None


class TestQualifiedNameRejection:
    """`Settings` values are single identifiers, so a pre-qualified name is a bug."""

    def test_dotted_table_name_is_rejected(self):
        with pytest.raises(InvalidQueryError, match="single identifiers"):

            class Dotted(Document):
                name: str

                class Settings:
                    table_name = "v1.roles"

    def test_the_embedded_quote_hack_is_rejected(self):
        with pytest.raises(InvalidQueryError, match="single identifiers"):

            class Hacked(Document):
                name: str

                class Settings:
                    table_name = 'v1"."roles'

    def test_dotted_schema_name_is_rejected(self):
        with pytest.raises(InvalidQueryError, match="single identifiers"):

            class DottedSchema(Document):
                name: str

                class Settings:
                    schema_name = "a.b"


class TestSchemaCreation:
    def test_schema_and_table_are_created(self, sync_db):
        rows = get_session().fetchall(
            "SELECT schema_name, table_name FROM duckdb_tables() "
            "WHERE table_name IN ('roles', 'permission') ORDER BY schema_name, table_name"
        )

        assert rows == [("v1", "permission"), ("v1", "roles"), ("v2", "roles")]

    def test_index_lands_in_the_tables_schema(self, sync_db):
        rows = get_session().fetchall(
            "SELECT schema_name, table_name FROM duckdb_indexes() "
            "WHERE index_name = 'idx_roles_name' ORDER BY schema_name"
        )

        assert rows == [("v1", "roles"), ("v2", "roles")]


class TestCrudInsideASchema:
    def test_insert_and_get(self, sync_db):
        role = Role(name="admin", display_name="Administrator").insert_sync()

        assert role.id == 1
        fetched = Role.get_sync(role.id)
        assert fetched is not None
        assert fetched.name == "admin"
        assert fetched.display_name == "Administrator"

    def test_sequence_drives_the_primary_key(self, sync_db):
        first = Role(name="a").insert_sync()
        second = Role(name="b").insert_sync()

        assert [first.id, second.id] == [1, 2]

    def test_find_update_and_delete(self, sync_db):
        Role(name="admin").insert_sync()
        Role(name="viewer").insert_sync()

        assert Role.find(Role.name == "admin").to_list_sync()[0].name == "admin"
        assert Role.count_sync() == 2

        Role.find(Role.name == "viewer").to_list_sync()[0].delete_sync()
        assert Role.count_sync() == 1

        role = Role.find_one_sync(Role.name == "admin")
        role.display_name = "Root"
        role.save_sync()
        assert Role.get_sync(role.id).display_name == "Root"

    def test_unique_index_is_enforced(self, sync_db):
        from duckling import DocumentAlreadyExists

        Role(name="admin").insert_sync()
        with pytest.raises(DocumentAlreadyExists):
            Role(name="admin").insert_sync()

    def test_same_table_name_in_two_schemas_stays_separate(self, sync_db):
        Role(name="admin").insert_sync()
        ArchivedRole(name="admin").insert_sync()
        ArchivedRole(name="legacy").insert_sync()

        assert Role.count_sync() == 1
        assert ArchivedRole.count_sync() == 2
        assert sorted(r.name for r in ArchivedRole.find_all().to_list_sync()) == [
            "admin",
            "legacy",
        ]

    def test_delete_all_is_scoped_to_one_schema(self, sync_db):
        Role(name="admin").insert_sync()
        ArchivedRole(name="admin").insert_sync()

        Role.delete_all_sync()

        assert Role.count_sync() == 0
        assert ArchivedRole.count_sync() == 1

    def test_projection_and_sort_are_qualified(self, sync_db):
        Role(name="b").insert_sync()
        Role(name="a").insert_sync()

        names = [r.name for r in Role.find_all().sort("+name").to_list_sync()]
        assert names == ["a", "b"]

        projected = Role.find_all().project("name").to_list_sync()
        assert sorted(r.name for r in projected) == ["a", "b"]

    @pytest.mark.asyncio
    async def test_aggregate_is_qualified(self, async_db):
        from duckling import Count

        await Role(name="a").insert()
        await Role(name="b").insert()

        assert (await Role.find_all().aggregate(total=Count()))["total"] == 2

    @pytest.mark.asyncio
    async def test_async_crud(self, async_db):
        role = await Role(name="admin").insert()

        fetched = await Role.get(role.id)
        assert fetched.name == "admin"
        assert await Role.count() == 1

        await role.delete()
        assert await Role.count() == 0


class TestRecreateTables:
    def test_recreate_drops_and_rebuilds_in_the_schema(self):
        from duckling import init_duckling_sync

        from .models import ALL_MODELS

        init_duckling_sync(database=":memory:", document_models=ALL_MODELS)
        Role(name="admin").insert_sync()
        assert Role.count_sync() == 1

        init_duckling_sync(
            database=":memory:", document_models=ALL_MODELS, recreate_tables=True
        )
        assert Role.count_sync() == 0
