"""
Tests for custom string primary key support in Duckling.
Verifies that Document subclasses can declare their own id type
(str, UUID, or any non-integer) and duckling respects it fully.
"""
from __future__ import annotations

import uuid
import pytest
from pydantic import Field
from duckling import Document, init_duckling_sync
from duckling.connection import DucklingSession


@pytest.fixture(autouse=True)
def reset_session():
    DucklingSession.reset()
    yield
    DucklingSession.reset()


# --- Models ---

class FarmDoc(Document):
    """String ULID-style primary key — simulates farmdb pattern."""
    id: str = Field(default_factory=lambda: f"farm_{uuid.uuid4().hex[:8]}")
    name: str
    org_id: str

    class Settings:
        table_name = "farms"


class UUIDDoc(Document):
    """UUID primary key."""
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    title: str

    class Settings:
        table_name = "uuid_docs"


class IntDoc(Document):
    """Default auto-increment integer id — existing behavior unchanged."""
    name: str

    class Settings:
        table_name = "int_docs"


# --- String ID tests ---

def test_string_id_table_creates_without_sequence():
    init_duckling_sync(database=":memory:", document_models=[FarmDoc])
    farm = FarmDoc(name="Sunrise Farm", org_id="org-123")
    farm.insert_sync()
    assert farm.id.startswith("farm_")
    assert len(farm.id) > 0


def test_string_id_insert_and_get_sync():
    init_duckling_sync(database=":memory:", document_models=[FarmDoc])
    farm = FarmDoc(name="North Farm", org_id="org-abc")
    farm.insert_sync()

    fetched = FarmDoc.get_sync(farm.id)
    assert fetched is not None
    assert fetched.name == "North Farm"
    assert fetched.org_id == "org-abc"
    assert fetched.id == farm.id


def test_string_id_find_sync():
    init_duckling_sync(database=":memory:", document_models=[FarmDoc])
    FarmDoc(name="Farm A", org_id="org-1").insert_sync()
    FarmDoc(name="Farm B", org_id="org-1").insert_sync()
    FarmDoc(name="Farm C", org_id="org-2").insert_sync()

    results = FarmDoc.find(FarmDoc.org_id == "org-1").to_list_sync()
    assert len(results) == 2
    names = {r.name for r in results}
    assert names == {"Farm A", "Farm B"}


def test_string_id_save_updates_sync():
    init_duckling_sync(database=":memory:", document_models=[FarmDoc])
    farm = FarmDoc(name="Old Name", org_id="org-1")
    farm.insert_sync()

    farm.name = "New Name"
    farm.save_sync()

    fetched = FarmDoc.get_sync(farm.id)
    assert fetched.name == "New Name"


def test_string_id_delete_sync():
    init_duckling_sync(database=":memory:", document_models=[FarmDoc])
    farm = FarmDoc(name="To Delete", org_id="org-1")
    farm.insert_sync()
    farm.delete_sync()

    fetched = FarmDoc.get_sync(farm.id)
    assert fetched is None


def test_string_id_count_sync():
    init_duckling_sync(database=":memory:", document_models=[FarmDoc])
    FarmDoc(name="Farm A", org_id="org-1").insert_sync()
    FarmDoc(name="Farm B", org_id="org-1").insert_sync()
    assert FarmDoc.count_sync() == 2


# --- UUID ID tests ---

def test_uuid_id_insert_and_get_sync():
    init_duckling_sync(database=":memory:", document_models=[UUIDDoc])
    doc = UUIDDoc(title="Test Doc")
    doc.insert_sync()

    assert isinstance(doc.id, uuid.UUID)
    fetched = UUIDDoc.get_sync(doc.id)
    assert fetched is not None
    assert fetched.title == "Test Doc"
    assert fetched.id == doc.id


# --- Integer ID unchanged ---

def test_integer_id_behavior_unchanged():
    init_duckling_sync(database=":memory:", document_models=[IntDoc])
    doc = IntDoc(name="Test")
    doc.insert_sync()

    assert isinstance(doc.id, int)
    assert doc.id >= 1

    fetched = IntDoc.get_sync(doc.id)
    assert fetched is not None
    assert fetched.name == "Test"


def test_integer_and_string_id_models_coexist():
    init_duckling_sync(
        database=":memory:",
        document_models=[FarmDoc, IntDoc]
    )
    farm = FarmDoc(name="Mixed Farm", org_id="org-1")
    farm.insert_sync()
    assert isinstance(farm.id, str)

    int_doc = IntDoc(name="Mixed Int")
    int_doc.insert_sync()
    assert isinstance(int_doc.id, int)
