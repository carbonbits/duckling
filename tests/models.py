"""Shared document models used across the test suite."""

import datetime
import uuid
from typing import Annotated, List, Optional

from pydantic import Field

from duckling import Document, IndexSpec, generate_ulid


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


class Session(Document):
    """Custom string primary key generated with a ULID."""

    id: str = Field(default_factory=generate_ulid)
    user_email: str


class ApiKey(Document):
    """Custom UUID primary key."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    label: str


class Tag(Document):
    """Custom, caller-supplied string primary key (no default)."""

    id: str
    name: str


class LooseTag(Document):
    """Non-integer primary key with no default — the caller must supply one."""

    id: Optional[str] = None
    name: str


class Ticket(Document):
    """Primary key and a regular field stored under renamed columns."""

    id: str = Field(default_factory=generate_ulid, alias="key")
    subject: str
    priority: int = Field(default=0, alias="prio")


class Machine(Document):
    """Auto-increment primary key stored in a column named "machine_id"."""

    id: Optional[int] = Field(default=None, alias="machine_id")
    hostname: Annotated[str, IndexSpec(unique=True)] = Field(alias="host_name")


class Role(Document):
    """Schema-qualified table with an auto-increment primary key."""

    name: Annotated[str, IndexSpec(unique=True)]
    display_name: Optional[str] = None

    class Settings:
        schema_name = "v1"
        table_name = "roles"


class Permission(Document):
    """A second model in the same non-default schema."""

    code: str

    class Settings:
        schema_name = "v1"


class ArchivedRole(Document):
    """Same table name as `Role`, in a different schema."""

    name: Annotated[str, IndexSpec(unique=True)]

    class Settings:
        schema_name = "v2"
        table_name = "roles"


ALL_MODELS = [
    User,
    Product,
    Event,
    AutoNamed,
    Session,
    ApiKey,
    Tag,
    LooseTag,
    Ticket,
    Machine,
    Role,
    Permission,
    ArchivedRole,
]
