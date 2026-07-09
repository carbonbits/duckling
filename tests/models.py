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


ALL_MODELS = [User, Product, Event, AutoNamed, Session, ApiKey, Tag]
