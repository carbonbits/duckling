"""Shared document models used across the test suite."""

import datetime
from typing import Annotated, List, Optional

from duckling import Document, IndexSpec


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


ALL_MODELS = [User, Product, Event, AutoNamed]
