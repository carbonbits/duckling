"""The Document base class and the metaclass that powers class-level fields."""

from .base import Document
from .meta import DocumentMeta

__all__ = ["Document", "DocumentMeta"]
