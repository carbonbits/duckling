"""The Document base class and the metaclass that powers class-level fields."""

from duckling.document.base import Document
from duckling.document.meta import DocumentMeta

__all__ = ["Document", "DocumentMeta"]
