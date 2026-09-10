"""DocumentMeta — the metaclass behind class-level field access."""

from pydantic import BaseModel

from duckling.exceptions import InvalidQueryError
from duckling.fields import FieldProxy


class DocumentMeta(type(BaseModel)):
    """
    Metaclass for Document that installs FieldProxy descriptors on the class,
    enabling `User.name == "Alice"` style query expressions.
    """

    def __new__(mcs, name: str, bases: tuple, namespace: dict, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        # Skip the base Document class itself
        if name == "Document" and not any(
            hasattr(b, "_is_duckling_document") for b in bases
        ):
            cls._is_duckling_document = True
            return cls

        # For every model field, create a FieldProxy accessible on the class.
        # The proxy carries the *column* name, so every expression, sort helper
        # and operator built from it renders the right identifier for free.
        cls._field_proxies = {}
        for field_name, field_info in cls.model_fields.items():
            proxy = FieldProxy(field_info.alias or field_name, field_info.annotation)
            cls._field_proxies[field_name] = proxy

        mcs._check_column_names(cls)
        return cls

    @staticmethod
    def _check_column_names(cls) -> None:
        """
        Reject a model whose fields do not map onto distinct columns.

        An alias can collide with another field's name (or another alias), which
        would otherwise produce a table with two identically named columns.
        """
        seen: dict[str, str] = {}
        for field_name, field_info in cls.model_fields.items():
            column = field_info.alias or field_name
            if column in seen:
                raise InvalidQueryError(
                    f"{cls.__name__}: fields {seen[column]!r} and {field_name!r} "
                    f"both map to column {column!r}. Change one of the aliases."
                )
            seen[column] = field_name

    def __getattr__(cls, name: str):
        # Return FieldProxy for query building when accessing fields on the class
        if name.startswith("_") or name == "model_fields":
            raise AttributeError(name)
        proxies = cls.__dict__.get("_field_proxies", {})
        if name in proxies:
            return proxies[name]
        raise AttributeError(
            f"type object {cls.__name__!r} has no attribute {name!r}"
        )
