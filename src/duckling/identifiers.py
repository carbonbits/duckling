"""
SQL identifier quoting.

Every table, schema, sequence and column name Duckling renders goes through
`quote_ident`, so a name containing a quote, a dot or a reserved word survives
into SQL as the single identifier it was meant to be.
"""


def quote_ident(name: str) -> str:
    """
    Quote a single SQL identifier, escaping any embedded double quotes.

        quote_ident("roles")     -> '"roles"'
        quote_ident('we"ird')    -> '"we""ird"'

    The result is always exactly one identifier: a name that happens to contain
    a `"` or a `.` cannot escape its quotes to become a qualified reference.
    """
    return '"' + name.replace('"', '""') + '"'


def qualified_name(schema: str | None, name: str) -> str:
    """
    Render a possibly schema-qualified reference, quoting both parts.

        qualified_name(None, "roles")   -> '"roles"'
        qualified_name("v1", "roles")   -> '"v1"."roles"'
    """
    if schema:
        return f"{quote_ident(schema)}.{quote_ident(name)}"
    return quote_ident(name)
