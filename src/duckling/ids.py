"""ID generation helpers for custom primary keys."""

import os
import time

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def generate_ulid() -> str:
    """
    Generate a 26-character ULID (48-bit ms timestamp + 80-bit randomness),
    lexicographically sortable by creation time.

    Usage:
        class Session(Document):
            id: str = Field(default_factory=generate_ulid)
    """
    ms = int(time.time() * 1000)
    randomness = int.from_bytes(os.urandom(10), "big")
    value = (ms << 80) | randomness

    chars = []
    for _ in range(26):
        value, rem = divmod(value, 32)
        chars.append(_CROCKFORD[rem])
    return "".join(reversed(chars))
