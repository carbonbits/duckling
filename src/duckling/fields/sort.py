"""Sort direction."""

from __future__ import annotations

import enum


class SortDirection(enum.IntEnum):
    ASCENDING = 1
    DESCENDING = -1
