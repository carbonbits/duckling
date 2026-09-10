"""Async iterator over FindQuery results."""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .find_query import FindQuery


class FindQueryIterator:
    """Async iterator for FindQuery results."""

    def __init__(self, query: "FindQuery") -> None:
        self._query = query
        self._results: Optional[list] = None
        self._index = 0

    async def __anext__(self):
        if self._results is None:
            self._results = await self._query.to_list()
        if self._index >= len(self._results):
            raise StopAsyncIteration
        item = self._results[self._index]
        self._index += 1
        return item
