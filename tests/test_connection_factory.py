"""Tests for resolving a connection per call via `use_connection_factory`.

The motivating case: a host application that hands out a DuckDB connection per
thread. A single connection driven from two threads segfaults the interpreter,
so holding one handle confines Duckling to a single thread.
"""

import threading

import duckdb
import pytest

from duckling import DucklingSession, get_session, init_duckling_sync
from duckling.exceptions import NotInitializedError

from .models import User


@pytest.fixture
def owner_connection(tmp_path):
    """A connection owned by the host application, not by Duckling."""
    connection = duckdb.connect(str(tmp_path / "host.db"))
    yield connection
    connection.close()


class TestConnectionFactory:
    def test_factory_is_consulted_on_every_access(self, owner_connection):
        calls = []

        def factory():
            calls.append(1)
            return owner_connection

        init_duckling_sync(connection_factory=factory, document_models=[User])

        User(name="A", email="a@test.com", age=1).insert_sync()
        User.count_sync()

        # Not cached: each query resolved the connection again.
        assert len(calls) > 1

    def test_each_thread_gets_its_own_cursor(self, owner_connection):
        """The point of the feature — mirrors how a host hands out cursors."""
        local = threading.local()

        def factory():
            cursor = getattr(local, "cursor", None)
            if cursor is None:
                cursor = owner_connection.cursor()
                local.cursor = cursor
            return cursor

        init_duckling_sync(connection_factory=factory, document_models=[User])

        errors: list[str] = []

        def hammer(n: int):
            try:
                for i in range(50):
                    User(name=f"user-{n}-{i}", email=f"u{n}-{i}@test.com", age=i).insert_sync()
                    User.count_sync()
            except Exception as exc:  # noqa: BLE001 - collected for the assertion
                errors.append(f"{type(exc).__name__}: {exc}")

        threads = [threading.Thread(target=hammer, args=(n,)) for n in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == []
        assert User.count_sync() == 300

    def test_writes_through_one_thread_are_visible_to_another(self, owner_connection):
        def factory():
            return owner_connection.cursor()

        init_duckling_sync(connection_factory=factory, document_models=[User])

        def insert():
            User(name="Threaded", email="threaded@test.com", age=7).insert_sync()

        thread = threading.Thread(target=insert)
        thread.start()
        thread.join()

        assert User.find_one_sync(User.name == "Threaded") is not None

    def test_reset_does_not_close_a_factory_connection(self, owner_connection):
        """The host still owns it — closing would pull the rug out."""
        init_duckling_sync(connection_factory=lambda: owner_connection, document_models=[User])

        DucklingSession.reset()

        # Still usable by its owner after Duckling let go.
        assert owner_connection.execute("SELECT 1").fetchone() == (1,)

    def test_close_does_not_close_a_factory_connection(self, owner_connection):
        init_duckling_sync(connection_factory=lambda: owner_connection, document_models=[User])

        get_session().close()

        assert owner_connection.execute("SELECT 1").fetchone() == (1,)

    def test_factory_takes_precedence_over_connection(self, owner_connection, tmp_path):
        unused = duckdb.connect(str(tmp_path / "unused.db"))
        try:
            init_duckling_sync(
                connection=unused,
                connection_factory=lambda: owner_connection,
                document_models=[User],
            )
            assert get_session().connection is owner_connection
        finally:
            unused.close()

    def test_a_factory_returning_none_is_reported(self):
        init_duckling_sync(connection_factory=lambda: None)

        with pytest.raises(NotInitializedError):
            get_session().connection

    def test_use_connection_clears_a_previous_factory(self, owner_connection, tmp_path):
        direct = duckdb.connect(str(tmp_path / "direct.db"))
        try:
            session = get_session()
            session.use_connection_factory(lambda: owner_connection)
            session.use_connection(direct)

            assert session.connection is direct
        finally:
            direct.close()
