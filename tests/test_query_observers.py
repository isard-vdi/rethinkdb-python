"""Unit tests for Connection.add_query_observer / remove_query_observer.

The hook firing logic is tested at the registry level (no real query path)
so we don't need a live server. Integration tests cover the
end-to-end firing in tests/integration/.
"""

import logging
from unittest.mock import Mock

import pytest

from rethinkdb.net import Connection


def _bare_connection() -> Connection:
    """Build a Connection without going through __init__ (which opens TCP)."""
    conn = Connection.__new__(Connection)
    conn._on_query_start = []
    conn._on_query_end = []
    return conn


@pytest.mark.unit
class TestQueryObserverRegistry:
    def test_add_both_hooks(self):
        conn = _bare_connection()
        on_start = Mock()
        on_end = Mock()
        token = conn.add_query_observer(on_start=on_start, on_end=on_end)
        assert conn._on_query_start == [on_start]
        assert conn._on_query_end == [on_end]
        assert token == (on_start, on_end)

    def test_add_only_start(self):
        conn = _bare_connection()
        on_start = Mock()
        conn.add_query_observer(on_start=on_start)
        assert conn._on_query_start == [on_start]
        assert conn._on_query_end == []

    def test_add_only_end(self):
        conn = _bare_connection()
        on_end = Mock()
        conn.add_query_observer(on_end=on_end)
        assert conn._on_query_start == []
        assert conn._on_query_end == [on_end]

    def test_add_no_args_is_no_op(self):
        conn = _bare_connection()
        token = conn.add_query_observer()
        assert conn._on_query_start == []
        assert conn._on_query_end == []
        assert token == (None, None)

    def test_multiple_observers_run_in_order(self):
        conn = _bare_connection()
        order = []
        conn.add_query_observer(
            on_start=lambda q: order.append("a"),
            on_end=lambda q, d, e: order.append("end-a"),
        )
        conn.add_query_observer(
            on_start=lambda q: order.append("b"),
            on_end=lambda q, d, e: order.append("end-b"),
        )
        conn._fire_query_start(Mock())
        conn._fire_query_end(Mock(), 0.01, None)
        assert order == ["a", "b", "end-a", "end-b"]

    def test_remove_observer_round_trips(self):
        conn = _bare_connection()
        on_start = Mock()
        on_end = Mock()
        token = conn.add_query_observer(on_start=on_start, on_end=on_end)
        conn.remove_query_observer(token)
        assert conn._on_query_start == []
        assert conn._on_query_end == []

    def test_remove_idempotent(self):
        conn = _bare_connection()
        on_start = Mock()
        token = conn.add_query_observer(on_start=on_start)
        conn.remove_query_observer(token)
        # Removing twice must not raise.
        conn.remove_query_observer(token)


@pytest.mark.unit
class TestQueryObserverFiring:
    def test_fire_start_calls_each_observer_with_query(self):
        conn = _bare_connection()
        on_start = Mock()
        conn.add_query_observer(on_start=on_start)
        q = Mock()
        conn._fire_query_start(q)
        on_start.assert_called_once_with(q)

    def test_fire_end_passes_duration_and_exception(self):
        conn = _bare_connection()
        on_end = Mock()
        conn.add_query_observer(on_end=on_end)
        q = Mock()
        exc = ValueError("boom")
        conn._fire_query_end(q, 0.123, exc)
        on_end.assert_called_once_with(q, 0.123, exc)

    def test_observer_exception_does_not_propagate(self, caplog):
        conn = _bare_connection()

        def crashy(query):
            raise RuntimeError("observer crashed")

        good = Mock()
        conn.add_query_observer(on_start=crashy)
        conn.add_query_observer(on_start=good)

        with caplog.at_level(logging.WARNING):
            conn._fire_query_start(Mock())

        # Subsequent observers still run after a misbehaving one.
        good.assert_called_once()
        assert any(
            "Query start observer raised" in record.message for record in caplog.records
        )

    def test_end_observer_exception_does_not_propagate(self, caplog):
        conn = _bare_connection()

        def crashy(query, duration, exception):
            raise RuntimeError("end observer crashed")

        good = Mock()
        conn.add_query_observer(on_end=crashy)
        conn.add_query_observer(on_end=good)

        with caplog.at_level(logging.WARNING):
            conn._fire_query_end(Mock(), 0.5, None)

        good.assert_called_once()
        assert any(
            "Query end observer raised" in record.message for record in caplog.records
        )
