"""Asyncio integration tests.

These tests require a live RethinkDB server (default localhost:28015) and
are skipped automatically when one isn't reachable. They exercise the
asyncio backend end-to-end: connect, create table, insert, cursor iteration.
"""

import os

import pytest

from rethinkdb import r

INTEGRATION_TEST_DB = "integration_test"


@pytest.fixture
async def asyncio_conn():
    """Per-test asyncio connection against the configured RethinkDB host."""
    r.set_loop_type("asyncio")
    host = os.getenv("RETHINKDB_HOST", "127.0.0.1")
    conn = await r.connect(host=host)
    try:
        existing = await r.db_list().run(conn)
        if INTEGRATION_TEST_DB not in existing:
            await r.db_create(INTEGRATION_TEST_DB).run(conn)
        conn.use(INTEGRATION_TEST_DB)
        yield conn
    finally:
        try:
            await r.db_drop(INTEGRATION_TEST_DB).run(conn)
        except Exception:
            # Database may already be gone; teardown should not mask test errors.
            pass
        await conn.close()
        r.set_loop_type(None)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_insert_and_iterate(asyncio_conn):
    table_name = "test_asyncio"
    await r.table_create(table_name).run(asyncio_conn)

    table = r.table(table_name)
    await table.insert(
        {"id": 1, "name": "Iron Man", "first_appearance": "Tales of Suspense #39"}
    ).run(asyncio_conn)

    cursor = await table.run(asyncio_conn)
    seen = []
    async for hero in cursor:
        seen.append(hero["name"])

    assert seen == ["Iron Man"]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_pool_acquire_release(asyncio_conn):
    """End-to-end smoke test that AsyncioConnectionPool works with a real server."""
    host = os.getenv("RETHINKDB_HOST", "127.0.0.1")
    pool = r.create_pool(host=host, db=INTEGRATION_TEST_DB, max_size=3)

    try:
        async with pool.connection() as conn:
            value = await r.expr(42).run(conn)
            assert value == 42

        # Connection should be returned to the pool, not closed.
        assert pool.in_use == 0
        assert pool.idle == 1

        # Acquire again and verify reuse.
        async with pool.connection() as conn2:
            assert pool.in_use == 1
            assert await r.expr("hello").run(conn2) == "hello"
    finally:
        await pool.close()
        assert pool.closed
