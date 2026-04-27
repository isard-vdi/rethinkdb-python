"""Unit tests for rethinkdb.connection_pool — no live database required.

Both pool implementations (asyncio and thread-safe) are exercised here. The
connection_factory is a stub that returns simple objects implementing
``is_open()`` / ``close()`` so we can drive every code path without touching
TCP. Live-server validation lives in tests/integration/test_asyncio.py.
"""

import asyncio
import threading
import time
from typing import List

import pytest

from rethinkdb.connection_pool import (
    AsyncioConnectionPool,
    PoolClosedError,
    PoolError,
    PoolExhaustedError,
    ThreadSafeConnectionPool,
)
from rethinkdb.errors import ReqlDriverError


class FakeAsyncConnection:
    """Minimal stand-in for an asyncio Connection object."""

    def __init__(self, name: str = "conn") -> None:
        self.name = name
        self._open = True
        self.close_count = 0

    def is_open(self) -> bool:
        return self._open

    async def close(self, noreply_wait: bool = False) -> None:
        self.close_count += 1
        self._open = False

    def kill(self) -> None:
        """Simulate the server severing the connection."""
        self._open = False


class FakeSyncConnection:
    def __init__(self, name: str = "conn") -> None:
        self.name = name
        self._open = True
        self.close_count = 0

    def is_open(self) -> bool:
        return self._open

    def close(self, noreply_wait: bool = False) -> None:
        self.close_count += 1
        self._open = False

    def kill(self) -> None:
        self._open = False


# ----------------------------------------------------------------------
# AsyncioConnectionPool
# ----------------------------------------------------------------------


@pytest.fixture
def async_factory():
    """Factory that returns sequentially-named FakeAsyncConnections."""
    state = {"n": 0}
    created: List[FakeAsyncConnection] = []

    async def factory():
        state["n"] += 1
        c = FakeAsyncConnection(name=f"conn-{state['n']}")
        created.append(c)
        return c

    factory.created = created  # type: ignore[attr-defined]
    return factory


@pytest.mark.unit
class TestAsyncioConnectionPoolBasics:
    @pytest.mark.asyncio
    async def test_acquire_creates_new_connection(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=3)
        try:
            conn = await pool.acquire()
            assert conn.name == "conn-1"
            assert pool.size == 1
            assert pool.in_use == 1
            assert pool.idle == 0
            assert not pool.closed
        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_release_returns_to_pool(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=3)
        try:
            conn = await pool.acquire()
            await pool.release(conn)
            assert pool.in_use == 0
            assert pool.idle == 1
            assert pool.size == 1
        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_release_reuses_idle(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=3)
        try:
            c1 = await pool.acquire()
            await pool.release(c1)
            c2 = await pool.acquire()
            assert c2 is c1
            assert pool.size == 1
        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_connection_context_manager(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=3)
        try:
            async with pool.connection() as conn:
                assert conn.name == "conn-1"
                assert pool.in_use == 1
            assert pool.in_use == 0
            assert pool.idle == 1
        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_connection_releases_on_exception(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=3)
        try:
            with pytest.raises(RuntimeError, match="boom"):
                async with pool.connection():
                    raise RuntimeError("boom")
            assert pool.in_use == 0
            assert pool.idle == 1
        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_repr_contains_state(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=5)
        try:
            assert "AsyncioConnectionPool" in repr(pool)
            assert "max_size=5" in repr(pool)
            assert "size=0" in repr(pool)
        finally:
            await pool.close()


@pytest.mark.unit
class TestAsyncioConnectionPoolExhaustion:
    @pytest.mark.asyncio
    async def test_exhaustion_with_timeout_zero(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=2)
        try:
            await pool.acquire()
            await pool.acquire()
            with pytest.raises(PoolExhaustedError, match="max_size=2"):
                await pool.acquire(timeout=0)
        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_exhaustion_with_short_timeout(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=1)
        try:
            await pool.acquire()
            start = time.monotonic()
            with pytest.raises(PoolExhaustedError):
                await pool.acquire(timeout=0.1)
            elapsed = time.monotonic() - start
            assert 0.05 <= elapsed < 0.5, f"timeout drift: elapsed={elapsed}"
        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_release_wakes_waiter(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=1)
        try:
            c1 = await pool.acquire()

            async def waiter():
                return await pool.acquire(timeout=2.0)

            task = asyncio.create_task(waiter())
            await asyncio.sleep(0.05)
            assert not task.done()

            await pool.release(c1)
            c2 = await asyncio.wait_for(task, timeout=1.0)
            assert c2 is c1
        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_concurrent_acquire_bounded(self, async_factory):
        """N tasks racing for K slots all complete; no more than K conns are made."""
        pool = AsyncioConnectionPool(async_factory, max_size=3)
        try:
            results: list = []

            async def worker(i: int):
                async with pool.connection() as conn:
                    await asyncio.sleep(0.01)
                    results.append((i, conn.name))

            await asyncio.gather(*(worker(i) for i in range(20)))
            assert len(results) == 20
            distinct = {name for _, name in results}
            assert len(distinct) <= 3, f"created too many connections: {distinct}"
        finally:
            await pool.close()


@pytest.mark.unit
class TestAsyncioConnectionPoolHealth:
    @pytest.mark.asyncio
    async def test_dead_idle_connection_replaced(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=2)
        try:
            c1 = await pool.acquire()
            await pool.release(c1)
            c1.kill()

            c2 = await pool.acquire()
            assert c2 is not c1
            assert c1.close_count == 1
        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_max_idle_time_evicts(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=2, max_idle_time=0.05)
        try:
            c1 = await pool.acquire()
            await pool.release(c1)
            await asyncio.sleep(0.1)

            c2 = await pool.acquire()
            assert c2 is not c1
            assert c1.close_count == 1
        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_dead_active_connection_dropped_on_release(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=2)
        try:
            c1 = await pool.acquire()
            c1.kill()
            await pool.release(c1)
            assert pool.idle == 0
            assert pool.size == 0
            assert c1.close_count == 1
        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_factory_failure_releases_slot(self):
        attempts = {"n": 0}

        async def flaky_factory():
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise ConnectionError("first call fails")
            return FakeAsyncConnection(name=f"conn-{attempts['n']}")

        pool = AsyncioConnectionPool(flaky_factory, max_size=1)
        try:
            with pytest.raises(ConnectionError):
                await pool.acquire()
            # Slot must have been released or future acquires would deadlock.
            assert pool.size == 0
            conn = await pool.acquire()
            assert conn.name == "conn-2"
        finally:
            await pool.close()


@pytest.mark.unit
class TestAsyncioConnectionPoolClose:
    @pytest.mark.asyncio
    async def test_close_idempotent(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=2)
        await pool.close()
        assert pool.closed
        await pool.close()  # second call is a no-op

    @pytest.mark.asyncio
    async def test_close_closes_idle_and_active(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=3)
        c1 = await pool.acquire()
        c2 = await pool.acquire()
        await pool.release(c2)

        await pool.close()
        assert c1.close_count >= 1
        assert c2.close_count >= 1
        assert pool.closed
        assert pool.size == 0

    @pytest.mark.asyncio
    async def test_acquire_after_close_raises(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=1)
        await pool.close()
        with pytest.raises(PoolClosedError):
            await pool.acquire()

    @pytest.mark.asyncio
    async def test_release_after_close_does_not_crash(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=1)
        c1 = await pool.acquire()
        await pool.close()
        # close() removed c1 from _active and closed it; release() should be a no-op.
        await pool.release(c1)
        assert c1.close_count >= 1


@pytest.mark.unit
class TestAsyncioConnectionPoolEdgeCases:
    @pytest.mark.asyncio
    async def test_double_release_is_no_op(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=2)
        try:
            c1 = await pool.acquire()
            await pool.release(c1)
            await pool.release(c1)
            assert pool.idle == 1
            assert pool.in_use == 0
        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_release_alien_connection_is_no_op(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=2)
        try:
            alien = FakeAsyncConnection("not-from-pool")
            await pool.release(alien)
            assert pool.size == 0
        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_release_none_is_no_op(self, async_factory):
        pool = AsyncioConnectionPool(async_factory, max_size=1)
        try:
            await pool.release(None)
        finally:
            await pool.close()

    def test_max_size_validation(self):
        with pytest.raises(ValueError, match="max_size"):
            AsyncioConnectionPool(lambda: None, max_size=0)
        with pytest.raises(ValueError, match="max_size"):
            AsyncioConnectionPool(lambda: None, max_size=-1)


# ----------------------------------------------------------------------
# ThreadSafeConnectionPool
# ----------------------------------------------------------------------


@pytest.fixture
def sync_factory():
    state = {"n": 0}
    created: List[FakeSyncConnection] = []

    def factory():
        state["n"] += 1
        c = FakeSyncConnection(name=f"conn-{state['n']}")
        created.append(c)
        return c

    factory.created = created  # type: ignore[attr-defined]
    return factory


@pytest.mark.unit
class TestThreadSafeConnectionPoolBasics:
    def test_acquire_release_reuse(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=3)
        try:
            c1 = pool.acquire()
            assert pool.in_use == 1
            pool.release(c1)
            assert pool.idle == 1
            c2 = pool.acquire()
            assert c2 is c1
        finally:
            pool.close()

    def test_connection_context_manager(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=2)
        try:
            with pool.connection() as conn:
                assert conn.name == "conn-1"
                assert pool.in_use == 1
            assert pool.in_use == 0
            assert pool.idle == 1
        finally:
            pool.close()

    def test_connection_releases_on_exception(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=2)
        try:
            with pytest.raises(RuntimeError, match="kapow"):
                with pool.connection():
                    raise RuntimeError("kapow")
            assert pool.in_use == 0
            assert pool.idle == 1
        finally:
            pool.close()


@pytest.mark.unit
class TestThreadSafeConnectionPoolExhaustion:
    def test_exhaustion_with_timeout_zero(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=1)
        try:
            pool.acquire()
            with pytest.raises(PoolExhaustedError):
                pool.acquire(timeout=0)
        finally:
            pool.close()

    def test_exhaustion_with_short_timeout(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=1)
        try:
            pool.acquire()
            start = time.monotonic()
            with pytest.raises(PoolExhaustedError):
                pool.acquire(timeout=0.1)
            elapsed = time.monotonic() - start
            assert 0.05 <= elapsed < 0.5, f"timeout drift: elapsed={elapsed}"
        finally:
            pool.close()

    def test_release_wakes_thread_waiter(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=1)
        try:
            c1 = pool.acquire()
            result: list = []

            def worker():
                result.append(pool.acquire(timeout=2.0))

            t = threading.Thread(target=worker)
            t.start()
            time.sleep(0.05)
            assert t.is_alive()

            pool.release(c1)
            t.join(timeout=1.0)
            assert not t.is_alive()
            assert result[0] is c1
        finally:
            pool.close()


@pytest.mark.unit
class TestThreadSafeConnectionPoolHealth:
    def test_dead_idle_replaced(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=2)
        try:
            c1 = pool.acquire()
            pool.release(c1)
            c1.kill()
            c2 = pool.acquire()
            assert c2 is not c1
            assert c1.close_count == 1
        finally:
            pool.close()

    def test_max_idle_time_evicts(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=2, max_idle_time=0.05)
        try:
            c1 = pool.acquire()
            pool.release(c1)
            time.sleep(0.1)
            c2 = pool.acquire()
            assert c2 is not c1
            assert c1.close_count == 1
        finally:
            pool.close()

    def test_factory_failure_releases_slot(self):
        attempts = {"n": 0}

        def flaky_factory():
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise ConnectionError("first call fails")
            return FakeSyncConnection(name=f"conn-{attempts['n']}")

        pool = ThreadSafeConnectionPool(flaky_factory, max_size=1)
        try:
            with pytest.raises(ConnectionError):
                pool.acquire()
            assert pool.size == 0
            c = pool.acquire()
            assert c.name == "conn-2"
        finally:
            pool.close()


@pytest.mark.unit
class TestThreadSafeConnectionPoolClose:
    def test_close_idempotent(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=2)
        pool.close()
        assert pool.closed
        pool.close()

    def test_close_closes_all(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=3)
        c1 = pool.acquire()
        c2 = pool.acquire()
        pool.release(c2)

        pool.close()
        assert c1.close_count >= 1
        assert c2.close_count >= 1
        assert pool.size == 0

    def test_acquire_after_close_raises(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=1)
        pool.close()
        with pytest.raises(PoolClosedError):
            pool.acquire()


@pytest.mark.unit
class TestThreadSafeConnectionPoolAsyncWrappers:
    @pytest.mark.asyncio
    async def test_acquire_async(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=2)
        try:
            conn = await pool.acquire_async()
            assert conn.name == "conn-1"
            await pool.release_async(conn)
            assert pool.idle == 1
        finally:
            await pool.close_async()

    @pytest.mark.asyncio
    async def test_connection_async_context_manager(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=2)
        try:
            async with pool.connection_async() as conn:
                assert conn.name == "conn-1"
            assert pool.idle == 1
        finally:
            await pool.close_async()


@pytest.mark.unit
class TestThreadSafeConnectionPoolEdgeCases:
    def test_double_release_is_no_op(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=2)
        try:
            c1 = pool.acquire()
            pool.release(c1)
            pool.release(c1)
            assert pool.idle == 1
        finally:
            pool.close()

    def test_release_alien_connection_is_no_op(self, sync_factory):
        pool = ThreadSafeConnectionPool(sync_factory, max_size=2)
        try:
            alien = FakeSyncConnection("alien")
            pool.release(alien)
            assert pool.size == 0
        finally:
            pool.close()

    def test_max_size_validation(self):
        with pytest.raises(ValueError, match="max_size"):
            ThreadSafeConnectionPool(lambda: None, max_size=0)


@pytest.mark.unit
class TestExceptionHierarchy:
    def test_pool_errors_subclass_reql_driver_error(self):
        assert issubclass(PoolError, ReqlDriverError)
        assert issubclass(PoolClosedError, PoolError)
        assert issubclass(PoolExhaustedError, PoolError)
