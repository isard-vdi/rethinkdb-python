# Copyright 2018 RethinkDB
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Connection pooling for the RethinkDB Python driver.

The RethinkDB wire protocol multiplexes many concurrent queries over a single
TCP connection by tagging each query with a token. The driver however serializes
writes per connection, so a single shared connection becomes a bottleneck under
load. Pooling a small number of connections (typically 4-10) is sufficient to
remove that bottleneck for most workloads.

Two implementations are provided:

* :class:`AsyncioConnectionPool` for asyncio-native consumers.
* :class:`ThreadSafeConnectionPool` for blocking code, with optional async
  wrappers so it can also be driven from asyncio code via thread-pool offload.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from contextlib import asynccontextmanager, contextmanager
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Deque,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)

from rethinkdb.errors import ReqlDriverError
from rethinkdb.logger import default_logger

__all__ = [
    "AsyncioConnectionPool",
    "PoolClosedError",
    "PoolError",
    "PoolExhaustedError",
    "ThreadSafeConnectionPool",
]


class PoolError(ReqlDriverError):
    """Base class for connection pool errors."""


class PoolClosedError(PoolError):
    """Raised when an operation is attempted on a closed pool."""


class PoolExhaustedError(PoolError):
    """Raised when no connection becomes available within the timeout."""


AsyncFactory = Callable[[], Awaitable[Any]]
SyncFactory = Callable[[], Any]


def _validate_max_size(max_size: int) -> None:
    if max_size < 1:
        raise ValueError(f"max_size must be >= 1 (got {max_size})")


class AsyncioConnectionPool:
    """Asyncio connection pool.

    The factory is an async callable returning a connection-like object that
    exposes ``is_open() -> bool`` and ``await close(noreply_wait: bool = ...)``.

    :param connection_factory: async callable that returns a fresh connection.
    :param max_size: maximum number of physical connections held by the pool.
    :param max_idle_time: seconds an idle connection may sit before being
        evicted on its next acquisition attempt.
    """

    def __init__(
        self,
        connection_factory: AsyncFactory,
        max_size: int = 10,
        max_idle_time: float = 300.0,
    ) -> None:
        _validate_max_size(max_size)
        self.connection_factory = connection_factory
        self.max_size = max_size
        self.max_idle_time = max_idle_time

        self._idle: Deque[Tuple[Any, float]] = deque()
        self._active: Set[Any] = set()
        self._created = 0
        self._closed = False
        self._lock = asyncio.Lock()
        self._available = asyncio.Condition(self._lock)

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def size(self) -> int:
        """Total physical connections owned by the pool (idle + in-use)."""
        return self._created

    @property
    def in_use(self) -> int:
        """Connections currently checked out by callers."""
        return len(self._active)

    @property
    def idle(self) -> int:
        """Connections sitting available in the pool."""
        return len(self._idle)

    def __repr__(self) -> str:
        return (
            f"AsyncioConnectionPool(size={self.size}, in_use={self.in_use}, "
            f"idle={self.idle}, max_size={self.max_size}, closed={self.closed})"
        )

    async def acquire(self, timeout: Optional[float] = None) -> Any:
        """Acquire a connection, optionally blocking for up to ``timeout`` seconds."""
        if self._closed:
            raise PoolClosedError("Connection pool is closed")

        deadline = None if timeout is None else time.monotonic() + timeout
        must_create = False
        stale: List[Any] = []

        try:
            async with self._available:
                while not must_create:
                    if self._closed:
                        raise PoolClosedError("Connection pool is closed")

                    found = self._pop_valid_idle_locked(stale)
                    if found is not None:
                        self._active.add(found)
                        return found

                    if self._created < self.max_size:
                        self._created += 1
                        must_create = True
                        break

                    remaining = (
                        None if deadline is None else deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        raise PoolExhaustedError(
                            f"Connection pool exhausted (max_size={self.max_size}, "
                            f"in_use={self.in_use})"
                        )
                    try:
                        await asyncio.wait_for(self._available.wait(), remaining)
                    except asyncio.TimeoutError:
                        raise PoolExhaustedError(
                            f"Connection pool exhausted (max_size={self.max_size}, "
                            f"in_use={self.in_use})"
                        ) from None
        finally:
            for conn in stale:
                await self._safe_close(conn)

        # Create the new connection outside the lock — it may be slow.
        try:
            conn = await self.connection_factory()
        except BaseException:
            async with self._available:
                self._created -= 1
                self._available.notify_all()
            raise

        async with self._available:
            if self._closed:
                self._created -= 1
                self._available.notify_all()
                await self._safe_close(conn)
                raise PoolClosedError("Connection pool was closed during acquire")
            self._active.add(conn)
            return conn

    async def release(self, connection: Any) -> None:
        """Return ``connection`` to the pool. Idempotent for unknown connections."""
        if connection is None:
            return

        to_close: Optional[Any] = None
        async with self._available:
            if connection not in self._active:
                # Double-release or a stranger — drop silently.
                return
            self._active.remove(connection)

            if self._closed:
                to_close = connection
                self._created -= 1
            elif self._is_usable(connection):
                self._idle.append((connection, time.monotonic()))
            else:
                to_close = connection
                self._created -= 1

            self._available.notify_all()

        if to_close is not None:
            await self._safe_close(to_close)

    async def close(self) -> None:
        """Close every connection in the pool. Idempotent."""
        async with self._available:
            if self._closed:
                return
            self._closed = True
            idle_to_close = [conn for conn, _ in self._idle]
            self._idle.clear()
            active_to_close = list(self._active)
            self._active.clear()
            self._created = 0
            self._available.notify_all()

        if not idle_to_close and not active_to_close:
            return
        await asyncio.gather(
            *(self._safe_close(c) for c in idle_to_close + active_to_close),
            return_exceptions=True,
        )

    @asynccontextmanager
    async def connection(self, timeout: Optional[float] = None) -> AsyncIterator[Any]:
        """Async context manager that acquires a connection and releases it on exit."""
        conn = await self.acquire(timeout=timeout)
        try:
            yield conn
        finally:
            await self.release(conn)

    def _pop_valid_idle_locked(self, stale: List[Any]) -> Optional[Any]:
        """Pop entries from ``self._idle`` until a valid one is found.

        Stale entries are appended to ``stale`` for the caller to close outside
        the lock. Caller MUST hold ``self._lock``.
        """
        while self._idle:
            conn, idle_since = self._idle.popleft()
            if (time.monotonic() - idle_since) >= self.max_idle_time:
                stale.append(conn)
                self._created -= 1
                continue
            if not self._is_usable(conn):
                stale.append(conn)
                self._created -= 1
                continue
            return conn
        return None

    @staticmethod
    def _is_usable(connection: Any) -> bool:
        try:
            return bool(connection.is_open())
        except Exception:
            return False

    @staticmethod
    async def _safe_close(connection: Any) -> None:
        try:
            await connection.close(noreply_wait=False)
        except Exception as exc:
            default_logger.warning(f"Error closing pooled connection: {exc!r}")


class ThreadSafeConnectionPool:
    """Thread-safe blocking connection pool.

    The factory is a sync callable returning a connection-like object that
    exposes ``is_open() -> bool`` and ``close(noreply_wait: bool = ...)``.

    Async wrappers (``acquire_async`` / ``release_async`` / ``close_async`` /
    ``connection_async``) are available so the same pool can be driven from
    asyncio code via the default thread-pool executor — useful when a service
    has both sync and async call paths against the same database.
    """

    def __init__(
        self,
        connection_factory: SyncFactory,
        max_size: int = 10,
        max_idle_time: float = 300.0,
    ) -> None:
        _validate_max_size(max_size)
        self.connection_factory = connection_factory
        self.max_size = max_size
        self.max_idle_time = max_idle_time

        self._idle: Deque[Tuple[Any, float]] = deque()
        self._active: Set[Any] = set()
        self._created = 0
        self._closed = False
        self._lock = threading.RLock()
        self._available = threading.Condition(self._lock)

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def size(self) -> int:
        return self._created

    @property
    def in_use(self) -> int:
        return len(self._active)

    @property
    def idle(self) -> int:
        return len(self._idle)

    def __repr__(self) -> str:
        return (
            f"ThreadSafeConnectionPool(size={self.size}, in_use={self.in_use}, "
            f"idle={self.idle}, max_size={self.max_size}, closed={self.closed})"
        )

    def acquire(self, timeout: Optional[float] = None) -> Any:
        """Acquire a connection, optionally blocking for up to ``timeout`` seconds."""
        if self._closed:
            raise PoolClosedError("Connection pool is closed")

        deadline = None if timeout is None else time.monotonic() + timeout
        must_create = False
        stale: List[Any] = []

        try:
            with self._available:
                while not must_create:
                    if self._closed:
                        raise PoolClosedError("Connection pool is closed")

                    found = self._pop_valid_idle_locked(stale)
                    if found is not None:
                        self._active.add(found)
                        return found

                    if self._created < self.max_size:
                        self._created += 1
                        must_create = True
                        break

                    remaining = (
                        None if deadline is None else deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        raise PoolExhaustedError(
                            f"Connection pool exhausted (max_size={self.max_size}, "
                            f"in_use={self.in_use})"
                        )
                    self._available.wait(remaining)
                    if (
                        deadline is not None
                        and time.monotonic() >= deadline
                        and not (self._idle or self._created < self.max_size)
                    ):
                        # Timed out without progress.
                        continue  # next loop will raise on remaining<=0
        finally:
            for conn in stale:
                self._safe_close(conn)

        try:
            conn = self.connection_factory()
        except BaseException:
            with self._available:
                self._created -= 1
                self._available.notify_all()
            raise

        with self._available:
            if self._closed:
                self._created -= 1
                self._available.notify_all()
                self._safe_close(conn)
                raise PoolClosedError("Connection pool was closed during acquire")
            self._active.add(conn)
            return conn

    def release(self, connection: Any) -> None:
        """Return ``connection`` to the pool. Idempotent for unknown connections."""
        if connection is None:
            return

        to_close: Optional[Any] = None
        with self._available:
            if connection not in self._active:
                return
            self._active.remove(connection)

            if self._closed:
                to_close = connection
                self._created -= 1
            elif self._is_usable(connection):
                self._idle.append((connection, time.monotonic()))
            else:
                to_close = connection
                self._created -= 1

            self._available.notify_all()

        if to_close is not None:
            self._safe_close(to_close)

    def close(self) -> None:
        """Close every connection in the pool. Idempotent."""
        with self._available:
            if self._closed:
                return
            self._closed = True
            idle_to_close = [conn for conn, _ in self._idle]
            self._idle.clear()
            active_to_close = list(self._active)
            self._active.clear()
            self._created = 0
            self._available.notify_all()

        for conn in idle_to_close:
            self._safe_close(conn)
        for conn in active_to_close:
            self._safe_close(conn)

    @contextmanager
    def connection(self, timeout: Optional[float] = None) -> Iterator[Any]:
        """Context manager that acquires a connection and releases it on exit."""
        conn = self.acquire(timeout=timeout)
        try:
            yield conn
        finally:
            self.release(conn)

    async def acquire_async(self, timeout: Optional[float] = None) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.acquire, timeout)

    async def release_async(self, connection: Any) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.release, connection)

    async def close_async(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.close)

    @asynccontextmanager
    async def connection_async(
        self, timeout: Optional[float] = None
    ) -> AsyncIterator[Any]:
        conn = await self.acquire_async(timeout=timeout)
        try:
            yield conn
        finally:
            await self.release_async(conn)

    def _pop_valid_idle_locked(self, stale: List[Any]) -> Optional[Any]:
        while self._idle:
            conn, idle_since = self._idle.popleft()
            if (time.monotonic() - idle_since) >= self.max_idle_time:
                stale.append(conn)
                self._created -= 1
                continue
            if not self._is_usable(conn):
                stale.append(conn)
                self._created -= 1
                continue
            return conn
        return None

    @staticmethod
    def _is_usable(connection: Any) -> bool:
        try:
            return bool(connection.is_open())
        except Exception:
            return False

    @staticmethod
    def _safe_close(connection: Any) -> None:
        try:
            connection.close(noreply_wait=False)
        except Exception as exc:
            default_logger.warning(f"Error closing pooled connection: {exc!r}")
