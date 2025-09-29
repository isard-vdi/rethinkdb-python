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

"""Universal connection pooling interface for RethinkDB Python driver"""

import asyncio
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from contextlib import asynccontextmanager, contextmanager
from typing import Any, AsyncGenerator, Generator, Optional


class ConnectionPool(ABC):
    """Abstract base class for connection pools"""

    @abstractmethod
    async def acquire(self):
        """Acquire a connection from the pool"""
        pass

    @abstractmethod
    async def release(self, connection):
        """Release a connection back to the pool"""
        pass

    @abstractmethod
    async def close(self):
        """Close all connections in the pool"""
        pass

    @abstractmethod
    def connection(self):
        """Return a context manager for acquiring/releasing connections"""
        pass


class AsyncioConnectionPool(ConnectionPool):
    """High-performance asyncio connection pool"""

    def __init__(self, connection_factory, max_size=10, min_size=1, max_idle_time=300):
        self.connection_factory = connection_factory
        self.max_size = max_size
        self.min_size = min_size
        self.max_idle_time = max_idle_time
        self._pool = deque()
        self._active_connections = set()
        self._pool_lock = asyncio.Lock()
        self._closed = False

    async def acquire(self):
        """Acquire a connection from the pool"""
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        async with self._pool_lock:
            # Try to get a connection from the pool
            while self._pool:
                conn, idle_since = self._pool.popleft()

                # Check if connection is still valid and not too old
                if (conn.is_open() and
                    (time.time() - idle_since) < self.max_idle_time):
                    self._active_connections.add(conn)
                    return conn
                else:
                    # Connection expired or closed, clean it up
                    try:
                        await conn.close(noreply_wait=False)
                    except:
                        pass  # Ignore cleanup errors

            # No valid connections in pool, create new one if under limit
            if len(self._active_connections) < self.max_size:
                conn = await self.connection_factory()
                self._active_connections.add(conn)
                return conn

            # Pool exhausted, could implement waiting or raise error
            raise RuntimeError(
                f"Connection pool exhausted (max_size={self.max_size})"
            )

    async def release(self, connection):
        """Release a connection back to the pool"""
        if self._closed:
            try:
                await connection.close(noreply_wait=False)
            except:
                pass
            return

        async with self._pool_lock:
            if connection in self._active_connections:
                self._active_connections.remove(connection)

                # Only keep connection if it's still open and pool isn't full
                if (connection.is_open() and
                    len(self._pool) < self.max_size):
                    self._pool.append((connection, time.time()))
                else:
                    try:
                        await connection.close(noreply_wait=False)
                    except:
                        pass

    async def close(self):
        """Close all connections in the pool"""
        async with self._pool_lock:
            self._closed = True

            # Close all pooled connections
            while self._pool:
                conn, _ = self._pool.popleft()
                try:
                    await conn.close(noreply_wait=False)
                except:
                    pass

            # Close all active connections
            for conn in list(self._active_connections):
                try:
                    await conn.close(noreply_wait=False)
                except:
                    pass
            self._active_connections.clear()

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator:
        """Async context manager for connection acquisition/release"""
        conn = await self.acquire()
        try:
            yield conn
        finally:
            await self.release(conn)


class ThreadSafeConnectionPool(ConnectionPool):
    """Thread-safe connection pool for blocking operations"""

    def __init__(self, connection_factory, max_size=10, min_size=1, max_idle_time=300):
        self.connection_factory = connection_factory
        self.max_size = max_size
        self.min_size = min_size
        self.max_idle_time = max_idle_time
        self._pool = deque()
        self._active_connections = set()
        self._lock = threading.RLock()
        self._closed = False

    def acquire(self):
        """Acquire a connection from the pool (blocking)"""
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        with self._lock:
            # Try to get a connection from the pool
            while self._pool:
                conn, idle_since = self._pool.popleft()

                # Check if connection is still valid and not too old
                if (conn.is_open() and
                    (time.time() - idle_since) < self.max_idle_time):
                    self._active_connections.add(conn)
                    return conn
                else:
                    # Connection expired or closed, clean it up
                    try:
                        conn.close(noreply_wait=False)
                    except:
                        pass

            # No valid connections in pool, create new one if under limit
            if len(self._active_connections) < self.max_size:
                conn = self.connection_factory()
                self._active_connections.add(conn)
                return conn

            raise RuntimeError(
                f"Connection pool exhausted (max_size={self.max_size})"
            )

    def release(self, connection):
        """Release a connection back to the pool (blocking)"""
        if self._closed:
            try:
                connection.close(noreply_wait=False)
            except:
                pass
            return

        with self._lock:
            if connection in self._active_connections:
                self._active_connections.remove(connection)

                # Only keep connection if it's still open and pool isn't full
                if (connection.is_open() and
                    len(self._pool) < self.max_size):
                    self._pool.append((connection, time.time()))
                else:
                    try:
                        connection.close(noreply_wait=False)
                    except:
                        pass

    def close(self):
        """Close all connections in the pool (blocking)"""
        with self._lock:
            self._closed = True

            # Close all pooled connections
            while self._pool:
                conn, _ = self._pool.popleft()
                try:
                    conn.close(noreply_wait=False)
                except:
                    pass

            # Close all active connections
            for conn in list(self._active_connections):
                try:
                    conn.close(noreply_wait=False)
                except:
                    pass
            self._active_connections.clear()

    @contextmanager
    def connection(self) -> Generator:
        """Context manager for connection acquisition/release"""
        conn = self.acquire()
        try:
            yield conn
        finally:
            self.release(conn)

    # Async interface compatibility
    async def acquire_async(self):
        """Async wrapper for acquire (runs in thread pool)"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.acquire)

    async def release_async(self, connection):
        """Async wrapper for release (runs in thread pool)"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.release, connection)

    async def close_async(self):
        """Async wrapper for close (runs in thread pool)"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.close)