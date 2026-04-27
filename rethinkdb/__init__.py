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

import builtins

from rethinkdb import errors, version
from rethinkdb.connection_pool import (
    AsyncioConnectionPool,
    PoolClosedError,
    PoolError,
    PoolExhaustedError,
    ThreadSafeConnectionPool,
)

__all__ = [
    "RethinkDB",
    "AsyncioConnectionPool",
    "ThreadSafeConnectionPool",
    "PoolError",
    "PoolClosedError",
    "PoolExhaustedError",
] + errors.__all__
__version__ = version.VERSION


class RethinkDB(builtins.object):
    def __init__(self):
        super(RethinkDB, self).__init__()

        from rethinkdb import (
            _dump,
            _export,
            _import,
            _index_rebuild,
            _restore,
            ast,
            net,
            query,
        )

        self._dump = _dump
        self._export = _export
        self._import = _import
        self._index_rebuild = _index_rebuild
        self._restore = _restore

        # Re-export internal modules for backward compatibility
        self.ast = ast
        self.errors = errors
        self.net = net
        self.query = query

        net.Connection._r = self

        for module in (self.net, self.query, self.ast, self.errors):
            for function_name in module.__all__:
                setattr(self, function_name, getattr(module, function_name))

        self.set_loop_type(None)

    def set_loop_type(self, library=None):
        if library == "asyncio":
            from rethinkdb.asyncio_net import net_asyncio

            self.connection_type = net_asyncio.Connection

        if library == "gevent":
            from rethinkdb.gevent_net import net_gevent

            self.connection_type = net_gevent.Connection

        if library == "tornado":
            from rethinkdb.tornado_net import net_tornado

            self.connection_type = net_tornado.Connection

        if library == "trio":
            from rethinkdb.trio_net import net_trio

            self.connection_type = net_trio.Connection

        if library == "twisted":
            from rethinkdb.twisted_net import net_twisted

            self.connection_type = net_twisted.Connection

        if library is None or self.connection_type is None:
            self.connection_type = self.net.DefaultConnection

        return

    def connect(self, *args, **kwargs):
        return self.make_connection(self.connection_type, *args, **kwargs)

    def create_pool(self, *args, max_size=10, max_idle_time=300.0, **kwargs):
        """Create a connection pool that produces connections via :meth:`connect`.

        Returns an :class:`AsyncioConnectionPool` if the active loop type is
        ``"asyncio"``, otherwise a :class:`ThreadSafeConnectionPool`. Raises
        :class:`RuntimeError` for the tornado/trio/gevent/twisted backends —
        those would require backend-specific pool implementations.

        ``*args`` and ``**kwargs`` are forwarded to :meth:`connect` each time
        the factory creates a new physical connection.
        """
        from rethinkdb.asyncio_net.net_asyncio import Connection as _AsyncioConnection

        if self.connection_type is _AsyncioConnection:

            async def async_factory():
                return await self.connect(*args, **kwargs)

            return AsyncioConnectionPool(
                async_factory,
                max_size=max_size,
                max_idle_time=max_idle_time,
            )

        if self.connection_type is self.net.DefaultConnection:

            def sync_factory():
                return self.connect(*args, **kwargs)

            return ThreadSafeConnectionPool(
                sync_factory,
                max_size=max_size,
                max_idle_time=max_idle_time,
            )

        raise RuntimeError(
            f"Connection pooling is not supported for "
            f"{self.connection_type.__name__}; use 'asyncio' loop type or the "
            "default sync backend, or construct a pool directly with a custom "
            "connection factory."
        )


# Initialize r after all imports are resolved
# This is now safe because we fixed the relative imports
r = RethinkDB()
