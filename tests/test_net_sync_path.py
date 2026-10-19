"""Unit tests for the blocking driver against a socketpair fake server."""

import datetime
import errno
import json
import socket
import struct
import threading
import time
import tracemalloc
import warnings

import pytest

from rethinkdb import RethinkDB
from rethinkdb.ast import ReQLDecoder
from rethinkdb.connection_pool import ThreadSafeConnectionPool
from rethinkdb.errors import ReqlDriverError, ReqlTimeoutError
from rethinkdb.net import (
    ConnectionInstance,
    DefaultConnection,
    Response,
    SocketWrapper,
    pQuery,
    pResponse,
)

r = RethinkDB()


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError
        buf += chunk
    return buf


def _read_query(sock):
    token, length = struct.unpack("<QL", _recv_exact(sock, 12))
    return token, json.loads(_recv_exact(sock, length))


def _frame(token, rtype, data):
    body = json.dumps({"t": rtype, "r": data}).encode()
    return struct.pack("<QL", token, len(body)) + body


def _tcp_pair():
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    client = socket.create_connection(listener.getsockname())
    server, _ = listener.accept()
    listener.close()
    return client, server


class CountingSocket:
    def __init__(self, sock, fail_recv=None):
        self._sock = sock
        self._fail_recv = fail_recv
        self.recv_calls = 0
        self.sockopts = []

    def recv(self, *args):
        self.recv_calls += 1
        if self._fail_recv is not None:
            raise self._fail_recv
        return self._sock.recv(*args)

    def recv_into(self, *args):
        self.recv_calls += 1
        if self._fail_recv is not None:
            raise self._fail_recv
        return self._sock.recv_into(*args)

    def setsockopt(self, *args):
        self.sockopts.append(args)
        return self._sock.setsockopt(*args)

    def __getattr__(self, name):
        return getattr(self._sock, name)


def _wire(timeout=5, wrap=None, **kwargs):
    conn = DefaultConnection(
        "h", 1, None, None, "admin", None, timeout, {}, 10, **kwargs
    )
    client, server = socket.socketpair()
    inst = ConnectionInstance(conn)
    sw = object.__new__(SocketWrapper)
    sw._socket = wrap(client) if wrap else client
    sw._read_buffer = None
    sw.host = "h"
    sw.port = 1
    sw.ssl = {}
    inst._socket = sw
    conn._instance = inst
    return conn, server


class FakeServer:
    """Answers queries with ``handler(token, query) -> bytes | None``."""

    def __init__(self, sock, handler):
        self.sock = sock
        self.handler = handler
        self.queries = []
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def _loop(self):
        try:
            while True:
                token, query = _read_query(self.sock)
                self.queries.append((token, query))
                out = self.handler(token, query)
                if out:
                    self.sock.sendall(out)
        except (EOFError, OSError):
            pass

    def stop(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
        self.thread.join(2)


def echo_handler(delay=0.0):
    def handler(token, query):
        if query[0] == pQuery.START:
            if delay:
                time.sleep(delay)
            return _frame(token, pResponse.SUCCESS_ATOM, [query[1]])
        if query[0] == pQuery.SERVER_INFO:
            return _frame(token, pResponse.SERVER_INFO, [{"name": "fake"}])
        if query[0] == pQuery.NOREPLY_WAIT:
            return _frame(token, pResponse.WAIT_COMPLETE, [])
        return None

    return handler


def _bounded(fn, limit=3.0, on_hang=None):
    box = {}

    def target():
        try:
            box["result"] = fn()
        except BaseException as exc:
            box["exc"] = exc

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(limit)
    if t.is_alive():
        if on_hang is not None:
            on_hang()
        t.join(2)
        pytest.fail("call did not return within %.1fs" % limit)
    if "exc" in box:
        raise box["exc"]
    return box.get("result")


def _close_quietly(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    sock.close()


@pytest.mark.unit
class TestReadDeadline:
    def test_deadline_applies_after_partial_chunk(self):
        conn, server = _wire()
        sw = conn._instance._socket
        server.sendall(b"ABC")
        start = time.monotonic()
        with pytest.raises(ReqlTimeoutError):
            _bounded(
                lambda: sw.recvall(6, time.monotonic() + 0.2),
                on_hang=lambda: _close_quietly(server),
            )
        assert time.monotonic() - start < 1.5
        _close_quietly(server)

    def test_partial_frame_resumes_after_cursor_wait(self):
        conn, server = _wire()

        def handler(token, query):
            if query[0] == pQuery.START:
                return _frame(token, pResponse.SUCCESS_PARTIAL, [1])
            if query[0] == pQuery.CONTINUE and not hasattr(handler, "rest"):
                frame = _frame(token, pResponse.SUCCESS_SEQUENCE, [2, 3])
                handler.rest = frame[20:]
                return frame[:20]
            return None

        srv = FakeServer(server, handler)
        try:
            cursor = _bounded(lambda: r.expr([1]).run(conn), on_hang=srv.stop)
            assert cursor.next(wait=0.5) == 1
            with pytest.raises(ReqlTimeoutError):
                _bounded(lambda: cursor.next(wait=0.2), on_hang=srv.stop)
            assert conn.is_open()
            server.sendall(handler.rest)
            assert _bounded(lambda: cursor.next(wait=1)) == 2
            assert _bounded(lambda: cursor.next(wait=1)) == 3
            assert conn._instance._socket._read_buffer is None
            assert conn._instance._header_in_progress is None
        finally:
            srv.stop()

    def test_read_buffer_is_cleared_after_use(self):
        conn, server = _wire()
        sw = conn._instance._socket
        sw._read_buffer = b"ABC"
        server.sendall(b"DEF")
        assert bytes(sw.recvall(6, None)) == b"ABCDEF"
        server.sendall(b"GHIJKL")
        assert bytes(sw.recvall(6, None)) == b"GHIJKL"
        assert not sw._read_buffer
        _close_quietly(server)


@pytest.mark.unit
class TestReadBuffers:
    def test_large_frame_is_filled_in_place(self):
        conn, server = _wire()
        sw = conn._instance._socket
        size = 8 * 1024 * 1024
        payload = bytes(range(256)) * (size // 256)

        def feed():
            for i in range(0, size, 65536):
                server.sendall(payload[i : i + 65536])

        feeder = threading.Thread(target=feed, daemon=True)
        tracemalloc.start()
        try:
            feeder.start()
            got = _bounded(lambda: sw.recvall(size, None), limit=10)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        feeder.join(2)
        assert got == payload
        assert peak < size * 1.5
        _close_quietly(server)

    def test_handshake_reads_replies_in_chunks(self, monkeypatch):
        client, server = _tcp_pair()
        wrapped = CountingSocket(client)
        monkeypatch.setattr(socket, "create_connection", lambda *a, **k: wrapped)

        class FakeHandshake:
            def __init__(self):
                self.responses = []

            def reset(self):
                pass

            def next_message(self, response):
                if response is None:
                    return b"hello\0"
                self.responses.append(response)
                if len(self.responses) == 1:
                    return ""
                return None

        parent = type("P", (), {})()
        parent._parent = DefaultConnection("h", 1, None, None, "admin", None, 5, {}, 10)
        parent._parent.handshake = FakeHandshake()
        server.sendall(b'{"first":"reply"}\0{"second":"reply"}\0')
        sw = _bounded(lambda: SocketWrapper(parent, 2))
        assert parent._parent.handshake.responses == [
            b'{"first":"reply"}',
            b'{"second":"reply"}',
        ]
        assert wrapped.recv_calls <= 3
        sw.close()
        _close_quietly(server)


@pytest.mark.unit
class TestPoisoning:
    def test_unexpected_oserror_closes_connection(self):
        conn, server = _wire(
            wrap=lambda s: CountingSocket(s, fail_recv=OSError(errno.EIO, "boom"))
        )
        with pytest.raises(ReqlDriverError):
            _bounded(conn.server, on_hang=lambda: _close_quietly(server))
        assert not conn.is_open()
        _close_quietly(server)
