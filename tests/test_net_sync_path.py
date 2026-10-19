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


@pytest.mark.unit
class TestReadTimeoutOption:
    def test_query_read_timeout_closes_connection(self):
        conn, server = _wire()
        srv = FakeServer(server, lambda token, query: None)
        try:
            with pytest.raises(ReqlTimeoutError):
                _bounded(
                    lambda: r.expr(1).run(conn, read_timeout=0.2), on_hang=srv.stop
                )
            assert not conn.is_open()
            assert "read_timeout" not in json.dumps(srv.queries)
        finally:
            srv.stop()

    def test_timeout_mid_frame_closes_connection(self):
        conn, server = _wire(read_timeout=0.2)

        def handler(token, query):
            return _frame(token, pResponse.SERVER_INFO, [{"name": "x"}])[:15]

        srv = FakeServer(server, handler)
        try:
            with pytest.raises(ReqlTimeoutError):
                _bounded(conn.server, on_hang=srv.stop)
            assert not conn.is_open()
        finally:
            srv.stop()

    def test_connection_default_read_timeout(self):
        conn, server = _wire(read_timeout=0.2)
        srv = FakeServer(server, lambda token, query: None)
        try:
            start = time.monotonic()
            with pytest.raises(ReqlTimeoutError):
                _bounded(lambda: r.expr(1).run(conn), on_hang=srv.stop)
            assert time.monotonic() - start < 1.5
        finally:
            srv.stop()

    def test_run_option_is_not_sent_and_answer_arrives(self):
        conn, server = _wire()
        srv = FakeServer(server, echo_handler())
        try:
            assert _bounded(lambda: r.expr(7).run(conn, read_timeout=2)) == 7
            assert conn.is_open()
            assert "read_timeout" not in json.dumps(srv.queries)
        finally:
            srv.stop()


@pytest.mark.unit
class TestThreadSafety:
    def test_two_threads_share_one_connection(self):
        conn, server = _wire()
        srv = FakeServer(server, echo_handler(delay=0.005))
        errors = []

        def worker(base):
            try:
                for i in range(25):
                    value = base + i
                    got = r.expr(value).run(conn)
                    if got != value:
                        errors.append((value, got))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(b,)) for b in (0, 1000)]
        try:
            for t in threads:
                t.start()
            for t in threads:
                t.join(5)
            alive = any(t.is_alive() for t in threads)
        finally:
            srv.stop()
        assert not alive
        assert errors == []

    def test_cursor_batches_while_other_thread_queries(self):
        conn, server = _wire()
        batches = {}

        def handler(token, query):
            if query[0] == pQuery.START and query[1] == "feed":
                batches[token] = [[3, 4], [5, 6], [7, 8]]
                return _frame(token, pResponse.SUCCESS_PARTIAL, [1, 2])
            if query[0] == pQuery.CONTINUE:
                time.sleep(0.005)
                left = batches[token]
                if not left:
                    return _frame(token, pResponse.SUCCESS_SEQUENCE, [])
                data = left.pop(0)
                rtype = (
                    pResponse.SUCCESS_PARTIAL if left else pResponse.SUCCESS_SEQUENCE
                )
                return _frame(token, rtype, data)
            return echo_handler()(token, query)

        srv = FakeServer(server, handler)
        items, errors = [], []

        def iterate():
            try:
                for _ in range(10):
                    items.extend(list(r.expr("feed").run(conn)))
            except Exception as exc:
                errors.append(exc)

        def query():
            try:
                for i in range(30):
                    if r.expr(i).run(conn) != i:
                        errors.append(i)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=iterate), threading.Thread(target=query)]
        try:
            for t in threads:
                t.start()
            for t in threads:
                t.join(5)
            alive = any(t.is_alive() for t in threads)
        finally:
            srv.stop()
        assert not alive
        assert errors == []
        assert items == list(range(1, 9)) * 10

    def test_tokens_are_unique_across_threads(self):
        conn = DefaultConnection("h", 1, None, None, "admin", None, 5, {}, 10)
        out = []

        def take():
            out.append([conn._new_token() for _ in range(20000)])

        threads = [threading.Thread(target=take) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        tokens = [t for chunk in out for t in chunk]
        assert len(set(tokens)) == len(tokens)


@pytest.mark.unit
class TestCloseAndReconnect:
    def _dead(self, timeout=5):
        conn, server = _wire(timeout=timeout)
        _close_quietly(server)
        conn._instance._socket.close()
        return conn

    def test_close_on_dead_socket_skips_noreply_wait(self):
        conn = self._dead()
        _bounded(conn.close)
        assert conn._instance is None

    def test_reconnect_on_dead_connection(self):
        conn = self._dead()

        class FakeInstance:
            def __init__(self, parent, **kwargs):
                self.parent = parent

            def connect(self, timeout):
                return self.parent

            def is_open(self):
                return True

        conn._conn_type = FakeInstance
        assert _bounded(conn.reconnect) is conn
        assert isinstance(conn._instance, FakeInstance)

    def test_close_bounds_noreply_wait(self):
        conn, server = _wire(timeout=0.3)
        srv = FakeServer(server, lambda token, query: None)
        try:
            start = time.monotonic()
            with pytest.raises(ReqlTimeoutError):
                _bounded(lambda: conn.close(noreply_wait=True), on_hang=srv.stop)
            assert time.monotonic() - start < 2
            assert not conn.is_open()
        finally:
            srv.stop()
