"""Edge-case tests for the asyncio backend's ``_read_until`` helper.

The asyncio handshake reads null-terminated JSON messages with this helper.
The legacy implementation did its own chunked reads and silently discarded
any bytes that arrived after the delimiter in the same TCP frame; under
connection-pool warm-up the V1.0 handshake's pipelined replies could land
in one frame, the post-delimiter half got dropped, and the next read
surfaced as ``ReqlDriverError: Connection interrupted during handshake``.

These tests pin the corrected behaviour: the helper now defers to
``StreamReader.readuntil`` so trailing bytes remain in the reader's
internal buffer for the next call.
"""

import asyncio

import pytest

from rethinkdb.asyncio_net.net_asyncio import _read_until


def _make_reader(*chunks: bytes, eof: bool = True) -> asyncio.StreamReader:
    """Build a fully-buffered :class:`asyncio.StreamReader` from the given
    chunks. Equivalent to feeding ``data_received`` callbacks in the
    transport layer — but with the data already buffered, the reader
    behaves exactly like a real socket that received the same frames.
    """
    reader = asyncio.StreamReader()
    for chunk in chunks:
        reader.feed_data(chunk)
    if eof:
        reader.feed_eof()
    return reader


@pytest.mark.unit
class TestReadUntil:
    @pytest.mark.asyncio
    async def test_delimiter_in_single_chunk(self):
        reader = _make_reader(b"hello\0world")
        result = await _read_until(reader, b"\0")
        assert result == b"hello\0"

    @pytest.mark.asyncio
    async def test_delimiter_split_across_chunks(self):
        # Handshake response delivered in two halves with the delimiter
        # at the start of the second chunk.
        reader = _make_reader(b'{"success":true}', b"\0")
        result = await _read_until(reader, b"\0")
        assert result == b'{"success":true}\0'

    @pytest.mark.asyncio
    async def test_delimiter_at_frame_boundary(self):
        reader = _make_reader(b"ack\0")
        result = await _read_until(reader, b"\0")
        assert result == b"ack\0"

    @pytest.mark.asyncio
    async def test_eof_before_delimiter(self):
        # Connection died mid-message — return what we have, no exception.
        reader = _make_reader(b"partial")
        result = await _read_until(reader, b"\0")
        assert result == b"partial"

    @pytest.mark.asyncio
    async def test_immediate_eof(self):
        reader = _make_reader()
        result = await _read_until(reader, b"\0")
        assert result == b""

    @pytest.mark.asyncio
    async def test_multibyte_delimiter(self):
        reader = _make_reader(b"prefix\r\nsuffix\r\n")
        result = await _read_until(reader, b"\r\n")
        assert result == b"prefix\r\n"

    @pytest.mark.asyncio
    async def test_handshake_split_with_concurrent_yields(self):
        """Bytes arriving after event-loop ticks must be reassembled."""
        reader = asyncio.StreamReader()

        async def feeder() -> None:
            msg = (
                b'{"success":true,"min_protocol_version":0,'
                b'"max_protocol_version":1}\0'
            )
            for i in range(0, len(msg), 12):
                # Yield to the loop between feeds — simulates separate
                # ``data_received`` callbacks on the transport.
                await asyncio.sleep(0)
                reader.feed_data(msg[i : i + 12])
            reader.feed_eof()

        feed_task = asyncio.create_task(feeder())
        result = await _read_until(reader, b"\0")
        await feed_task
        assert result == (
            b'{"success":true,"min_protocol_version":0,' b'"max_protocol_version":1}\0'
        )

    @pytest.mark.asyncio
    async def test_post_delimiter_bytes_preserved_for_next_call(self):
        """Regression for the IsardVDI handshake-race bug.

        When two null-terminated messages arrive in one TCP frame (the
        V1.0 handshake's pipelined replies under pool warm-up), the
        bytes after the first delimiter MUST remain in the stream for
        the next ``_read_until`` call. The previous chunked
        implementation silently discarded them and the second
        handshake step would block on EOF.
        """
        reader = _make_reader(b"first-message\0second-message\0")

        first = await _read_until(reader, b"\0")
        second = await _read_until(reader, b"\0")

        assert first == b"first-message\0"
        assert second == b"second-message\0"

    @pytest.mark.asyncio
    async def test_three_pipelined_messages_in_one_frame(self):
        """Stretch case: SCRAM-SHA-256 doesn't pipeline three replies,
        but the helper's contract is "consume exactly one delimited
        message per call" so verify it scales beyond two."""
        reader = _make_reader(b"a\0bb\0ccc\0")

        assert (await _read_until(reader, b"\0")) == b"a\0"
        assert (await _read_until(reader, b"\0")) == b"bb\0"
        assert (await _read_until(reader, b"\0")) == b"ccc\0"
