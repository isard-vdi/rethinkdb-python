"""Edge-case tests for the asyncio backend's chunked _read_until() helper.

The fork rewrote _read_until to do 1 KB chunked reads instead of upstream's
byte-by-byte loop. The optimisation is correct on a normal network but has a
documented edge case: if the delimiter is the last byte of one TCP frame
and the next bytes belong to a different message, the chunked read could
overshoot. RethinkDB's handshake protocol uses null-terminated framing
where the delimiter only appears at message ends, so in practice this is
safe — but we want a regression net for it.
"""

import asyncio

import pytest

from rethinkdb.asyncio_net.net_asyncio import _read_until


class FakeStreamReader:
    """StreamReader stand-in that hands out pre-staged byte chunks.

    Each call to ``read(n)`` returns the next staged chunk verbatim,
    regardless of ``n`` — the implementation should still find the
    delimiter and stop, even if a chunk is split across what would be
    multiple TCP frames.
    """

    def __init__(self, chunks):
        self._chunks = list(chunks)
        self._read_calls = 0

    async def read(self, n):
        self._read_calls += 1
        if not self._chunks:
            return b""
        return self._chunks.pop(0)


@pytest.mark.unit
class TestReadUntil:
    @pytest.mark.asyncio
    async def test_delimiter_in_single_chunk(self):
        reader = FakeStreamReader([b"hello\0world", b""])
        result = await _read_until(reader, b"\0")
        assert result == b"hello\0"
        # Should stop at the delimiter; only one read call needed.
        assert reader._read_calls == 1

    @pytest.mark.asyncio
    async def test_delimiter_split_across_chunks(self):
        # The handshake response is delivered in two halves, with the
        # delimiter at the start of the second chunk.
        reader = FakeStreamReader([b'{"success":true}', b"\0", b""])
        result = await _read_until(reader, b"\0")
        assert result == b'{"success":true}\0'

    @pytest.mark.asyncio
    async def test_delimiter_at_frame_boundary(self):
        # Delimiter is the last byte of the chunk — common case for short
        # ack messages on a slow network.
        reader = FakeStreamReader([b"ack\0", b""])
        result = await _read_until(reader, b"\0")
        assert result == b"ack\0"

    @pytest.mark.asyncio
    async def test_eof_before_delimiter(self):
        # Connection died mid-message; we get back what we read, no infinite loop.
        reader = FakeStreamReader([b"partial", b""])
        result = await _read_until(reader, b"\0")
        assert result == b"partial"

    @pytest.mark.asyncio
    async def test_immediate_eof(self):
        reader = FakeStreamReader([b""])
        result = await _read_until(reader, b"\0")
        assert result == b""

    @pytest.mark.asyncio
    async def test_multibyte_delimiter(self):
        reader = FakeStreamReader([b"prefix\r\nsuffix", b""])
        result = await _read_until(reader, b"\r\n")
        assert result == b"prefix\r\n"

    @pytest.mark.asyncio
    async def test_handshake_split_with_concurrent_yields(self):
        """Simulate slow network: each chunk arrives after an event-loop tick.

        This is the scenario the fork's IsardVDI evaluation flagged — handshake
        bytes split across multiple data_received callbacks. The chunked read
        must correctly accumulate them and find the terminating null.
        """

        class SlowReader:
            def __init__(self, chunks):
                self._chunks = list(chunks)

            async def read(self, n):
                if not self._chunks:
                    return b""
                # Yield to the event loop between chunks so other tasks run.
                await asyncio.sleep(0)
                return self._chunks.pop(0)

        # Mimic a 60-byte JSON handshake response delivered in 5 frames.
        msg = b'{"success":true,"min_protocol_version":0,"max_protocol_version":1}\0'
        reader = SlowReader([msg[i : i + 12] for i in range(0, len(msg), 12)])
        result = await _read_until(reader, b"\0")
        assert result == msg
