"""Unit tests for the binary wire format encoding/decoding in conn.py.

Tests lease_token support in fetch response parsing, ack encoding, and fail encoding
WITHOUT requiring a running Corvo server.

Run with: .venv/bin/pytest tests/test_wire_format.py -v
"""

import struct
from unittest.mock import patch, MagicMock

import pytest

from corvo_client.conn import (
    Conn,
    AckJob,
    FailJob,
    _append_len_prefixed,
    _read_len_prefixed,
    FRAME_HEADER_FMT,
    FRAME_HEADER_SIZE,
    MSG_ACK_BATCH,
    MSG_FAIL_BATCH,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_len_prefixed(data: bytes) -> bytes:
    """Build a [len:u8][bytes...] field, matching the wire format."""
    return bytes([len(data)]) + data


def _build_fetch_response_payload(jobs: list[dict]) -> bytes:
    """Build a raw fetch response payload from a list of job dicts.

    Each job dict should have: id, queue, attempt, max_retries,
    checkpoint, tags, payload, lease_token.
    """
    buf = bytearray()
    buf.extend(struct.pack("<H", len(jobs)))

    for job in jobs:
        # [id:lenPrefixed][queue:lenPrefixed]
        buf.extend(_build_len_prefixed(job["id"].encode()))
        buf.extend(_build_len_prefixed(job["queue"].encode()))

        # [attempt:u16][max_retries:u16]
        buf.extend(struct.pack("<HH", job["attempt"], job["max_retries"]))

        # [checkpoint:lenPrefixed][tags:lenPrefixed]
        buf.extend(_build_len_prefixed(job.get("checkpoint", "").encode()))
        buf.extend(_build_len_prefixed(job.get("tags", "").encode()))

        # [payload_len:u16][payload_bytes]
        payload = job.get("payload", b"")
        buf.extend(struct.pack("<H", len(payload)))
        buf.extend(payload)

        # [lease_token:u64LE]
        buf.extend(struct.pack("<Q", job["lease_token"]))

    return bytes(buf)


def _capture_ack_payload(acks: list[AckJob]) -> bytes:
    """Encode an ack batch and return just the payload bytes (no frame header).

    Replicates the encoding logic from Conn.ack_batch / Conn.send_ack
    so we can inspect the raw bytes without a socket.
    """
    buf = bytearray()
    buf.extend(struct.pack("<H", len(acks)))

    for ack in acks:
        _append_len_prefixed(buf, ack.job_id.encode())
        _append_len_prefixed(buf, ack.queue.encode())
        buf.append(ack.ack_status)

        flags = 0
        if ack.result:
            flags |= 0x01
        if ack.checkpoint:
            flags |= 0x02
        if ack.hold_reason:
            flags |= 0x04
        if ack.lease_token:
            flags |= 0x08
        buf.append(flags)

        if flags & 0x01:
            _append_len_prefixed(buf, ack.result.encode())
        if flags & 0x02:
            _append_len_prefixed(buf, ack.checkpoint.encode())
        if flags & 0x04:
            _append_len_prefixed(buf, ack.hold_reason.encode())
        if flags & 0x08:
            buf.extend(struct.pack("<Q", ack.lease_token))

    return bytes(buf)


def _capture_fail_payload(jobs: list[FailJob]) -> bytes:
    """Encode a fail batch and return just the payload bytes (no frame header).

    Replicates the encoding logic from Conn.fail_batch / Conn.send_fail.
    """
    buf = bytearray()
    buf.extend(struct.pack("<H", len(jobs)))

    for job in jobs:
        _append_len_prefixed(buf, job.job_id.encode())
        _append_len_prefixed(buf, job.queue.encode())
        _append_len_prefixed(buf, job.error.encode())
        _append_len_prefixed(buf, job.backtrace.encode())

        flags = 0
        if job.lease_token:
            flags |= 0x01
        buf.append(flags)

        if flags & 0x01:
            buf.extend(struct.pack("<Q", job.lease_token))

    return bytes(buf)


def _capture_sent_payload(conn_method, *args) -> bytes:
    """Call a Conn method that sends data, capturing the raw bytes sent over the wire.

    Returns only the payload portion (strips the 9-byte frame header).
    """
    captured = bytearray()

    mock_sock = MagicMock()
    mock_sock.sendall = lambda data: captured.extend(data)

    conn = Conn("127.0.0.1", 9999)
    conn._sock = mock_sock

    # For send_ack / send_fail (fire-and-forget), no response needed
    conn_method(conn, *args)

    # Strip frame header (9 bytes) to get the payload
    assert len(captured) >= FRAME_HEADER_SIZE
    return bytes(captured[FRAME_HEADER_SIZE:])


# ---------------------------------------------------------------------------
# 1. Fetch response parsing -- lease_token decoded correctly
# ---------------------------------------------------------------------------

class TestFetchResponseParsing:
    """Test _decode_fetch_response with hand-crafted binary buffers."""

    def test_single_job_with_lease_token(self):
        """A single job with a non-zero lease_token should be parsed correctly."""
        token = 0xDEADBEEF_CAFEBABE
        raw = _build_fetch_response_payload([{
            "id": "job-1",
            "queue": "emails",
            "attempt": 1,
            "max_retries": 3,
            "checkpoint": "",
            "tags": "",
            "payload": b'{"hello":"world"}',
            "lease_token": token,
        }])

        conn = Conn.__new__(Conn)
        jobs = conn._decode_fetch_response(memoryview(bytearray(raw)))

        assert len(jobs) == 1
        job = jobs[0]
        assert job["id"] == "job-1"
        assert job["queue"] == "emails"
        assert job["attempt"] == 1
        assert job["max_retries"] == 3
        assert job["checkpoint"] == ""
        assert job["tags"] == ""
        assert job["payload"] == b'{"hello":"world"}'
        assert job["lease_token"] == token

    def test_multiple_jobs_with_lease_tokens(self):
        """Multiple jobs each get their own lease_token."""
        raw = _build_fetch_response_payload([
            {
                "id": "job-a",
                "queue": "q1",
                "attempt": 1,
                "max_retries": 5,
                "checkpoint": "cp-1",
                "tags": "env:prod",
                "payload": b"data-a",
                "lease_token": 1001,
            },
            {
                "id": "job-b",
                "queue": "q2",
                "attempt": 2,
                "max_retries": 10,
                "checkpoint": "",
                "tags": "",
                "payload": b"",
                "lease_token": 2002,
            },
        ])

        conn = Conn.__new__(Conn)
        jobs = conn._decode_fetch_response(memoryview(bytearray(raw)))

        assert len(jobs) == 2
        assert jobs[0]["id"] == "job-a"
        assert jobs[0]["lease_token"] == 1001
        assert jobs[0]["checkpoint"] == "cp-1"
        assert jobs[0]["tags"] == "env:prod"
        assert jobs[0]["payload"] == b"data-a"

        assert jobs[1]["id"] == "job-b"
        assert jobs[1]["lease_token"] == 2002
        assert jobs[1]["payload"] == b""

    def test_lease_token_zero(self):
        """lease_token=0 is a valid value and should parse to 0."""
        raw = _build_fetch_response_payload([{
            "id": "job-z",
            "queue": "q",
            "attempt": 0,
            "max_retries": 0,
            "checkpoint": "",
            "tags": "",
            "payload": b"",
            "lease_token": 0,
        }])

        conn = Conn.__new__(Conn)
        jobs = conn._decode_fetch_response(memoryview(bytearray(raw)))

        assert len(jobs) == 1
        assert jobs[0]["lease_token"] == 0

    def test_lease_token_max_u64(self):
        """lease_token at max u64 value should round-trip correctly."""
        max_u64 = (1 << 64) - 1
        raw = _build_fetch_response_payload([{
            "id": "job-max",
            "queue": "q",
            "attempt": 0,
            "max_retries": 0,
            "checkpoint": "",
            "tags": "",
            "payload": b"",
            "lease_token": max_u64,
        }])

        conn = Conn.__new__(Conn)
        jobs = conn._decode_fetch_response(memoryview(bytearray(raw)))

        assert jobs[0]["lease_token"] == max_u64

    def test_empty_response(self):
        """An empty response (count=0) should return an empty list."""
        raw = struct.pack("<H", 0)  # count=0

        conn = Conn.__new__(Conn)
        jobs = conn._decode_fetch_response(memoryview(bytearray(raw)))

        assert jobs == []

    def test_response_too_short(self):
        """A response shorter than 2 bytes should return an empty list."""
        conn = Conn.__new__(Conn)
        jobs = conn._decode_fetch_response(memoryview(bytearray(b"\x00")))
        assert jobs == []

    def test_lease_token_byte_layout(self):
        """Verify the exact byte position of lease_token in the response buffer."""
        token = 0x0102030405060708
        raw = _build_fetch_response_payload([{
            "id": "j",
            "queue": "q",
            "attempt": 0,
            "max_retries": 0,
            "checkpoint": "",
            "tags": "",
            "payload": b"",
            "lease_token": token,
        }])

        # The last 8 bytes of the buffer should be the lease_token in LE
        expected_token_bytes = struct.pack("<Q", token)
        assert raw[-8:] == expected_token_bytes


# ---------------------------------------------------------------------------
# 2. Ack request encoding -- lease_token flag and value
# ---------------------------------------------------------------------------

class TestAckEncoding:
    """Test ack batch encoding with lease_token."""

    def test_ack_with_lease_token_sets_flag(self):
        """When lease_token is non-zero, flag 0x08 must be set."""
        token = 0xAAAABBBBCCCCDDDD
        ack = AckJob(job_id="job-1", queue="emails", lease_token=token)
        payload = _capture_ack_payload([ack])

        # Parse: [count:u16][job_id:lenPrefixed][queue:lenPrefixed][ack_status:u8][flags:u8]...
        pos = 0
        count = struct.unpack_from("<H", payload, pos)[0]
        pos += 2
        assert count == 1

        # Skip job_id
        jid_len = payload[pos]
        pos += 1 + jid_len

        # Skip queue
        q_len = payload[pos]
        pos += 1 + q_len

        # ack_status
        ack_status = payload[pos]
        pos += 1
        assert ack_status == 0  # ACK_DONE

        # flags
        flags = payload[pos]
        pos += 1
        assert flags & 0x08 != 0, "flag 0x08 should be set when lease_token is non-zero"

        # lease_token should be the last 8 bytes
        token_value = struct.unpack_from("<Q", payload, pos)[0]
        assert token_value == token

    def test_ack_with_lease_token_exact_bytes(self):
        """Verify the exact encoded bytes for a minimal ack with lease_token."""
        token = 42
        ack = AckJob(job_id="j1", queue="q1", lease_token=token)
        payload = _capture_ack_payload([ack])

        expected = bytearray()
        expected.extend(struct.pack("<H", 1))          # count=1
        expected.extend(b"\x02j1")                      # job_id: len=2, "j1"
        expected.extend(b"\x02q1")                      # queue: len=2, "q1"
        expected.append(0)                               # ack_status=0
        expected.append(0x08)                            # flags=0x08 (lease_token only)
        expected.extend(struct.pack("<Q", 42))           # lease_token=42

        assert payload == bytes(expected)

    def test_ack_with_all_optional_fields_and_lease_token(self):
        """Ack with result, checkpoint, hold_reason, and lease_token."""
        token = 999
        ack = AckJob(
            job_id="job-2",
            queue="work",
            ack_status=1,  # ACK_HOLD
            result="ok",
            checkpoint="cp",
            hold_reason="waiting",
            lease_token=token,
        )
        payload = _capture_ack_payload([ack])

        pos = 2  # skip count
        # skip job_id
        pos += 1 + payload[pos]
        # skip queue
        pos += 1 + payload[pos]
        # ack_status
        assert payload[pos] == 1  # ACK_HOLD
        pos += 1
        # flags
        flags = payload[pos]
        pos += 1
        assert flags == 0x01 | 0x02 | 0x04 | 0x08  # all four flags set

        # result
        r_len = payload[pos]
        pos += 1
        assert payload[pos:pos + r_len] == b"ok"
        pos += r_len

        # checkpoint
        c_len = payload[pos]
        pos += 1
        assert payload[pos:pos + c_len] == b"cp"
        pos += c_len

        # hold_reason
        h_len = payload[pos]
        pos += 1
        assert payload[pos:pos + h_len] == b"waiting"
        pos += h_len

        # lease_token
        lt = struct.unpack_from("<Q", payload, pos)[0]
        assert lt == token

    def test_send_ack_on_conn_sets_flag(self):
        """Conn.send_ack with lease_token produces flag 0x08 in the sent bytes."""
        token = 0x1234
        ack = AckJob(job_id="j", queue="q", lease_token=token)
        payload = _capture_sent_payload(Conn.send_ack, [ack])

        # Parse the payload the same way
        pos = 2  # skip count
        pos += 1 + payload[pos]  # skip job_id
        pos += 1 + payload[pos]  # skip queue
        pos += 1  # skip ack_status

        flags = payload[pos]
        pos += 1
        assert flags == 0x08

        lt = struct.unpack_from("<Q", payload, pos)[0]
        assert lt == token


# ---------------------------------------------------------------------------
# 3. Fail request encoding -- lease_token flag and value
# ---------------------------------------------------------------------------

class TestFailEncoding:
    """Test fail batch encoding with lease_token."""

    def test_fail_with_lease_token_sets_flag(self):
        """When lease_token is non-zero, flag 0x01 must be set in fail encoding."""
        token = 0xBEEF
        job = FailJob(job_id="job-f1", queue="q", error="boom", lease_token=token)
        payload = _capture_fail_payload([job])

        pos = 2  # skip count
        # skip job_id
        pos += 1 + payload[pos]
        # skip queue
        pos += 1 + payload[pos]
        # skip error
        pos += 1 + payload[pos]
        # skip backtrace (empty)
        pos += 1 + payload[pos]

        flags = payload[pos]
        pos += 1
        assert flags == 0x01, f"expected flag 0x01 for lease_token in fail, got 0x{flags:02x}"

        lt = struct.unpack_from("<Q", payload, pos)[0]
        assert lt == token

    def test_fail_with_lease_token_exact_bytes(self):
        """Verify exact encoded bytes for a fail with lease_token."""
        token = 7777
        job = FailJob(job_id="f1", queue="q1", error="err", lease_token=token)
        payload = _capture_fail_payload([job])

        expected = bytearray()
        expected.extend(struct.pack("<H", 1))            # count=1
        expected.extend(b"\x02f1")                        # job_id
        expected.extend(b"\x02q1")                        # queue
        expected.extend(b"\x03err")                       # error
        expected.extend(b"\x00")                          # backtrace (empty)
        expected.append(0x01)                             # flags=0x01
        expected.extend(struct.pack("<Q", 7777))          # lease_token

        assert payload == bytes(expected)

    def test_fail_with_backtrace_and_lease_token(self):
        """Fail with both backtrace and lease_token."""
        token = 12345
        job = FailJob(
            job_id="f2",
            queue="q",
            error="oops",
            backtrace="line1\nline2",
            lease_token=token,
        )
        payload = _capture_fail_payload([job])

        pos = 2  # skip count
        pos += 1 + payload[pos]  # skip job_id
        pos += 1 + payload[pos]  # skip queue
        pos += 1 + payload[pos]  # skip error
        # backtrace
        bt_len = payload[pos]
        pos += 1
        assert payload[pos:pos + bt_len] == b"line1\nline2"
        pos += bt_len

        flags = payload[pos]
        pos += 1
        assert flags == 0x01

        lt = struct.unpack_from("<Q", payload, pos)[0]
        assert lt == token

    def test_send_fail_on_conn_sets_flag(self):
        """Conn.send_fail with lease_token produces flag 0x01 in the sent bytes."""
        token = 0x9999
        job = FailJob(job_id="j", queue="q", error="e", lease_token=token)
        payload = _capture_sent_payload(Conn.send_fail, [job])

        pos = 2  # skip count
        pos += 1 + payload[pos]  # skip job_id
        pos += 1 + payload[pos]  # skip queue
        pos += 1 + payload[pos]  # skip error
        pos += 1 + payload[pos]  # skip backtrace

        flags = payload[pos]
        pos += 1
        assert flags == 0x01

        lt = struct.unpack_from("<Q", payload, pos)[0]
        assert lt == token


# ---------------------------------------------------------------------------
# 4. Backward compatibility -- lease_token=0 should NOT set flags
# ---------------------------------------------------------------------------

class TestBackwardCompat:
    """Ensure lease_token=0 does not alter the flags byte."""

    def test_ack_lease_token_zero_no_flag(self):
        """Ack with lease_token=0 should NOT set flag 0x08."""
        ack = AckJob(job_id="j1", queue="q1", lease_token=0)
        payload = _capture_ack_payload([ack])

        pos = 2  # skip count
        pos += 1 + payload[pos]  # skip job_id
        pos += 1 + payload[pos]  # skip queue
        pos += 1  # skip ack_status

        flags = payload[pos]
        assert flags & 0x08 == 0, "flag 0x08 must NOT be set when lease_token=0"

    def test_ack_lease_token_zero_no_extra_bytes(self):
        """Ack with lease_token=0 should be the same size as without lease_token."""
        ack_with = AckJob(job_id="j1", queue="q1", lease_token=0)
        ack_without = AckJob(job_id="j1", queue="q1")

        payload_with = _capture_ack_payload([ack_with])
        payload_without = _capture_ack_payload([ack_without])

        assert payload_with == payload_without, \
            "lease_token=0 should produce identical bytes to no lease_token"

    def test_fail_lease_token_zero_no_flag(self):
        """Fail with lease_token=0 should NOT set flag 0x01."""
        job = FailJob(job_id="f1", queue="q1", error="err", lease_token=0)
        payload = _capture_fail_payload([job])

        pos = 2  # skip count
        pos += 1 + payload[pos]  # skip job_id
        pos += 1 + payload[pos]  # skip queue
        pos += 1 + payload[pos]  # skip error
        pos += 1 + payload[pos]  # skip backtrace

        flags = payload[pos]
        assert flags & 0x01 == 0, "flag 0x01 must NOT be set when lease_token=0"

    def test_fail_lease_token_zero_no_extra_bytes(self):
        """Fail with lease_token=0 should be the same size as without lease_token."""
        job_with = FailJob(job_id="f1", queue="q1", error="err", lease_token=0)
        job_without = FailJob(job_id="f1", queue="q1", error="err")

        payload_with = _capture_fail_payload([job_with])
        payload_without = _capture_fail_payload([job_without])

        assert payload_with == payload_without, \
            "lease_token=0 should produce identical bytes to no lease_token"

    def test_ack_with_result_but_no_lease_token(self):
        """Ack with result but lease_token=0 should only set flag 0x01."""
        ack = AckJob(job_id="j1", queue="q1", result="done", lease_token=0)
        payload = _capture_ack_payload([ack])

        pos = 2  # skip count
        pos += 1 + payload[pos]  # skip job_id
        pos += 1 + payload[pos]  # skip queue
        pos += 1  # skip ack_status

        flags = payload[pos]
        assert flags == 0x01, f"expected flags=0x01 (result only), got 0x{flags:02x}"
