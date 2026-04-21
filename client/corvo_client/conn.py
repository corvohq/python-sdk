"""Binary RPC connection for Corvo.

Implements the Corvo binary wire protocol over TCP with TCP_NODELAY.
This is the high-throughput path — no HTTP overhead, no JSON parsing.

Wire format (9-byte frame header):
    [msg_type:u8][req_id:u32LE][payload_len:u32LE][payload...]

Response frames have msg_type with the 0x80 bit set.
"""
from __future__ import annotations

import socket
import struct
from dataclasses import dataclass
from typing import Optional

# -- Protocol constants -------------------------------------------------------

FRAME_HEADER_SIZE = 9
FRAME_HEADER_FMT = "<BII"  # msg_type:u8, req_id:u32, payload_len:u32

# Request types
MSG_ENQUEUE_BATCH = 0x01
MSG_FETCH_BATCH = 0x02
MSG_ACK_BATCH = 0x03
MSG_PING = 0x04
MSG_HEARTBEAT = 0x06
MSG_FAIL_BATCH = 0x07

# Response types
MSG_ENQUEUE_BATCH_RESP = 0x81
MSG_FETCH_BATCH_RESP = 0x82
MSG_ACK_BATCH_RESP = 0x83
MSG_PONG = 0x84
MSG_HEARTBEAT_RESP = 0x86
MSG_FAIL_BATCH_RESP = 0x87
MSG_ERROR = 0xFF

MSG_CANCEL_SIGNAL = 0x08  # server -> client push

# Bulk action request/response
MSG_BULK_ACTION = 0x14
MSG_BULK_ACTION_RESP = 0x94

DEFAULT_LEASE_MS = 30_000

# -- Backoff constants --------------------------------------------------------

BACKOFF_NONE = 0
BACKOFF_FIXED = 1
BACKOFF_LINEAR = 2
BACKOFF_EXPONENTIAL = 3

# -- Ack status constants -----------------------------------------------------

ACK_DONE = 0
ACK_HOLD = 1


# -- Data types ---------------------------------------------------------------

@dataclass
class EnqueueJob:
    queue: str
    job_id: str
    priority: int = 2
    max_retries: int = 3
    backoff: int = 0
    base_delay_ms: int = 0
    max_delay_ms: int = 0
    unique_period_s: int = 0
    scheduled_at_ns: int = 0
    expire_after_ms: int = 0
    chain_step: int = 0
    payload: bytes = b""
    unique_key: str = ""
    tags: str = ""
    batch_id: str = ""
    chain_id: str = ""
    chain_config: str = ""
    group: str = ""
    parent_id: str = ""


@dataclass
class AckJob:
    job_id: str
    queue: str
    ack_status: int = 0  # ACK_DONE
    result: str = ""
    checkpoint: str = ""
    hold_reason: str = ""
    lease_token: int = 0


@dataclass
class FailJob:
    job_id: str
    queue: str
    error: str
    backtrace: str = ""
    lease_token: int = 0


@dataclass
class HeartbeatJob:
    job_id: str
    queue: str
    progress: str = ""
    checkpoint: str = ""


class RpcError(Exception):
    """Raised when the server returns an error frame."""
    pass


class ConnectionError_(Exception):
    """Raised when the TCP connection fails or is lost."""
    pass


# -- Binary encoding helpers --------------------------------------------------

def _append_len_prefixed(buf: bytearray, data: bytes) -> None:
    """Append [len:u8][bytes...] to buf."""
    n = len(data)
    if n > 255:
        raise ValueError(f"len-prefixed field too long: {n} > 255")
    buf.append(n)
    buf.extend(data)


def _append_u16_prefixed(buf: bytearray, data: bytes) -> None:
    """Append [len:u16LE][bytes...] to buf."""
    n = len(data)
    if n > 65535:
        raise ValueError(f"u16-prefixed field too long: {n} > 65535")
    buf.extend(struct.pack("<H", n))
    buf.extend(data)


def _read_len_prefixed(data: memoryview, pos: int) -> tuple[bytes, int]:
    """Read [len:u8][bytes...] from data at pos. Returns (value, new_pos)."""
    n = data[pos]
    pos += 1
    val = bytes(data[pos:pos + n])
    return val, pos + n


# -- Conn class ----------------------------------------------------------------

class Conn:
    """Low-level binary RPC connection to a Corvo server.

    Uses a single persistent TCP connection with TCP_NODELAY.
    Reconnects automatically on connection errors.

    Example::

        conn = Conn("127.0.0.1", 9878)
        conn.ping()
        n = conn.enqueue_batch([EnqueueJob(queue="emails", job_id="job-1")])
        conn.subscribe(["emails"], worker_id="w1", credits=10)
        jobs = conn.read_pushed_jobs()  # blocks until server pushes
        conn.ack_batch([AckJob(job_id=j["id"], queue="emails") for j in jobs])
        conn.close()
    """

    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._sock: Optional[socket.socket] = None
        self._req_id: int = 0
        self._recv_buf = bytearray(65536)

    def close(self) -> None:
        """Close the TCP connection."""
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def __enter__(self) -> "Conn":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    # -- Public API ------------------------------------------------------------

    def enqueue_batch(self, jobs: list[EnqueueJob]) -> int:
        """Enqueue a batch of jobs. Returns the number of jobs enqueued."""
        count = len(jobs)

        buf = bytearray()

        # [count:u16]
        buf.extend(struct.pack("<H", count))

        for job in jobs:
            queue_b = job.queue.encode()
            jid_b = job.job_id.encode()

            # Fixed fields per job.
            _append_len_prefixed(buf, queue_b)
            _append_len_prefixed(buf, jid_b)
            buf.append(job.priority)
            buf.extend(struct.pack("<H", job.max_retries))
            buf.append(job.backoff)
            buf.extend(struct.pack("<I", job.base_delay_ms))
            buf.extend(struct.pack("<I", job.max_delay_ms))
            buf.extend(struct.pack("<I", job.unique_period_s))
            buf.extend(struct.pack("<Q", job.scheduled_at_ns))
            buf.extend(struct.pack("<I", job.expire_after_ms))
            buf.extend(struct.pack("<H", job.chain_step))

            # Compute flags from non-empty optional fields.
            flags = 0
            if job.payload:
                flags |= 0x0001
            if job.unique_key:
                flags |= 0x0002
            if job.tags:
                flags |= 0x0004
            if job.batch_id:
                flags |= 0x0008
            if job.chain_id:
                flags |= 0x0010
            if job.chain_config:
                flags |= 0x0020
            if job.group:
                flags |= 0x0040
            if job.parent_id:
                flags |= 0x0080

            buf.extend(struct.pack("<H", flags))

            # Optional fields, written in flag order.
            if flags & 0x0001:
                # Payload uses u16 length prefix.
                _append_u16_prefixed(buf, job.payload)
            if flags & 0x0002:
                _append_len_prefixed(buf, job.unique_key.encode())
            if flags & 0x0004:
                _append_len_prefixed(buf, job.tags.encode())
            if flags & 0x0008:
                _append_len_prefixed(buf, job.batch_id.encode())
            if flags & 0x0010:
                _append_len_prefixed(buf, job.chain_id.encode())
            if flags & 0x0020:
                _append_len_prefixed(buf, job.chain_config.encode())
            if flags & 0x0040:
                _append_len_prefixed(buf, job.group.encode())
            if flags & 0x0080:
                _append_len_prefixed(buf, job.parent_id.encode())

        resp = self._send_recv(MSG_ENQUEUE_BATCH, MSG_ENQUEUE_BATCH_RESP, bytes(buf))

        # Response: [count:u16][err_code:u8]
        resp_count, err_code = struct.unpack_from("<HB", resp)
        if err_code != 0:
            raise RpcError(f"enqueue_batch failed: err_code={err_code}")
        return resp_count

    def subscribe(
        self,
        queues: list[str],
        worker_id: str = "",
        credits: int = 1,
        lease_ms: int = DEFAULT_LEASE_MS,
    ) -> None:
        """Subscribe to job pushes from the given queues (fire-and-forget).

        Sends a MSG_FETCH_BATCH frame with the given credit count. The server
        will push MSG_FETCH_BATCH_RESP frames asynchronously as jobs arrive.
        Use read_pushed_jobs() to receive them.

        ``credits`` controls how many jobs the server is allowed to push before
        the client must replenish by calling subscribe() again.
        """
        worker_b = worker_id.encode()

        buf = bytearray()

        # [credits:u16][lease_ms:u32][worker_id:lenPrefixed][queue_count:u8]
        buf.extend(struct.pack("<HI", credits, lease_ms))
        _append_len_prefixed(buf, worker_b)
        buf.append(len(queues))

        # [queues:lenPrefixed...]
        for q in queues:
            _append_len_prefixed(buf, q.encode())

        self._send_only(MSG_FETCH_BATCH, bytes(buf))

    def read_pushed_jobs(self) -> list[dict]:
        """Read pushed jobs from the server (blocking).

        Blocks until the server sends a MSG_FETCH_BATCH_RESP frame.
        Returns a list of dicts with keys: id, queue, attempt, max_retries,
        checkpoint, tags, payload, lease_token.

        Raises RpcError on server error or unexpected message type.
        """
        sock = self._ensure_connected()

        # Read frame header.
        resp_hdr = self._recv_exact(sock, FRAME_HEADER_SIZE)
        resp_type, resp_id, resp_len = struct.unpack(FRAME_HEADER_FMT, resp_hdr)

        # Read payload.
        if resp_len > 0:
            if resp_len > len(self._recv_buf):
                self._recv_buf = bytearray(resp_len)
            self._recv_exact_into(sock, self._recv_buf, resp_len)
            resp_payload = memoryview(self._recv_buf)[:resp_len]
        else:
            resp_payload = memoryview(self._recv_buf)[:0]

        if resp_type == MSG_ERROR:
            err_msg = bytes(resp_payload).decode("utf-8", errors="replace")
            raise RpcError(f"server error: {err_msg}")

        if resp_type != MSG_FETCH_BATCH_RESP:
            raise RpcError(
                f"unexpected message type: 0x{resp_type:02x}, "
                f"expected MSG_FETCH_BATCH_RESP (0x{MSG_FETCH_BATCH_RESP:02x})"
            )

        return self._decode_fetch_response(resp_payload)

    def ack_batch(self, acks: list[AckJob]) -> None:
        """Acknowledge a batch of jobs."""
        buf = bytearray()

        # [count:u16]
        buf.extend(struct.pack("<H", len(acks)))

        for ack in acks:
            # [job_id:lenPrefixed][queue:lenPrefixed][ack_status:u8][flags:u8]
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

        resp = self._send_recv(MSG_ACK_BATCH, MSG_ACK_BATCH_RESP, bytes(buf))

        # Response: [affected:u16][err_code:u8]
        _, err_code = struct.unpack_from("<HB", resp)
        if err_code != 0:
            raise RpcError(f"ack_batch failed: err_code={err_code}")

    def fail_batch(self, jobs: list[FailJob]) -> None:
        """Fail a batch of jobs with error messages."""
        buf = bytearray()

        # [count:u16]
        buf.extend(struct.pack("<H", len(jobs)))

        # per job: [id:lenPrefixed][queue:lenPrefixed][error:lenPrefixed][backtrace:lenPrefixed][flags:u8][if flag 0x01: lease_token:u64LE]
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

        resp = self._send_recv(MSG_FAIL_BATCH, MSG_FAIL_BATCH_RESP, bytes(buf))

        # Response: [affected:u16][err_code:u8]
        _, err_code = struct.unpack_from("<HB", resp)
        if err_code != 0:
            raise RpcError(f"fail_batch failed: err_code={err_code}")

    def heartbeat(self, worker_id: str, jobs: list[HeartbeatJob]) -> None:
        """Send heartbeat for active jobs to extend leases and update progress/checkpoint."""
        buf = bytearray()

        # [worker_id:lenPrefixed][count:u16]
        _append_len_prefixed(buf, worker_id.encode())
        buf.extend(struct.pack("<H", len(jobs)))

        # per job: [job_id:lenPrefixed][queue:lenPrefixed][flags:u8]...
        for job in jobs:
            _append_len_prefixed(buf, job.job_id.encode())
            _append_len_prefixed(buf, job.queue.encode())

            flags = 0
            if job.progress:
                flags |= 0x01
            if job.checkpoint:
                flags |= 0x02
            buf.append(flags)

            if flags & 0x01:
                _append_len_prefixed(buf, job.progress.encode())
            if flags & 0x02:
                _append_len_prefixed(buf, job.checkpoint.encode())

        resp = self._send_recv(MSG_HEARTBEAT, MSG_HEARTBEAT_RESP, bytes(buf))

        # Response: [affected:u16][err_code:u8]
        _, err_code = struct.unpack_from("<HB", resp)
        if err_code != 0:
            raise RpcError(f"heartbeat failed: err_code={err_code}")

    def ping(self) -> None:
        """Send a ping and wait for pong. Useful for connection health checks."""
        self._send_recv(MSG_PING, MSG_PONG, b"")

    def read_frame(self) -> dict:
        """Read the next frame from the server, dispatching by message type.

        Use in a message loop after subscribe() to handle interleaved
        FETCH_RESP, ACK_RESP, FAIL_RESP, and CANCEL_SIGNAL frames.

        Returns a dict with 'type' key and type-specific data:
          {'type': 'fetch_resp', 'jobs': [...]}
          {'type': 'ack_resp', 'affected': int}
          {'type': 'fail_resp', 'affected': int}
          {'type': 'cancel_signal', 'job_ids': [...]}
          {'type': 'pong'}
          {'type': 'error', 'message': str}
        """
        sock = self._ensure_connected()

        resp_hdr = self._recv_exact(sock, FRAME_HEADER_SIZE)
        resp_type, resp_id, resp_len = struct.unpack(FRAME_HEADER_FMT, resp_hdr)

        if resp_len > 0:
            if resp_len > len(self._recv_buf):
                self._recv_buf = bytearray(resp_len)
            self._recv_exact_into(sock, self._recv_buf, resp_len)
            resp_payload = memoryview(self._recv_buf)[:resp_len]
        else:
            resp_payload = memoryview(self._recv_buf)[:0]

        if resp_type == MSG_FETCH_BATCH_RESP:
            return {"type": "fetch_resp", "jobs": self._decode_fetch_response(resp_payload)}

        if resp_type == MSG_ACK_BATCH_RESP:
            affected = struct.unpack_from("<H", resp_payload)[0] if len(resp_payload) >= 2 else 0
            return {"type": "ack_resp", "affected": affected}

        if resp_type == MSG_FAIL_BATCH_RESP:
            affected = struct.unpack_from("<H", resp_payload)[0] if len(resp_payload) >= 2 else 0
            return {"type": "fail_resp", "affected": affected}

        if resp_type == MSG_CANCEL_SIGNAL:
            job_ids = []
            if len(resp_payload) >= 2:
                count = struct.unpack_from("<H", resp_payload)[0]
                pos = 2
                for _ in range(count):
                    jid, pos = _read_len_prefixed(resp_payload, pos)
                    job_ids.append(jid.decode())
            return {"type": "cancel_signal", "job_ids": job_ids}

        if resp_type == MSG_PONG:
            return {"type": "pong"}

        if resp_type == MSG_ERROR:
            err_msg = bytes(resp_payload).decode("utf-8", errors="replace")
            return {"type": "error", "message": err_msg}

        raise RpcError(f"unexpected message type: 0x{resp_type:02x}")

    def send_ack(self, acks: list[AckJob]) -> None:
        """Send ack batch without waiting for response (fire-and-forget).

        The response will arrive via read_frame() as {'type': 'ack_resp'}.
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

        self._send_only(MSG_ACK_BATCH, bytes(buf))

    def send_fail(self, jobs: list[FailJob]) -> None:
        """Send fail batch without waiting for response (fire-and-forget).

        The response will arrive via read_frame() as {'type': 'fail_resp'}.
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

        self._send_only(MSG_FAIL_BATCH, bytes(buf))

    def cancel(self, job_ids: list[str]) -> int:
        """Cancel jobs by ID. Returns the number of jobs cancelled.

        Sends MSG_BULK_ACTION with cancel action (request-response).
        """
        buf = bytearray()
        # [action:u8][queue:lenPrefixed][count:u16][{id:lenPrefixed}...][flags:u8][now_ns:u64]
        buf.append(3)  # BulkAction.cancel = 3
        _append_len_prefixed(buf, b"")  # queue (empty)
        buf.extend(struct.pack("<H", len(job_ids)))
        for jid in job_ids:
            _append_len_prefixed(buf, jid.encode())
        buf.append(0)  # flags
        buf.extend(struct.pack("<Q", 0))  # now_ns (server uses its own clock)

        resp = self._send_recv(MSG_BULK_ACTION, MSG_BULK_ACTION_RESP, bytes(buf))
        if len(resp) >= 2:
            return struct.unpack_from("<H", resp)[0]
        return 0

    # -- Internal --------------------------------------------------------------

    def _connect(self) -> socket.socket:
        """Create a new TCP connection with TCP_NODELAY."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.connect((self._host, self._port))
        except OSError:
            sock.close()
            raise
        return sock

    def _ensure_connected(self) -> socket.socket:
        """Return the current socket, reconnecting if needed."""
        if self._sock is None:
            self._sock = self._connect()
        return self._sock

    def _send_recv(
        self,
        msg_type: int,
        expected_resp_type: int,
        payload: bytes,
    ) -> memoryview:
        """Send a request frame and read the response frame.

        The payload is passed directly as bytes.
        Returns a memoryview over self._recv_buf containing the response payload.
        On connection error, reconnects once and retries.
        """
        for attempt in range(2):
            try:
                sock = self._ensure_connected()
                self._req_id = (self._req_id + 1) & 0xFFFFFFFF

                # Build frame header + payload in one sendall.
                header = struct.pack(FRAME_HEADER_FMT, msg_type, self._req_id, len(payload))
                if len(payload) > 0:
                    sock.sendall(header + payload)
                else:
                    sock.sendall(header)

                # Read response header.
                resp_hdr = self._recv_exact(sock, FRAME_HEADER_SIZE)
                resp_type, resp_id, resp_len = struct.unpack(FRAME_HEADER_FMT, resp_hdr)

                # Read response payload.
                if resp_len > 0:
                    if resp_len > len(self._recv_buf):
                        self._recv_buf = bytearray(resp_len)
                    self._recv_exact_into(sock, self._recv_buf, resp_len)
                    resp_payload = memoryview(self._recv_buf)[:resp_len]
                else:
                    resp_payload = memoryview(self._recv_buf)[:0]

                # Check for error response.
                if resp_type == MSG_ERROR:
                    err_msg = bytes(resp_payload).decode("utf-8", errors="replace")
                    raise RpcError(f"server error: {err_msg}")

                if resp_type != expected_resp_type:
                    raise RpcError(
                        f"unexpected response type: 0x{resp_type:02x}, "
                        f"expected 0x{expected_resp_type:02x}"
                    )

                return resp_payload

            except (OSError, ConnectionError_) as exc:
                self._drop_connection()
                if attempt == 0:
                    continue
                raise ConnectionError_(
                    f"connection to {self._host}:{self._port} failed: {exc}"
                ) from exc

        raise ConnectionError_(f"connection to {self._host}:{self._port} failed")

    def _send_only(self, msg_type: int, payload: bytes) -> None:
        """Send a request frame without reading a response (fire-and-forget).

        On connection error, reconnects once and retries.
        """
        for attempt in range(2):
            try:
                sock = self._ensure_connected()
                self._req_id = (self._req_id + 1) & 0xFFFFFFFF

                header = struct.pack(FRAME_HEADER_FMT, msg_type, self._req_id, len(payload))
                if len(payload) > 0:
                    sock.sendall(header + payload)
                else:
                    sock.sendall(header)
                return

            except (OSError, ConnectionError_) as exc:
                self._drop_connection()
                if attempt == 0:
                    continue
                raise ConnectionError_(
                    f"connection to {self._host}:{self._port} failed: {exc}"
                ) from exc

        raise ConnectionError_(f"connection to {self._host}:{self._port} failed")

    def _recv_exact(self, sock: socket.socket, n: int) -> bytes:
        """Read exactly n bytes from the socket."""
        chunks = []
        remaining = n
        while remaining > 0:
            chunk = sock.recv(remaining)
            if not chunk:
                raise ConnectionError_("connection closed by server")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _recv_exact_into(self, sock: socket.socket, buf: bytearray, n: int) -> None:
        """Read exactly n bytes from the socket into buf."""
        filled = 0
        while filled < n:
            nbytes = sock.recv_into(memoryview(buf)[filled:n])
            if nbytes == 0:
                raise ConnectionError_("connection closed by server")
            filled += nbytes

    def _drop_connection(self) -> None:
        """Close the socket without raising."""
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def _decode_fetch_response(self, resp: memoryview) -> list[dict]:
        """Decode a fetch response payload into a list of job dicts.

        Response format:
            [count:u16]
            per job:
                [id:lenPrefixed][queue:lenPrefixed]
                [attempt:u16][max_retries:u16]
                [checkpoint:lenPrefixed][tags:lenPrefixed]
                [payload_len:u16][payload_bytes]
                [lease_token:u64LE]
        """
        if len(resp) < 2:
            return []

        count = struct.unpack_from("<H", resp)[0]
        pos = 2
        jobs = []

        for _ in range(count):
            job_id, pos = _read_len_prefixed(resp, pos)
            queue, pos = _read_len_prefixed(resp, pos)

            attempt, max_retries = struct.unpack_from("<HH", resp, pos)
            pos += 4

            checkpoint, pos = _read_len_prefixed(resp, pos)
            tags, pos = _read_len_prefixed(resp, pos)

            payload_len = struct.unpack_from("<H", resp, pos)[0]
            pos += 2
            payload = bytes(resp[pos:pos + payload_len])
            pos += payload_len

            lease_token = struct.unpack_from("<Q", resp, pos)[0]
            pos += 8

            jobs.append({
                "id": job_id.decode(),
                "queue": queue.decode(),
                "attempt": attempt,
                "max_retries": max_retries,
                "checkpoint": checkpoint.decode() if checkpoint else "",
                "tags": tags.decode() if tags else "",
                "payload": payload,
                "lease_token": lease_token,
            })

        return jobs
