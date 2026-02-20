"""gRPC-based RPC client for the Corvo worker.

Mirrors the Go reference implementation at go-sdk/rpc/client.go.
Uses grpcio for full bidirectional streaming support.
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import grpc

# Generated proto stubs — run generate_proto.sh to produce these.
from .gen.corvo.v1 import worker_pb2, worker_pb2_grpc


# ---------------------------------------------------------------------------
# Error types
# ---------------------------------------------------------------------------

class ErrNotLeader(Exception):
    """Raised when the server returns NOT_LEADER."""

    def __init__(self, leader_addr: str = "") -> None:
        self.leader_addr = leader_addr
        msg = f"NOT_LEADER: leader is at {leader_addr}" if leader_addr else "NOT_LEADER: leader unknown"
        super().__init__(msg)


# ---------------------------------------------------------------------------
# RPC Client
# ---------------------------------------------------------------------------

SDK_NAME = "corvo-python"
SDK_VERSION = "0.2.0"


class RpcClient:
    """Typed gRPC client for the Corvo WorkerService."""

    def __init__(
        self,
        url: str,
        *,
        bearer_token: str = "",
        api_key: str = "",
        api_key_header: str = "X-API-Key",
        headers: Optional[Dict[str, str]] = None,
        token_provider: Optional[Callable[[], str]] = None,
    ) -> None:
        self._url = url.rstrip("/")
        self._bearer_token = bearer_token
        self._api_key = api_key
        self._api_key_header = api_key_header
        self._headers = headers or {}
        self._token_provider = token_provider

        # Create gRPC channel
        target = self._url
        # Strip scheme for grpc
        for prefix in ("http://", "https://", "grpc://"):
            if target.startswith(prefix):
                target = target[len(prefix):]
                break

        # Use insecure channel for http://, secure for https://
        if self._url.startswith("http://"):
            self._channel = grpc.insecure_channel(target)
        else:
            self._channel = grpc.secure_channel(target, grpc.ssl_channel_credentials())

        self._stub = worker_pb2_grpc.WorkerServiceStub(self._channel)

    def _metadata(self) -> List[Tuple[str, str]]:
        md: List[Tuple[str, str]] = []
        md.append(("x-corvo-client-name", SDK_NAME))
        md.append(("x-corvo-client-version", SDK_VERSION))
        for k, v in self._headers.items():
            md.append((k, v))
        if self._api_key:
            md.append((self._api_key_header.lower(), self._api_key))
        token = self._bearer_token
        if self._token_provider is not None:
            token = self._token_provider()
        if token:
            md.append(("authorization", f"Bearer {token}"))
        return md

    def enqueue(self, queue: str, payload_json: str) -> worker_pb2.EnqueueResponse:
        req = worker_pb2.EnqueueRequest(queue=queue, payload_json=payload_json)
        return self._stub.Enqueue(req, metadata=self._metadata())

    def fail(self, job_id: str, error: str, backtrace: str = "") -> worker_pb2.FailResponse:
        req = worker_pb2.FailRequest(job_id=job_id, error=error, backtrace=backtrace)
        return self._stub.Fail(req, metadata=self._metadata())

    def heartbeat(self, jobs: Dict[str, worker_pb2.HeartbeatJobUpdate]) -> Dict[str, str]:
        req = worker_pb2.HeartbeatRequest(jobs=jobs)
        resp = self._stub.Heartbeat(req, metadata=self._metadata())
        return {jid: jr.status for jid, jr in resp.jobs.items()}

    def open_lifecycle_stream(self) -> "LifecycleStream":
        return LifecycleStream(self._stub, self._metadata())

    def close(self) -> None:
        self._channel.close()


# ---------------------------------------------------------------------------
# LifecycleStream
# ---------------------------------------------------------------------------

@dataclass
class LifecycleRequest:
    request_id: int = 0
    queues: List[str] = field(default_factory=list)
    worker_id: str = ""
    hostname: str = ""
    lease_duration: int = 30
    fetch_count: int = 0
    acks: List[Dict[str, Any]] = field(default_factory=list)
    enqueues: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class LifecycleResponse:
    request_id: int = 0
    jobs: List[Any] = field(default_factory=list)
    acked: int = 0
    enqueued_job_ids: List[str] = field(default_factory=list)
    error: str = ""
    leader_addr: str = ""


class LifecycleStream:
    """Wraps a gRPC bidi stream with mutex-serialized exchange."""

    def __init__(
        self,
        stub: worker_pb2_grpc.WorkerServiceStub,
        metadata: List[Tuple[str, str]],
    ) -> None:
        self._mu = threading.Lock()
        self._closed = False
        self._request_iter = _RequestIterator()
        self._response_iter = stub.StreamLifecycle(
            self._request_iter, metadata=metadata,
        )

    def exchange(self, req: LifecycleRequest) -> LifecycleResponse:
        with self._mu:
            if self._closed:
                raise StopIteration("stream closed")

            # Build proto acks
            ack_items = []
            for a in req.acks:
                job_id = (a.get("job_id") or "").strip()
                if not job_id:
                    raise ValueError("job_id is required in ack")
                result_json = (a.get("result_json") or "{}").strip() or "{}"
                ack_items.append(worker_pb2.AckBatchItem(
                    job_id=job_id,
                    result_json=result_json,
                ))

            # Build proto enqueues
            enqueue_items = []
            for e in req.enqueues:
                queue = (e.get("queue") or "").strip()
                if not queue:
                    raise ValueError("enqueue queue is required")
                payload = (e.get("payload_json") or "{}").strip() or "{}"
                enqueue_items.append(worker_pb2.LifecycleEnqueueItem(
                    queue=queue,
                    payload_json=payload,
                ))

            proto_req = worker_pb2.LifecycleStreamRequest(
                request_id=req.request_id,
                queues=req.queues,
                worker_id=req.worker_id,
                hostname=req.hostname,
                lease_duration=req.lease_duration,
                fetch_count=req.fetch_count,
                acks=ack_items,
                enqueues=enqueue_items,
            )

            self._request_iter.put(proto_req)
            msg = next(self._response_iter)

            if msg.error == "NOT_LEADER":
                raise ErrNotLeader(msg.leader_addr)

            return LifecycleResponse(
                request_id=msg.request_id,
                jobs=list(msg.jobs),
                acked=msg.acked,
                enqueued_job_ids=list(msg.enqueued_job_ids),
                error=msg.error,
                leader_addr=msg.leader_addr,
            )

    def close(self) -> None:
        with self._mu:
            if self._closed:
                return
            self._closed = True
        self._request_iter.close()


class _RequestIterator:
    """Thread-safe iterator that feeds requests into the bidi stream."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._queue: List[Any] = []
        self._done = False

    def put(self, item: Any) -> None:
        with self._cond:
            self._queue.append(item)
            self._cond.notify()

    def close(self) -> None:
        with self._cond:
            self._done = True
            self._cond.notify()

    def __iter__(self) -> "_RequestIterator":
        return self

    def __next__(self) -> Any:
        with self._cond:
            while not self._queue and not self._done:
                self._cond.wait()
            if self._queue:
                return self._queue.pop(0)
            raise StopIteration


# ---------------------------------------------------------------------------
# ResilientLifecycleStream
# ---------------------------------------------------------------------------

class ResilientLifecycleStream:
    """Wraps LifecycleStream with automatic NOT_LEADER reconnection."""

    def __init__(
        self,
        url: str,
        *,
        bearer_token: str = "",
        api_key: str = "",
        api_key_header: str = "X-API-Key",
        headers: Optional[Dict[str, str]] = None,
        token_provider: Optional[Callable[[], str]] = None,
    ) -> None:
        self._url = url
        self._auth_kwargs = dict(
            bearer_token=bearer_token,
            api_key=api_key,
            api_key_header=api_key_header,
            headers=headers,
            token_provider=token_provider,
        )
        self._client = RpcClient(self._url, **self._auth_kwargs)
        self._stream = self._client.open_lifecycle_stream()

    def exchange(self, req: LifecycleRequest) -> LifecycleResponse:
        try:
            return self._stream.exchange(req)
        except ErrNotLeader as e:
            if not e.leader_addr:
                raise
            self._stream.close()
            self._client.close()
            self._url = e.leader_addr
            self._client = RpcClient(self._url, **self._auth_kwargs)
            self._stream = self._client.open_lifecycle_stream()
            return self._stream.exchange(req)

    def close(self) -> None:
        self._stream.close()
        self._client.close()
