"""Optional gRPC transport for CorvoClient.

When use_rpc=True, enqueue/fail/heartbeat route through gRPC unary calls.
Other methods (search, bulk, getJob, queue management) remain HTTP-only.
"""
from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional, Tuple

import grpc

from .gen.corvo.v1 import worker_pb2, worker_pb2_grpc


class ClientRpc:
    """gRPC unary client for CorvoClient methods that have proto equivalents."""

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

        target = self._url
        for prefix in ("http://", "https://", "grpc://"):
            if target.startswith(prefix):
                target = target[len(prefix):]
                break

        if self._url.startswith("http://"):
            self._channel = grpc.insecure_channel(target)
        else:
            self._channel = grpc.secure_channel(target, grpc.ssl_channel_credentials())

        self._stub = worker_pb2_grpc.WorkerServiceStub(self._channel)

    def _metadata(self) -> List[Tuple[str, str]]:
        md: List[Tuple[str, str]] = []
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

    def enqueue(self, queue: str, payload: Any) -> Dict[str, Any]:
        payload_json = json.dumps(payload) if payload is not None else "{}"
        req = worker_pb2.EnqueueRequest(queue=queue, payload_json=payload_json)
        resp = self._stub.Enqueue(req, metadata=self._metadata())
        return {
            "job_id": resp.job_id,
            "status": resp.status,
            "unique_existing": resp.unique_existing,
        }

    def fail(self, job_id: str, error: str, backtrace: str = "") -> Dict[str, Any]:
        req = worker_pb2.FailRequest(job_id=job_id, error=error, backtrace=backtrace)
        resp = self._stub.Fail(req, metadata=self._metadata())
        return {"status": resp.status}

    def heartbeat(self, jobs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        proto_jobs = {}
        for jid, update in jobs.items():
            proto_jobs[jid] = worker_pb2.HeartbeatJobUpdate(
                progress_json=json.dumps(update.get("progress", {})) if update.get("progress") else "",
                checkpoint_json=json.dumps(update.get("checkpoint", {})) if update.get("checkpoint") else "",
            )
        req = worker_pb2.HeartbeatRequest(jobs=proto_jobs)
        resp = self._stub.Heartbeat(req, metadata=self._metadata())
        return {"jobs": {jid: {"status": jr.status} for jid, jr in resp.jobs.items()}}

    def close(self) -> None:
        self._channel.close()
