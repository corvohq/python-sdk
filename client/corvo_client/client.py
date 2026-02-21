from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional

import requests


class CorvoError(Exception):
    pass


class PayloadTooLargeError(CorvoError):
    """Raised when a job payload exceeds the server's configured limit.

    This error is not retryable; the payload must be reduced before re-enqueuing.
    """
    pass


@dataclass
class ChainStep:
    queue: str
    payload: Any


@dataclass
class ChainConfig:
    steps: List[ChainStep]
    on_failure: Optional[str] = None
    on_exit: Optional[ChainStep] = None


@dataclass
class EnqueueOptions:
    queue: str
    payload: Any
    priority: Optional[str] = None
    unique_key: Optional[str] = None
    unique_period: Optional[int] = None
    max_retries: Optional[int] = None
    scheduled_at: Optional[str] = None
    tags: Optional[Dict[str, str]] = None
    expire_after: Optional[str] = None
    retry_backoff: Optional[str] = None
    retry_base_delay: Optional[str] = None
    retry_max_delay: Optional[str] = None
    chain: Optional[ChainConfig] = None


class CorvoClient:
    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        headers: Optional[Dict[str, str]] = None,
        bearer_token: str = "",
        api_key: str = "",
        api_key_header: str = "X-API-Key",
        token_provider: Optional[Callable[[], str]] = None,
        use_rpc: bool = False,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.headers = headers or {}
        self.bearer_token = bearer_token
        self.api_key = api_key
        self.api_key_header = api_key_header
        self.token_provider = token_provider
        self._use_rpc = use_rpc
        self._rpc_client: Optional[Any] = None

    def _get_rpc(self) -> Any:
        if self._rpc_client is None:
            from .rpc import ClientRpc
            self._rpc_client = ClientRpc(
                self.base_url,
                bearer_token=self.bearer_token,
                api_key=self.api_key,
                api_key_header=self.api_key_header,
                headers=dict(self.headers) if self.headers else None,
                token_provider=self.token_provider,
            )
        return self._rpc_client

    def enqueue(self, queue: str, payload: Any, **kwargs: Any) -> Dict[str, Any]:
        if self._use_rpc and not kwargs:
            return self._get_rpc().enqueue(queue, payload)
        body = {"queue": queue, "payload": payload}
        body.update(kwargs)
        return self._request("POST", "/api/v1/enqueue", body)

    def enqueue_with(self, opts: EnqueueOptions) -> Dict[str, Any]:
        body = {k: v for k, v in asdict(opts).items() if v is not None}
        return self._request("POST", "/api/v1/enqueue", body)

    def get_job(self, job_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/v1/jobs/{job_id}")

    def search(self, filt: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/api/v1/jobs/search", filt)

    def bulk(self, req: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/api/v1/jobs/bulk", req)

    def bulk_status(self, bulk_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/v1/bulk/{bulk_id}")

    def ack(self, job_id: str, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._request("POST", f"/api/v1/ack/{job_id}", body or {})

    def fetch(self, queues: list[str], worker_id: str, hostname: str = "corvo-worker", timeout: int = 30) -> Optional[Dict[str, Any]]:
        out = self._request(
            "POST",
            "/api/v1/fetch",
            {"queues": queues, "worker_id": worker_id, "hostname": hostname, "timeout": timeout},
        )
        if not out or not out.get("job_id"):
            return None
        return out

    def fail(self, job_id: str, error: str, backtrace: str = "") -> Dict[str, Any]:
        if self._use_rpc:
            return self._get_rpc().fail(job_id, error, backtrace)
        return self._request("POST", f"/api/v1/fail/{job_id}", {"error": error, "backtrace": backtrace})

    def heartbeat(self, jobs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        if self._use_rpc:
            return self._get_rpc().heartbeat(jobs)
        return self._request("POST", "/api/v1/heartbeat", {"jobs": jobs})

    def fetch_batch(self, queues: list[str], worker_id: str, hostname: str = "corvo-worker", timeout: int = 30, count: int = 10) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/api/v1/fetch/batch",
            {"queues": queues, "worker_id": worker_id, "hostname": hostname, "timeout": timeout, "count": count},
        )

    def ack_batch(self, acks: list[Dict[str, Any]]) -> Dict[str, Any]:
        return self._request("POST", "/api/v1/ack/batch", {"acks": acks})

    def enqueue_batch(self, jobs: list[Dict[str, Any]], batch: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        body: Dict[str, Any] = {"jobs": jobs}
        if batch is not None:
            body["batch"] = batch
        return self._request("POST", "/api/v1/enqueue/batch", body)

    def retry_job(self, job_id: str) -> Dict[str, Any]:
        return self._request("POST", f"/api/v1/jobs/{job_id}/retry")

    def cancel_job(self, job_id: str) -> Dict[str, Any]:
        return self._request("POST", f"/api/v1/jobs/{job_id}/cancel")

    def move_job(self, job_id: str, target_queue: str) -> Dict[str, Any]:
        return self._request("POST", f"/api/v1/jobs/{job_id}/move", {"queue": target_queue})

    def delete_job(self, job_id: str) -> Dict[str, Any]:
        return self._request("DELETE", f"/api/v1/jobs/{job_id}")

    def bulk_get_jobs(self, ids: List[str]) -> List[Dict[str, Any]]:
        result = self._request("POST", "/api/v1/jobs/bulk-get", {"job_ids": ids})
        return result.get("jobs", [])

    def subscribe(
        self,
        queues: Optional[List[str]] = None,
        job_ids: Optional[List[str]] = None,
        types: Optional[List[str]] = None,
        last_event_id: Optional[int] = None,
    ):
        """Stream lifecycle events via SSE. Yields dicts for each event."""
        import json as _json

        params: Dict[str, str] = {}
        if queues:
            params["queues"] = ",".join(queues)
        if job_ids:
            params["job_ids"] = ",".join(job_ids)
        if types:
            params["types"] = ",".join(types)
        if last_event_id is not None:
            params["last_event_id"] = str(last_event_id)

        url = self.base_url + "/api/v1/events"
        headers: Dict[str, str] = {}
        headers.update(self.headers)
        if self.api_key:
            headers[self.api_key_header or "X-API-Key"] = self.api_key
        token = self.bearer_token
        if self.token_provider is not None:
            token = self.token_provider()
        if token:
            headers["Authorization"] = f"Bearer {token}"

        resp = self.session.get(url, params=params, headers=headers, stream=True, timeout=None)
        if not resp.ok:
            raise CorvoError(f"SSE stream failed: HTTP {resp.status_code}")

        event_type = ""
        event_id = ""
        data_lines: List[str] = []

        for line in resp.iter_lines(decode_unicode=True):
            if line is None:
                continue
            if line.startswith("event: "):
                event_type = line[7:]
            elif line.startswith("id: "):
                event_id = line[4:]
            elif line.startswith("data: "):
                data_lines.append(line[6:])
            elif line == "":
                if data_lines:
                    try:
                        data = _json.loads("\n".join(data_lines))
                        yield {"type": event_type, "id": event_id, "data": data}
                    except ValueError:
                        pass
                event_type = ""
                event_id = ""
                data_lines = []

    def _request(self, method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = self.base_url + path
        headers = {"Content-Type": "application/json"}
        headers.update(self.headers)
        if self.api_key:
            headers[self.api_key_header or "X-API-Key"] = self.api_key
        token = self.bearer_token
        if self.token_provider is not None:
            token = self.token_provider()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        resp = self.session.request(method=method, url=url, json=body, timeout=self.timeout, headers=headers)
        if not resp.ok:
            msg = f"HTTP {resp.status_code}"
            code = ""
            try:
                data = resp.json()
                if isinstance(data, dict):
                    if data.get("error"):
                        msg = str(data["error"])
                    code = str(data.get("code", ""))
            except Exception:
                pass
            if code == "PAYLOAD_TOO_LARGE":
                raise PayloadTooLargeError(msg)
            raise CorvoError(msg)
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()
