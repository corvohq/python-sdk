"""Auto-batching pooled client for Corvo.

Provides a ``Client`` class that wraps *N* internal ``CorvoClient`` instances
(lanes) and transparently batches individual ``enqueue()`` calls into bulk
HTTP requests.  Other operations (fetch, ack, ...) are passed through to the
underlying clients via round-robin.

Typical usage::

    from corvo_client import Client

    client = Client("http://localhost:7400", lanes=8)
    job_id = client.enqueue("emails", {"to": "a@b.com"})
    client.close()
"""

from __future__ import annotations

import threading
import time as _time
from dataclasses import asdict
from typing import Any, Callable, Dict, List, Optional

from .client import CorvoClient, EnqueueOptions


class _PendingEnqueue:
    """One caller waiting for its job to be flushed."""

    __slots__ = ("body", "result", "error", "done")

    def __init__(self, body: Dict[str, Any]) -> None:
        self.body = body
        self.result: Optional[Dict[str, Any]] = None
        self.error: Optional[Exception] = None
        self.done = threading.Event()


class _Lane:
    """A single lane: one CorvoClient, one buffer, one flush thread."""

    def __init__(self, corvo: CorvoClient, max_batch: int, flush_interval_s: float) -> None:
        self._client = corvo
        self._max_batch = max_batch
        self._flush_interval_s = flush_interval_s

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._pending: List[_PendingEnqueue] = []
        self._closed = False

        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    # -- public (called by Client) ------------------------------------------

    def submit(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Add *body* to the batch buffer and block until it is flushed.

        Returns the enqueue result dict (with ``job_id``).
        Raises whatever error the batch request raised.
        """
        entry = _PendingEnqueue(body)
        with self._cond:
            if self._closed:
                raise RuntimeError("Client is closed")
            self._pending.append(entry)
            if len(self._pending) >= self._max_batch:
                self._cond.notify_all()

        # Wait outside the lock so the flush thread can acquire it.
        entry.done.wait()

        if entry.error is not None:
            raise entry.error
        assert entry.result is not None
        return entry.result

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()
        self._thread.join()

    @property
    def client(self) -> CorvoClient:
        return self._client

    # -- flush thread --------------------------------------------------------

    def _run(self) -> None:
        while True:
            batch: List[_PendingEnqueue]

            with self._cond:
                # Wait until there is work, the batch is full, or we're closed.
                deadline = _time.monotonic() + self._flush_interval_s
                while not self._pending and not self._closed:
                    remaining = deadline - _time.monotonic()
                    if remaining <= 0:
                        break
                    self._cond.wait(timeout=remaining)

                if not self._pending and self._closed:
                    return

                # Swap the buffer out so new callers can start filling next batch.
                batch = self._pending
                self._pending = []

            if not batch:
                continue

            self._flush(batch)

    def _flush(self, batch: List[_PendingEnqueue]) -> None:
        jobs = [entry.body for entry in batch]

        try:
            resp = self._client.enqueue_batch(jobs)
        except Exception as exc:
            for entry in batch:
                entry.error = exc
                entry.done.set()
            return

        # enqueue_batch returns {"jobs": [{...}, ...]} on success.
        result_jobs: List[Dict[str, Any]] = resp.get("jobs", [])

        for i, entry in enumerate(batch):
            if i < len(result_jobs):
                rj = result_jobs[i]
                # Normalise to include a top-level job_id for convenience.
                if "id" in rj:
                    entry.result = {"job": rj, "job_id": rj["id"]}
                else:
                    entry.result = {"job": rj, "job_id": rj.get("job_id", "")}
            else:
                # Server returned fewer results than we sent -- treat as error.
                entry.error = RuntimeError(
                    f"batch response missing entry at index {i}"
                )
            entry.done.set()


class Client:
    """Auto-batching, connection-pooled Corvo client.

    Individual ``enqueue()`` calls from multiple threads are transparently
    grouped into batch HTTP requests (group-commit pattern).  All other
    operations are passed through to the underlying ``CorvoClient`` instances
    via round-robin.

    Parameters
    ----------
    base_url:
        Corvo server URL, e.g. ``"http://localhost:7400"``.
    lanes:
        Number of internal ``CorvoClient`` instances / flush threads.
    max_batch:
        Flush when a lane has accumulated this many enqueue entries.
    flush_interval_ms:
        Flush interval in milliseconds (even if the batch is not full).
    **kwargs:
        Forwarded to each ``CorvoClient`` constructor (``bearer_token``,
        ``api_key``, ``timeout``, ``headers``, etc.).
    """

    def __init__(
        self,
        base_url: str,
        *,
        lanes: int = 8,
        max_batch: int = 256,
        flush_interval_ms: float = 1,
        **kwargs: Any,
    ) -> None:
        if lanes < 1:
            raise ValueError("lanes must be >= 1")
        if max_batch < 1:
            raise ValueError("max_batch must be >= 1")

        flush_interval_s = flush_interval_ms / 1000.0

        self._lanes: List[_Lane] = []
        for _ in range(lanes):
            corvo = CorvoClient(base_url, **kwargs)
            self._lanes.append(_Lane(corvo, max_batch, flush_interval_s))

        self._counter = 0
        self._counter_lock = threading.Lock()
        self._closed = False

    # -- lane selection ------------------------------------------------------

    def _next_lane(self) -> _Lane:
        with self._counter_lock:
            idx = self._counter % len(self._lanes)
            self._counter += 1
        return self._lanes[idx]

    def _next_client(self) -> CorvoClient:
        return self._next_lane().client

    # -- auto-batched enqueue -----------------------------------------------

    def enqueue(self, queue: str, payload: Any, **kwargs: Any) -> str:
        """Enqueue a job, auto-batched with other concurrent callers.

        Returns the job ID (string).  Blocks until the batch containing this
        job has been flushed to the server.
        """
        body: Dict[str, Any] = {"queue": queue, "payload": payload}
        body.update(kwargs)
        result = self._next_lane().submit(body)
        return result.get("job_id", "")

    def enqueue_with(self, opts: EnqueueOptions) -> str:
        """Enqueue using an ``EnqueueOptions`` dataclass, auto-batched.

        Returns the job ID (string).
        """
        body = {k: v for k, v in asdict(opts).items() if v is not None}
        result = self._next_lane().submit(body)
        return result.get("job_id", "")

    # -- passthrough operations (round-robin) --------------------------------

    def fetch(
        self,
        queues: list[str],
        worker_id: str,
        hostname: str = "corvo-worker",
        timeout: int = 30,
    ) -> Optional[Dict[str, Any]]:
        return self._next_client().fetch(queues, worker_id, hostname=hostname, timeout=timeout)

    def ack(self, job_id: str, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._next_client().ack(job_id, body)

    def fail(self, job_id: str, error: str, backtrace: str = "") -> Dict[str, Any]:
        return self._next_client().fail(job_id, error, backtrace)

    def fetch_batch(
        self,
        queues: list[str],
        worker_id: str,
        hostname: str = "corvo-worker",
        timeout: int = 30,
        count: int = 10,
    ) -> Dict[str, Any]:
        return self._next_client().fetch_batch(queues, worker_id, hostname=hostname, timeout=timeout, count=count)

    def ack_batch(self, acks: list[Dict[str, Any]]) -> Dict[str, Any]:
        return self._next_client().ack_batch(acks)

    def enqueue_batch(self, jobs: list[Dict[str, Any]], batch: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Direct batch enqueue (bypasses auto-batching)."""
        return self._next_client().enqueue_batch(jobs, batch)

    def get_job(self, job_id: str) -> Dict[str, Any]:
        return self._next_client().get_job(job_id)

    def search(self, filt: Dict[str, Any]) -> Dict[str, Any]:
        return self._next_client().search(filt)

    def bulk(self, req: Dict[str, Any]) -> Dict[str, Any]:
        return self._next_client().bulk(req)

    def bulk_status(self, bulk_id: str) -> Dict[str, Any]:
        return self._next_client().bulk_status(bulk_id)

    def cancel_job(self, job_id: str) -> Dict[str, Any]:
        return self._next_client().cancel_job(job_id)

    def move_job(self, job_id: str, target_queue: str) -> Dict[str, Any]:
        return self._next_client().move_job(job_id, target_queue)

    def delete_job(self, job_id: str) -> Dict[str, Any]:
        return self._next_client().delete_job(job_id)

    def create_batch(self, callback_queue: str, callback_payload: Any = None) -> Dict[str, Any]:
        return self._next_client().create_batch(callback_queue, callback_payload)

    def seal_batch(self, batch_id: str) -> Dict[str, Any]:
        return self._next_client().seal_batch(batch_id)

    def get_server_info(self) -> Dict[str, Any]:
        return self._next_client().get_server_info()

    def bulk_get_jobs(self, ids: List[str]) -> List[Dict[str, Any]]:
        return self._next_client().bulk_get_jobs(ids)

    def heartbeat(self, jobs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        return self._next_client().heartbeat(jobs)

    def subscribe(
        self,
        queues: Optional[List[str]] = None,
        job_ids: Optional[List[str]] = None,
        types: Optional[List[str]] = None,
        last_event_id: Optional[int] = None,
    ):
        return self._next_client().subscribe(queues=queues, job_ids=job_ids, types=types, last_event_id=last_event_id)

    # -- lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """Stop all flush threads and flush remaining entries.

        Blocks until every pending enqueue has been flushed (or errored).
        """
        if self._closed:
            return
        self._closed = True
        for lane in self._lanes:
            lane.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __del__(self) -> None:
        # Best-effort cleanup when garbage collected.
        try:
            self.close()
        except Exception:
            pass
