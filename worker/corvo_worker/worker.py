from __future__ import annotations

import json
import queue
import signal
import socket
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from corvo_client import CorvoClient

Handler = Callable[[Dict[str, Any], "JobContext"], None]


@dataclass
class WorkerConfig:
    queues: list[str]
    worker_id: str
    hostname: str = socket.gethostname()
    concurrency: int = 10
    shutdown_timeout_s: float = 30.0
    fetch_batch_size: int = 1
    ack_batch_size: int = 1
    use_rpc: bool = False


class JobContext:
    def __init__(self, worker: "CorvoWorker", job_id: str) -> None:
        self._worker = worker
        self._job_id = job_id

    def is_cancelled(self) -> bool:
        with self._worker._mu:
            return self._worker._active.get(self._job_id, {}).get("cancelled", False)

    def checkpoint(self, data: Dict[str, Any]) -> None:
        self._worker.client.heartbeat({self._job_id: {"checkpoint": data}})

    def progress(self, current: int, total: int, message: str) -> None:
        self._worker.client.heartbeat(
            {self._job_id: {"progress": {"current": current, "total": total, "message": message}}}
        )


class CorvoWorker:
    def __init__(self, client: CorvoClient, cfg: WorkerConfig) -> None:
        self.client = client
        self.cfg = cfg
        self._handlers: Dict[str, Handler] = {}
        self._active: Dict[str, Dict[str, Any]] = {}
        self._mu = threading.Lock()
        self._stop = threading.Event()

    def register(self, queue: str, handler: Handler) -> None:
        self._handlers[queue] = handler

    def start(self) -> None:
        is_main = threading.current_thread() is threading.main_thread()
        if is_main:
            prev_int = signal.signal(signal.SIGINT, self._on_signal)
            prev_term = signal.signal(signal.SIGTERM, self._on_signal)
        try:
            if self.cfg.use_rpc:
                self._start_rpc()
            else:
                self._start_http()
        finally:
            if is_main:
                signal.signal(signal.SIGINT, prev_int)
                signal.signal(signal.SIGTERM, prev_term)

    def _start_http(self) -> None:
        hb = threading.Thread(target=self._heartbeat_loop, daemon=True)
        hb.start()
        workers = [threading.Thread(target=self._fetch_loop, daemon=True) for _ in range(self.cfg.concurrency)]
        for t in workers:
            t.start()
        for t in workers:
            t.join()

    def _start_rpc(self) -> None:
        from .rpc import ResilientLifecycleStream, LifecycleRequest, RpcClient

        # Create RPC client for unary calls (heartbeat, fail)
        auth_kwargs = self._rpc_auth_kwargs()
        rpc_client = RpcClient(self.client.base_url, **auth_kwargs)

        # RPC heartbeat thread
        hb = threading.Thread(target=self._rpc_heartbeat_loop, args=(rpc_client,), daemon=True)
        hb.start()

        # Single stream thread that feeds jobs to worker threads
        job_queue: queue.Queue[Dict[str, Any]] = queue.Queue(maxsize=self.cfg.concurrency)
        ack_queue: queue.Queue[Dict[str, str]] = queue.Queue()
        enqueue_queue: queue.Queue[Dict[str, str]] = queue.Queue()

        stream_thread = threading.Thread(
            target=self._rpc_stream_loop,
            args=(auth_kwargs, job_queue, ack_queue, enqueue_queue),
            daemon=True,
        )
        stream_thread.start()

        # Worker threads pull from job_queue
        worker_threads = [
            threading.Thread(
                target=self._rpc_worker_loop,
                args=(job_queue, ack_queue, rpc_client),
                daemon=True,
            )
            for _ in range(self.cfg.concurrency)
        ]
        for t in worker_threads:
            t.start()
        for t in worker_threads:
            t.join()

        rpc_client.close()

    def _rpc_auth_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {}
        if self.client.bearer_token:
            kwargs["bearer_token"] = self.client.bearer_token
        if self.client.api_key:
            kwargs["api_key"] = self.client.api_key
            kwargs["api_key_header"] = self.client.api_key_header
        if self.client.headers:
            kwargs["headers"] = dict(self.client.headers)
        if self.client.token_provider:
            kwargs["token_provider"] = self.client.token_provider
        return kwargs

    def _rpc_stream_loop(
        self,
        auth_kwargs: Dict[str, Any],
        job_queue: queue.Queue[Dict[str, Any]],
        ack_queue: queue.Queue[Dict[str, str]],
        enqueue_queue: queue.Queue[Dict[str, str]],
    ) -> None:
        from .rpc import ResilientLifecycleStream, LifecycleRequest

        stream = ResilientLifecycleStream(self.client.base_url, **auth_kwargs)
        request_id = 0

        try:
            while not self._stop.is_set():
                try:
                    # Drain ack and enqueue queues
                    acks = []
                    while True:
                        try:
                            acks.append(ack_queue.get_nowait())
                        except queue.Empty:
                            break

                    enqueues = []
                    while True:
                        try:
                            enqueues.append(enqueue_queue.get_nowait())
                        except queue.Empty:
                            break

                    with self._mu:
                        active_count = len(self._active)
                    fetch_count = max(0, self.cfg.concurrency - active_count)

                    request_id += 1
                    req = LifecycleRequest(
                        request_id=request_id,
                        queues=self.cfg.queues,
                        worker_id=self.cfg.worker_id,
                        hostname=self.cfg.hostname,
                        lease_duration=30,
                        fetch_count=fetch_count,
                        acks=acks,
                        enqueues=enqueues,
                    )

                    resp = stream.exchange(req)

                    for proto_job in resp.jobs:
                        job = _proto_job_to_dict(proto_job)
                        job_queue.put(job)

                except Exception:
                    if not self._stop.is_set():
                        time.sleep(1.0)
        finally:
            stream.close()

    def _rpc_worker_loop(
        self,
        job_queue: queue.Queue[Dict[str, Any]],
        ack_queue: queue.Queue[Dict[str, str]],
        rpc_client: Any,
    ) -> None:
        while not self._stop.is_set():
            try:
                job = job_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            job_id = str(job["job_id"])
            queue_name = str(job["queue"])
            handler = self._handlers.get(queue_name)

            if handler is None:
                ack_queue.put({"job_id": job_id, "result_json": "{}"})
                continue

            with self._mu:
                self._active[job_id] = {"cancelled": False}

            ctx = JobContext(self, job_id)
            try:
                handler(job, ctx)
                ack_queue.put({"job_id": job_id, "result_json": "{}"})
            except Exception as exc:
                try:
                    rpc_client.fail(job_id, str(exc))
                except Exception:
                    pass
            finally:
                with self._mu:
                    self._active.pop(job_id, None)

    def _rpc_heartbeat_loop(self, rpc_client: Any) -> None:
        from .gen.corvo.v1 import worker_pb2

        while not self._stop.is_set():
            time.sleep(15.0)
            if self._stop.is_set():
                return
            with self._mu:
                active = list(self._active.keys())
            if not active:
                continue
            jobs = {jid: worker_pb2.HeartbeatJobUpdate() for jid in active}
            try:
                result = rpc_client.heartbeat(jobs)
                with self._mu:
                    for jid, status in result.items():
                        if status == "cancel" and jid in self._active:
                            self._active[jid]["cancelled"] = True
            except Exception:
                pass

    def stop(self) -> None:
        self._stop.set()
        deadline = time.time() + self.cfg.shutdown_timeout_s
        while time.time() < deadline:
            with self._mu:
                if not self._active:
                    return
            time.sleep(0.1)

        with self._mu:
            remaining = list(self._active.keys())
        for job_id in remaining:
            try:
                self.client.fail(job_id, "worker_shutdown")
            except Exception:
                pass

    def _on_signal(self, signum: int, frame: Optional[object]) -> None:
        self.stop()

    # -----------------------------------------------------------------------
    # HTTP fetch loop (original)
    # -----------------------------------------------------------------------

    def _fetch_loop(self) -> None:
        ack_buffer: list[Dict[str, Any]] = []
        while not self._stop.is_set():
            try:
                self._flush_acks(ack_buffer)
                if self.cfg.fetch_batch_size > 1:
                    result = self.client.fetch_batch(
                        self.cfg.queues, self.cfg.worker_id, self.cfg.hostname,
                        timeout=30, count=self.cfg.fetch_batch_size,
                    )
                    jobs = result.get("jobs", []) if result else []
                else:
                    job = self.client.fetch(self.cfg.queues, self.cfg.worker_id, self.cfg.hostname, timeout=30)
                    jobs = [job] if job else []

                for job in jobs:
                    if not job:
                        continue
                    job_id = str(job["job_id"])
                    queue_name = str(job["queue"])

                    handler = self._handlers.get(queue_name)
                    if handler is None:
                        ack_buffer.append({"job_id": job_id})
                        continue

                    with self._mu:
                        self._active[job_id] = {"cancelled": False}

                    ctx = JobContext(self, job_id)
                    try:
                        handler(job, ctx)
                        ack_buffer.append({"job_id": job_id})
                    except Exception as exc:
                        self.client.fail(job_id, str(exc))
                    finally:
                        with self._mu:
                            self._active.pop(job_id, None)

                self._flush_acks(ack_buffer)
            except Exception:
                time.sleep(1.0)

    def _flush_acks(self, buffer: list[Dict[str, Any]]) -> None:
        while len(buffer) >= self.cfg.ack_batch_size and self.cfg.ack_batch_size > 1:
            batch = buffer[:self.cfg.ack_batch_size]
            del buffer[:self.cfg.ack_batch_size]
            self.client.ack_batch(batch)
        if buffer and self.cfg.ack_batch_size <= 1:
            for item in buffer:
                self.client.ack(item["job_id"], {})
            buffer.clear()

    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(15.0)
            if self._stop.is_set():
                return
            with self._mu:
                active = list(self._active.keys())
            if not active:
                continue
            jobs = {job_id: {} for job_id in active}
            try:
                result = self.client.heartbeat(jobs)
                statuses = result.get("jobs", {}) if isinstance(result, dict) else {}
                if isinstance(statuses, dict):
                    for job_id, state in statuses.items():
                        if isinstance(state, dict) and state.get("status") == "cancel":
                            with self._mu:
                                if job_id in self._active:
                                    self._active[job_id]["cancelled"] = True
            except Exception:
                pass


def _proto_job_to_dict(proto_job: Any) -> Dict[str, Any]:
    """Convert a proto FetchBatchJob to the dict format expected by handlers."""
    payload: Any = {}
    if proto_job.payload_json:
        try:
            payload = json.loads(proto_job.payload_json)
        except (json.JSONDecodeError, TypeError):
            pass
    checkpoint = None
    if proto_job.checkpoint_json:
        try:
            checkpoint = json.loads(proto_job.checkpoint_json)
        except (json.JSONDecodeError, TypeError):
            pass
    tags = None
    if proto_job.tags_json:
        try:
            tags = json.loads(proto_job.tags_json)
        except (json.JSONDecodeError, TypeError):
            pass

    return {
        "job_id": proto_job.job_id,
        "queue": proto_job.queue,
        "payload": payload,
        "attempt": proto_job.attempt,
        "max_retries": proto_job.max_retries,
        "lease_duration": proto_job.lease_duration,
        "checkpoint": checkpoint,
        "tags": tags,
    }
