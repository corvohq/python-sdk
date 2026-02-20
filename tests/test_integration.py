"""Integration tests for the Corvo Python SDK.

Run with: pytest tests/test_integration.py -v
Requires a running Corvo server (default: http://localhost:8080).
"""

import os
import threading
import time
import uuid

import pytest

from corvo_client import CorvoClient, EnqueueOptions
from corvo_worker import CorvoWorker, WorkerConfig

CORVO_URL = os.environ.get("CORVO_URL", "http://localhost:8080")


@pytest.fixture
def client():
    return CorvoClient(CORVO_URL, timeout=10.0)


def test_job_lifecycle(client):
    """Test enqueue -> fetch -> ack -> verify completed."""
    result = client.enqueue("py-test", {"hello": "world"})
    assert result["job_id"], "expected non-empty job_id"
    job_id = result["job_id"]

    job = client.fetch(["py-test"], "py-integration-worker", timeout=5)
    assert job is not None, "expected a job"
    assert job["job_id"] == job_id
    assert job["payload"]["hello"] == "world"

    client.ack(job_id)

    got = client.get_job(job_id)
    assert got["state"] == "completed", f"expected completed, got {got['state']}"


def test_enqueue_with_options(client):
    """Test enqueue_with using EnqueueOptions."""
    opts = EnqueueOptions(
        queue="py-opts-test",
        payload={"key": "value"},
        priority="high",
        max_retries=5,
        tags={"env": "test", "sdk": "python"},
    )
    result = client.enqueue_with(opts)
    job_id = result["job_id"]

    got = client.get_job(job_id)
    assert got["priority"] == 1, f"expected priority=1 (high), got {got['priority']}"
    assert got["max_retries"] == 5, f"expected max_retries=5, got {got['max_retries']}"

    # Clean up
    job = client.fetch(["py-opts-test"], "w1", timeout=3)
    if job:
        client.ack(job["job_id"])


def test_search(client):
    """Test job search."""
    r1 = client.enqueue("py-search-test", {"n": 1}, tags={"batch": "search-test"})
    r2 = client.enqueue("py-search-test", {"n": 2}, tags={"batch": "search-test"})

    result = client.search({"queue": "py-search-test", "state": ["pending"]})
    assert result["total"] >= 2, f"expected at least 2, got {result['total']}"

    # Clean up
    for _ in range(2):
        job = client.fetch(["py-search-test"], "w1", timeout=1)
        if job:
            client.ack(job["job_id"])


def test_fail_and_retry(client):
    """Test fail -> retry lifecycle."""
    result = client.enqueue("py-fail-test", {"x": 1}, max_retries=2)
    job_id = result["job_id"]

    job = client.fetch(["py-fail-test"], "w1", timeout=5)
    assert job is not None
    client.fail(job_id, "test error")

    # Wait for retry (default backoff ~5s)
    time.sleep(6)

    job2 = client.fetch(["py-fail-test"], "w1", timeout=5)
    assert job2 is not None, "expected retry job"
    assert job2["attempt"] == 2, f"expected attempt=2, got {job2['attempt']}"

    client.ack(job2["job_id"])

    got = client.get_job(job_id)
    assert got["state"] == "completed", f"expected completed, got {got['state']}"


def test_batch_enqueue(client):
    """Test batch enqueue."""
    result = client.enqueue_batch([
        {"queue": "py-batch-test", "payload": {"n": 1}},
        {"queue": "py-batch-test", "payload": {"n": 2}},
        {"queue": "py-batch-test", "payload": {"n": 3}},
    ])
    assert len(result["job_ids"]) == 3

    # Clean up
    for _ in range(3):
        job = client.fetch(["py-batch-test"], "w1", timeout=3)
        if job:
            client.ack(job["job_id"])


def test_worker(client):
    """Test the worker processes a job end-to-end."""
    queue = f"py-worker-test-{uuid.uuid4().hex[:8]}"
    result = client.enqueue(queue, {"task": "process"})
    job_id = result["job_id"]

    processed = threading.Event()
    processed_id = [None]

    def handler(job, ctx):
        processed_id[0] = job["job_id"]
        processed.set()

    worker = CorvoWorker(
        client,
        WorkerConfig(
            queues=[queue],
            worker_id="py-integration-test-worker",
            concurrency=1,
            shutdown_timeout_s=5.0,
        ),
    )
    worker.register(queue, handler)

    worker_thread = threading.Thread(target=worker.start, daemon=True)
    worker_thread.start()

    assert processed.wait(timeout=15), "timed out waiting for job"
    assert processed_id[0] == job_id

    got = client.get_job(job_id)
    assert got["state"] == "completed", f"expected completed, got {got['state']}"

    worker.stop()
    worker_thread.join(timeout=10)
