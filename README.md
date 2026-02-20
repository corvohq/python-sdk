# Corvo Python SDK

Python client and worker runtime for [Corvo](https://corvo.dev), the fast job queue.

## Packages

- `corvo-client` — HTTP client for the Corvo API
- `corvo-worker` — Worker runtime for processing jobs

## Installation

```bash
pip install corvo-client
# For worker:
pip install corvo-worker
```

## Quick Start

### Client

```python
from corvo_client import CorvoClient

client = CorvoClient("http://localhost:7080", api_key="your-key")
result = client.enqueue("emails", {"to": "user@example.com"})
print("Enqueued:", result["job_id"])
```

### Worker

```python
from corvo_client import CorvoClient
from corvo_worker import CorvoWorker, WorkerConfig

client = CorvoClient("http://localhost:7080", api_key="your-key")
worker = CorvoWorker(client, WorkerConfig(
    queues=["emails"],
    worker_id="worker-1",
    concurrency=5,
))

def handle_email(job, ctx):
    print(f"Processing: {job['job_id']}")
    ctx.progress(1, 1, "Done")

worker.register("emails", handle_email)
worker.start()
```

## Local Development

```bash
pip install -e ./client -e ./worker
```

## Authentication

```python
# API Key
CorvoClient(url, api_key="key")

# Bearer token
CorvoClient(url, bearer_token="token")

# Dynamic token provider
CorvoClient(url, token_provider=lambda: get_token())

# Custom headers
CorvoClient(url, headers={"X-Custom": "value"})
```

## License

MIT
