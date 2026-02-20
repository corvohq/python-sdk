import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EnqueueRequest(_message.Message):
    __slots__ = ("queue", "payload_json", "agent")
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    AGENT_FIELD_NUMBER: _ClassVar[int]
    queue: str
    payload_json: str
    agent: AgentConfig
    def __init__(self, queue: _Optional[str] = ..., payload_json: _Optional[str] = ..., agent: _Optional[_Union[AgentConfig, _Mapping]] = ...) -> None: ...

class EnqueueResponse(_message.Message):
    __slots__ = ("job_id", "status", "unique_existing")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_EXISTING_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    status: str
    unique_existing: bool
    def __init__(self, job_id: _Optional[str] = ..., status: _Optional[str] = ..., unique_existing: bool = ...) -> None: ...

class FetchRequest(_message.Message):
    __slots__ = ("queues", "worker_id", "hostname", "lease_duration")
    QUEUES_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    LEASE_DURATION_FIELD_NUMBER: _ClassVar[int]
    queues: _containers.RepeatedScalarFieldContainer[str]
    worker_id: str
    hostname: str
    lease_duration: int
    def __init__(self, queues: _Optional[_Iterable[str]] = ..., worker_id: _Optional[str] = ..., hostname: _Optional[str] = ..., lease_duration: _Optional[int] = ...) -> None: ...

class FetchResponse(_message.Message):
    __slots__ = ("found", "job_id", "queue", "payload_json", "attempt", "max_retries", "lease_duration", "checkpoint_json", "tags_json", "agent")
    FOUND_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    MAX_RETRIES_FIELD_NUMBER: _ClassVar[int]
    LEASE_DURATION_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_JSON_FIELD_NUMBER: _ClassVar[int]
    TAGS_JSON_FIELD_NUMBER: _ClassVar[int]
    AGENT_FIELD_NUMBER: _ClassVar[int]
    found: bool
    job_id: str
    queue: str
    payload_json: str
    attempt: int
    max_retries: int
    lease_duration: int
    checkpoint_json: str
    tags_json: str
    agent: AgentState
    def __init__(self, found: bool = ..., job_id: _Optional[str] = ..., queue: _Optional[str] = ..., payload_json: _Optional[str] = ..., attempt: _Optional[int] = ..., max_retries: _Optional[int] = ..., lease_duration: _Optional[int] = ..., checkpoint_json: _Optional[str] = ..., tags_json: _Optional[str] = ..., agent: _Optional[_Union[AgentState, _Mapping]] = ...) -> None: ...

class FetchBatchRequest(_message.Message):
    __slots__ = ("queues", "worker_id", "hostname", "lease_duration", "count")
    QUEUES_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    LEASE_DURATION_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    queues: _containers.RepeatedScalarFieldContainer[str]
    worker_id: str
    hostname: str
    lease_duration: int
    count: int
    def __init__(self, queues: _Optional[_Iterable[str]] = ..., worker_id: _Optional[str] = ..., hostname: _Optional[str] = ..., lease_duration: _Optional[int] = ..., count: _Optional[int] = ...) -> None: ...

class FetchBatchJob(_message.Message):
    __slots__ = ("job_id", "queue", "payload_json", "attempt", "max_retries", "lease_duration", "checkpoint_json", "tags_json", "agent")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    MAX_RETRIES_FIELD_NUMBER: _ClassVar[int]
    LEASE_DURATION_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_JSON_FIELD_NUMBER: _ClassVar[int]
    TAGS_JSON_FIELD_NUMBER: _ClassVar[int]
    AGENT_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    queue: str
    payload_json: str
    attempt: int
    max_retries: int
    lease_duration: int
    checkpoint_json: str
    tags_json: str
    agent: AgentState
    def __init__(self, job_id: _Optional[str] = ..., queue: _Optional[str] = ..., payload_json: _Optional[str] = ..., attempt: _Optional[int] = ..., max_retries: _Optional[int] = ..., lease_duration: _Optional[int] = ..., checkpoint_json: _Optional[str] = ..., tags_json: _Optional[str] = ..., agent: _Optional[_Union[AgentState, _Mapping]] = ...) -> None: ...

class FetchBatchResponse(_message.Message):
    __slots__ = ("jobs",)
    JOBS_FIELD_NUMBER: _ClassVar[int]
    jobs: _containers.RepeatedCompositeFieldContainer[FetchBatchJob]
    def __init__(self, jobs: _Optional[_Iterable[_Union[FetchBatchJob, _Mapping]]] = ...) -> None: ...

class UsageReport(_message.Message):
    __slots__ = ("input_tokens", "output_tokens", "cache_creation_tokens", "cache_read_tokens", "model", "provider", "cost_usd")
    INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CACHE_CREATION_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CACHE_READ_TOKENS_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    COST_USD_FIELD_NUMBER: _ClassVar[int]
    input_tokens: int
    output_tokens: int
    cache_creation_tokens: int
    cache_read_tokens: int
    model: str
    provider: str
    cost_usd: float
    def __init__(self, input_tokens: _Optional[int] = ..., output_tokens: _Optional[int] = ..., cache_creation_tokens: _Optional[int] = ..., cache_read_tokens: _Optional[int] = ..., model: _Optional[str] = ..., provider: _Optional[str] = ..., cost_usd: _Optional[float] = ...) -> None: ...

class AckRequest(_message.Message):
    __slots__ = ("job_id", "result_json", "usage", "checkpoint_json", "agent_status", "hold_reason", "trace_json")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    RESULT_JSON_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_JSON_FIELD_NUMBER: _ClassVar[int]
    AGENT_STATUS_FIELD_NUMBER: _ClassVar[int]
    HOLD_REASON_FIELD_NUMBER: _ClassVar[int]
    TRACE_JSON_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    result_json: str
    usage: UsageReport
    checkpoint_json: str
    agent_status: str
    hold_reason: str
    trace_json: str
    def __init__(self, job_id: _Optional[str] = ..., result_json: _Optional[str] = ..., usage: _Optional[_Union[UsageReport, _Mapping]] = ..., checkpoint_json: _Optional[str] = ..., agent_status: _Optional[str] = ..., hold_reason: _Optional[str] = ..., trace_json: _Optional[str] = ...) -> None: ...

class AckResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class AckBatchItem(_message.Message):
    __slots__ = ("job_id", "result_json", "usage")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    RESULT_JSON_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    result_json: str
    usage: UsageReport
    def __init__(self, job_id: _Optional[str] = ..., result_json: _Optional[str] = ..., usage: _Optional[_Union[UsageReport, _Mapping]] = ...) -> None: ...

class AckBatchRequest(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[AckBatchItem]
    def __init__(self, items: _Optional[_Iterable[_Union[AckBatchItem, _Mapping]]] = ...) -> None: ...

class AckBatchResponse(_message.Message):
    __slots__ = ("acked",)
    ACKED_FIELD_NUMBER: _ClassVar[int]
    acked: int
    def __init__(self, acked: _Optional[int] = ...) -> None: ...

class LifecycleStreamRequest(_message.Message):
    __slots__ = ("request_id", "queues", "worker_id", "hostname", "lease_duration", "fetch_count", "acks", "enqueues")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    QUEUES_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    LEASE_DURATION_FIELD_NUMBER: _ClassVar[int]
    FETCH_COUNT_FIELD_NUMBER: _ClassVar[int]
    ACKS_FIELD_NUMBER: _ClassVar[int]
    ENQUEUES_FIELD_NUMBER: _ClassVar[int]
    request_id: int
    queues: _containers.RepeatedScalarFieldContainer[str]
    worker_id: str
    hostname: str
    lease_duration: int
    fetch_count: int
    acks: _containers.RepeatedCompositeFieldContainer[AckBatchItem]
    enqueues: _containers.RepeatedCompositeFieldContainer[LifecycleEnqueueItem]
    def __init__(self, request_id: _Optional[int] = ..., queues: _Optional[_Iterable[str]] = ..., worker_id: _Optional[str] = ..., hostname: _Optional[str] = ..., lease_duration: _Optional[int] = ..., fetch_count: _Optional[int] = ..., acks: _Optional[_Iterable[_Union[AckBatchItem, _Mapping]]] = ..., enqueues: _Optional[_Iterable[_Union[LifecycleEnqueueItem, _Mapping]]] = ...) -> None: ...

class LifecycleEnqueueItem(_message.Message):
    __slots__ = ("queue", "payload_json", "agent")
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    AGENT_FIELD_NUMBER: _ClassVar[int]
    queue: str
    payload_json: str
    agent: AgentConfig
    def __init__(self, queue: _Optional[str] = ..., payload_json: _Optional[str] = ..., agent: _Optional[_Union[AgentConfig, _Mapping]] = ...) -> None: ...

class AgentConfig(_message.Message):
    __slots__ = ("max_iterations", "max_cost_usd", "iteration_timeout")
    MAX_ITERATIONS_FIELD_NUMBER: _ClassVar[int]
    MAX_COST_USD_FIELD_NUMBER: _ClassVar[int]
    ITERATION_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    max_iterations: int
    max_cost_usd: float
    iteration_timeout: str
    def __init__(self, max_iterations: _Optional[int] = ..., max_cost_usd: _Optional[float] = ..., iteration_timeout: _Optional[str] = ...) -> None: ...

class AgentState(_message.Message):
    __slots__ = ("max_iterations", "max_cost_usd", "iteration_timeout", "iteration", "total_cost_usd")
    MAX_ITERATIONS_FIELD_NUMBER: _ClassVar[int]
    MAX_COST_USD_FIELD_NUMBER: _ClassVar[int]
    ITERATION_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    ITERATION_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COST_USD_FIELD_NUMBER: _ClassVar[int]
    max_iterations: int
    max_cost_usd: float
    iteration_timeout: str
    iteration: int
    total_cost_usd: float
    def __init__(self, max_iterations: _Optional[int] = ..., max_cost_usd: _Optional[float] = ..., iteration_timeout: _Optional[str] = ..., iteration: _Optional[int] = ..., total_cost_usd: _Optional[float] = ...) -> None: ...

class LifecycleStreamResponse(_message.Message):
    __slots__ = ("request_id", "jobs", "acked", "error", "enqueued_job_ids", "leader_addr")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    JOBS_FIELD_NUMBER: _ClassVar[int]
    ACKED_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ENQUEUED_JOB_IDS_FIELD_NUMBER: _ClassVar[int]
    LEADER_ADDR_FIELD_NUMBER: _ClassVar[int]
    request_id: int
    jobs: _containers.RepeatedCompositeFieldContainer[FetchBatchJob]
    acked: int
    error: str
    enqueued_job_ids: _containers.RepeatedScalarFieldContainer[str]
    leader_addr: str
    def __init__(self, request_id: _Optional[int] = ..., jobs: _Optional[_Iterable[_Union[FetchBatchJob, _Mapping]]] = ..., acked: _Optional[int] = ..., error: _Optional[str] = ..., enqueued_job_ids: _Optional[_Iterable[str]] = ..., leader_addr: _Optional[str] = ...) -> None: ...

class FailRequest(_message.Message):
    __slots__ = ("job_id", "error", "backtrace")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    BACKTRACE_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    error: str
    backtrace: str
    def __init__(self, job_id: _Optional[str] = ..., error: _Optional[str] = ..., backtrace: _Optional[str] = ...) -> None: ...

class FailResponse(_message.Message):
    __slots__ = ("status", "next_attempt_at", "attempts_remaining")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    NEXT_ATTEMPT_AT_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_REMAINING_FIELD_NUMBER: _ClassVar[int]
    status: str
    next_attempt_at: _timestamp_pb2.Timestamp
    attempts_remaining: int
    def __init__(self, status: _Optional[str] = ..., next_attempt_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., attempts_remaining: _Optional[int] = ...) -> None: ...

class HeartbeatJobUpdate(_message.Message):
    __slots__ = ("progress_json", "checkpoint_json", "usage", "stream_delta")
    PROGRESS_JSON_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_JSON_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    STREAM_DELTA_FIELD_NUMBER: _ClassVar[int]
    progress_json: str
    checkpoint_json: str
    usage: UsageReport
    stream_delta: str
    def __init__(self, progress_json: _Optional[str] = ..., checkpoint_json: _Optional[str] = ..., usage: _Optional[_Union[UsageReport, _Mapping]] = ..., stream_delta: _Optional[str] = ...) -> None: ...

class HeartbeatRequest(_message.Message):
    __slots__ = ("jobs",)
    class JobsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: HeartbeatJobUpdate
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[HeartbeatJobUpdate, _Mapping]] = ...) -> None: ...
    JOBS_FIELD_NUMBER: _ClassVar[int]
    jobs: _containers.MessageMap[str, HeartbeatJobUpdate]
    def __init__(self, jobs: _Optional[_Mapping[str, HeartbeatJobUpdate]] = ...) -> None: ...

class HeartbeatJobResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...

class HeartbeatResponse(_message.Message):
    __slots__ = ("jobs",)
    class JobsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: HeartbeatJobResponse
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[HeartbeatJobResponse, _Mapping]] = ...) -> None: ...
    JOBS_FIELD_NUMBER: _ClassVar[int]
    jobs: _containers.MessageMap[str, HeartbeatJobResponse]
    def __init__(self, jobs: _Optional[_Mapping[str, HeartbeatJobResponse]] = ...) -> None: ...
