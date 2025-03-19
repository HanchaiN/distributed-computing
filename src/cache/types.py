from __future__ import annotations

from typing import NotRequired, TypedDict


class WorkerCapability(TypedDict):
    concurrency: int
    capacity: int
    methods: list[str]


class WorkerInfo(TypedDict):
    worker_id: str
    last_heartbeat: float
    capabilities: WorkerCapability
    active_tasks: list[str]


class CachedTask(TypedDict):
    task_id: str
    method: str
    parameters: str
    created_at: float
    assigned_to: NotRequired[str]
    lease_expires_at: NotRequired[float]
    retry_count: int
    max_retries: int


class CachedResult(TypedDict):
    worker_id: str
    completed_at: float
    execution_time: float
    result: str
