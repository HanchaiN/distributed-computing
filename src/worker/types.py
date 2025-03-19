from __future__ import annotations

from typing import TypedDict


class HealthCheckResponse(TypedDict):
    status: str
    worker_id: str
    active_tasks: int
    uptime: float


class CancelTaskResponse(TypedDict):
    status: str
    message: str
