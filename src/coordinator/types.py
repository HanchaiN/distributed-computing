from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

from ..cache.types import WorkerCapability, WorkerInfo

if TYPE_CHECKING:
    from typing import Literal, NotRequired

    from typing_extensions import TypeIs


# Data models
class ListTasksResponse(TypedDict):
    capacity: dict[str, tuple[int, int]]
    pending: list[str]
    in_progress: list[str]


class CreateTaskRequest(TypedDict):
    method: str
    parameters: str


class CreateTaskResponse(TypedDict):
    task_id: str
    status: Literal["completed", "queued"]
    queued: NotRequired[bool]  # default False
    cached: NotRequired[bool]  # default False


class GetTasksResponseComplete(TypedDict):
    task_id: str
    status: Literal["completed"]
    assigned_to: str
    completed_at: float
    execution_time: float
    result: str


class GetTasksResponsePending(TypedDict):
    task_id: str
    status: Literal["pending", "in_progress"]
    created_at: float
    assigned_to: NotRequired[str]  # default None
    retry_count: int


GetTasksResponse = GetTasksResponseComplete | GetTasksResponsePending


def get_tasks_response_is_complete(
    task: GetTasksResponse,
) -> TypeIs[GetTasksResponseComplete]:
    return task["status"] == "completed"


class ListWorkersResponse(TypedDict):
    count: int
    workers: dict[str, WorkerInfo]


class WorkerRegistrationRequest(TypedDict):
    worker_id: NotRequired[str]
    capabilities: WorkerCapability


class WorkerRegistrationResponse(TypedDict):
    worker_id: str
    registered_at: float


class WorkerHeartbeat(TypedDict):
    worker_id: str
    active_tasks: list[str]


class WorkerHeartbeatResponse(TypedDict):
    acknowledged: bool
    server_time: float


class WorkerUnregisterResponse(TypedDict):
    worker_id: str
    unregistered_at: float


class WorkerTasksGetResponseSuccess(TypedDict):
    status: Literal["task_assigned"]
    task_id: str
    method: str
    parameters: str
    lease_expires_at: float


class WorkerTasksGetResponseNoTask(TypedDict):
    status: Literal["no_task"]
    reason: Literal["worker_busy", "queue_empty", "no_compatible_method"]


WorkerTasksGetResponse = WorkerTasksGetResponseSuccess | WorkerTasksGetResponseNoTask


def worker_task_get_response_is_success(
    response: WorkerTasksGetResponse,
) -> TypeIs[WorkerTasksGetResponseSuccess]:
    return response["status"] == "task_assigned"


class WorkerTaskCompleteTaskRequest(TypedDict):
    task_id: str
    result: str
    worker_id: str
    success: bool
    execution_time: float  # in seconds
    completed_at: float


class WorkerTaskCompleteResponse(TypedDict):
    task_id: str
    status: Literal["success", "retried", "failed"]
