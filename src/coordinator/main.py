from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import redis
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi import status as http_status

from ..cache.types import CachedResult, CachedTask, WorkerInfo
from .model import (
    CreateTaskRequest,
    CreateTaskResponse,
    GetTasksResponseComplete,
    GetTasksResponsePending,
    ListTasksResponse,
    ListWorkersResponse,
    WorkerHeartbeat,
    WorkerHeartbeatResponse,
    WorkerRegistrationRequest,
    WorkerRegistrationResponse,
    WorkerTaskCompleteResponse,
    WorkerTaskCompleteTaskRequest,
    WorkerTasksGetResponseNoTask,
    WorkerTasksGetResponseSuccess,
    WorkerUnregisterResponse,
)

if TYPE_CHECKING:
    from typing import Iterable, Literal


# Configuration from environment variables
QUEUE_HOST = os.getenv("QUEUE_HOST", "queue")
QUEUE_PORT = int(os.getenv("QUEUE_PORT", "6379"))
CACHE_HOST = os.getenv("CACHE_HOST", "cache")
CACHE_PORT = int(os.getenv("CACHE_PORT", "6379"))
WORKER_TIMEOUT = int(os.getenv("WORKER_TIMEOUT", "30"))  # seconds
TASK_LEASE_TIME = int(os.getenv("TASK_LEASE_TIME", "300"))  # seconds
PRESERVE_WORKER = os.getenv("PRESERVE_WORKER", "true").lower() == "true"
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))


async def retry_task(
    task_queue: redis.Redis,
    workers: dict[str, WorkerInfo],
    task_id: str,
    preserve_worker=False,
    update_worker=True,
) -> bool:
    """Retry a task if possible, or mark as failed if max retries exceeded."""
    current_time = time.time()
    # Get task data
    task_queue.exists(f"task:{task_id}")
    if not task_queue.exists(f"task:{task_id}"):
        return False
    if (
        preserve_worker
        and (worker_id := task_queue.hget(f"task:{task_id}", "assigned_to")) is not None
        and worker_id in workers
        and task_id in workers[worker_id]["active_tasks"]
    ):
        # Worker is still active, no need to retry
        return True  # Task not retried, but considered successful
    # Increment retry count
    retry_count = task_queue.hincrby(f"task:{task_id}", "retry_count", 1)
    max_retries = int(task_queue.hget(f"task:{task_id}", "max_retries") or MAX_RETRIES)

    if update_worker:
        # TODO: Notify worker about task expiry
        # Update worker info if still registered
        if worker_id in workers:
            workers[worker_id]["active_tasks"].remove(task_id)

    if retry_count > max_retries:
        # Max retries exceeded, mark as failed
        task_queue.zrem("in_progress_tasks", task_id)
        # task_queue.zadd("failed_tasks", {task_id: current_time})
        return False

    # Reset task assignment
    task_queue.hdel(f"task:{task_id}", "assigned_to")
    task_queue.hdel(f"task:{task_id}", "lease_expires_at")

    # Remove from in-progress, add back to pending
    task_queue.zrem("in_progress_tasks", task_id)
    task_queue.zadd("pending_tasks", {task_id: current_time})
    return True


async def remove_worker(
    task_queue: redis.Redis, workers: dict[str, WorkerInfo], worker_id: str
):
    """Remove a worker and reassign its tasks"""
    fut = []
    for task_id in workers[worker_id]["active_tasks"]:
        fut.append(
            asyncio.create_task(
                retry_task(
                    task_queue,
                    workers,
                    task_id,
                    update_worker=False,
                    preserve_worker=False,
                )
            )
        )

    # Remove expired worker
    del workers[worker_id]

    await asyncio.gather(*fut)


# Background task to monitor worker health and reassign tasks
async def monitor_workers(
    task_queue: redis.Redis, result_cache: redis.Redis, workers: dict[str, WorkerInfo]
):
    """Background task to monitor worker health and reassign tasks"""
    fut = []
    while True:
        try:
            current_time = time.time()

            # Check for pending tasks that is currently being processed
            pending_tasks: Iterable[str] = task_queue.zrange("pending_tasks", 0, 10)

            for task_id in pending_tasks:
                if not task_queue.exists(f"task:{task_id}") or result_cache.exists(
                    f"result:{task_id}"
                ):
                    task_queue.zrem("pending_tasks", task_id)
                    continue

            # Check for expired workers
            for worker_id, info in list(workers.items()):
                if current_time - info["last_heartbeat"] <= WORKER_TIMEOUT:
                    continue
                # Worker is considered dead
                fut.append(
                    asyncio.create_task(remove_worker(task_queue, workers, worker_id))
                )

            # Check for expired task leases
            expired_tasks: Iterable[str] = task_queue.zrangebyscore(
                "in_progress_tasks",
                0,
                current_time,
            )

            for task_id in expired_tasks:
                fut.append(
                    asyncio.create_task(
                        retry_task(
                            task_queue,
                            workers,
                            task_id,
                            update_worker=True,
                            preserve_worker=PRESERVE_WORKER,
                        )
                    )
                )
        except Exception as e:  # pylint:disable=broad-exception-caught
            exc = RuntimeError("Error in monitor_workers")
            exc.__cause__ = e
            logging.exception(exc)

        # Sleep for a while
        await asyncio.gather(*fut, asyncio.sleep(10))


@asynccontextmanager
async def lifespan(app: FastAPI):  # pylint:disable=unused-argument,redefined-outer-name
    # Redis connections
    task_queue = redis.Redis(
        host=QUEUE_HOST, port=QUEUE_PORT, db=0, decode_responses=True
    )
    result_cache = redis.Redis(
        host=CACHE_HOST, port=CACHE_PORT, db=0, decode_responses=True
    )

    # In-memory worker registry (could be moved to Redis for production)
    workers: dict[str, WorkerInfo] = {}
    try:
        # Start background tasks when the application starts
        asyncio.create_task(monitor_workers(task_queue, result_cache, workers))
        yield {
            "task_queue": task_queue,
            "result_cache": result_cache,
            "workers": workers,
        }
    finally:
        task_queue.close()
        result_cache.close()


app = FastAPI(title="Task System Coordinator", lifespan=lifespan)


@app.get("/health")
def health_check(request: Request):
    """Simple health check endpoint"""
    try:
        # Check Redis connections
        request.state.task_queue.ping()
        request.state.result_cache.ping()
        return {"status": "healthy", "redis_connected": True}
    except redis.exceptions.ConnectionError:
        return Response(status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE)


@app.get("/tasks")
async def list_tasks(request: Request) -> ListTasksResponse:
    """List all active tasks and available capabilities"""
    task_queue: redis.Redis = request.state.task_queue
    workers: dict[str, WorkerInfo] = request.state.workers

    capacity: dict[str, int] = {}
    concurrency: dict[str, int] = {}
    for worker in workers.values():
        for method in worker["capabilities"]["methods"]:
            capacity[method] = capacity.get(method, 0) + max(
                0, worker["capabilities"]["capacity"]
            )
            concurrency[method] = concurrency.get(method, 0) + max(
                0, worker["capabilities"]["concurrency"]
            )
    pending: list[str] = task_queue.zrange("pending_tasks", 0, -1)
    in_progress: list[str] = task_queue.zrange("in_progress_tasks", 0, -1)

    return ListTasksResponse(
        capacity={
            method: (capacity.get(method, 0), concurrency.get(method, 0))
            for method in set(capacity) | set(concurrency)
        },
        pending=pending,
        in_progress=in_progress,
    )


@app.post("/tasks", status_code=http_status.HTTP_201_CREATED)
async def create_task(request: Request, task: CreateTaskRequest) -> CreateTaskResponse:
    """Create a new task and add it to the queue"""
    task_queue: redis.Redis = request.state.task_queue
    result_cache: redis.Redis = request.state.result_cache

    # Generate deterministic ID based on parameters for idempotency
    import hashlib

    task_hash = hashlib.sha256(f"{task.method}-{task.parameters}".encode()).hexdigest()
    task_id = f"{task.method}:{task_hash}"

    # Check if result is already cached
    is_cached: int = result_cache.exists(f"result:{task_id}")
    if is_cached:
        # Result exists, no need to queue task
        return CreateTaskResponse(task_id=task_id, status="completed", cached=True)

    # Check if task is already in the queue
    if task_queue.exists(f"task:{task_id}"):
        return CreateTaskResponse(task_id=task_id, status="queued", queued=True)

    # Store task in Redis
    task_queue.hset(
        f"task:{task_id}",
        mapping=dict(
            CachedTask(
                task_id=task_id,
                method=task.method,
                parameters=task.parameters,
                created_at=time.time(),
                retry_count=0,
                max_retries=MAX_RETRIES,
            )
        ),
    )
    # Add to pending queue
    task_queue.zadd("pending_tasks", {task_id: time.time()})

    return CreateTaskResponse(task_id=task_id, status="queued")


@app.get("/tasks/{task_id}")
async def get_task_status(
    request: Request, task_id: str
) -> GetTasksResponseComplete | GetTasksResponsePending:
    """Get the status and result (if available) of a task"""
    task_queue: redis.Redis = request.state.task_queue
    result_cache: redis.Redis = request.state.result_cache
    # Check if task result is in cache
    if result_cache.exists(f"result:{task_id}"):
        [worker_id, completed_at, execution_time, result] = result_cache.hmget(
            f"result:{task_id}",
            ["worker_id", "completed_at", "execution_time", "result"],
        )
        return GetTasksResponseComplete(
            task_id=task_id,
            status="completed",
            assigned_to=worker_id,
            completed_at=float(completed_at),
            execution_time=float(execution_time),
            result=result,
        )

    # Check if task exists in queue
    if not task_queue.exists(f"task:{task_id}"):
        raise HTTPException(status_code=404, detail="Task not found")

    status: Literal["pending", "in_progress"] = "pending"
    if (worker_id := task_queue.hget(f"task:{task_id}", "assigned_to")) is not None:
        lease_expires = float(
            task_queue.hget(f"task:{task_id}", "lease_expires_at") or 0
        )
        if time.time() < lease_expires:
            status = "in_progress"
        else:
            status = "pending"  # Lease expired, task will be reassigned
    if (
        status == "in_progress"
        and task_queue.zscore("in_progress_tasks", task_id) is None
    ):
        status = "pending"  # Task not in in-progress set, reassign it
        task_queue.hset(f"task:{task_id}", "assigned_to", None)
    if status == "pending" and task_queue.zscore("pending_task", task_id) is None:
        # Task not in pending set, reassign it
        task_queue.zadd("pending_tasks", {task_id: time.time()})

    [created_at, retry_count] = task_queue.hmget(
        f"task:{task_id}",
        ["created_at", "retry_count"],
    )
    return GetTasksResponsePending(
        task_id=task_id,
        status=status,
        created_at=float(created_at),
        assigned_to=worker_id if status == "in_progress" else None,
        retry_count=int(retry_count),
    )


@app.get("/workers")
async def list_workers(request: Request) -> ListWorkersResponse:
    """List all registered workers and their status"""
    workers: dict[str, WorkerInfo] = request.state.workers
    current_time = time.time()

    # Filter out workers that haven't sent heartbeats
    active_workers = {}
    for worker_id, info in workers.items():
        if current_time - info["last_heartbeat"] <= WORKER_TIMEOUT:
            active_workers[worker_id] = info

    return ListWorkersResponse(workers=active_workers, count=len(active_workers))


@app.post("/workers")
async def register_worker(
    request: Request, registration: WorkerRegistrationRequest
) -> WorkerRegistrationResponse:
    """Register a new worker with the coordinator"""
    workers: dict[str, WorkerInfo] = request.state.workers

    worker_id = registration.worker_id or f"worker-{uuid.uuid4()}"
    if worker_id in workers:
        raise HTTPException(status_code=409, detail="Worker already registered")

    # Register the worker
    registration_time = time.time()
    workers[worker_id] = WorkerInfo(
        worker_id=worker_id,
        last_heartbeat=registration_time,
        capabilities=registration.capabilities,
        active_tasks=[],
    )

    return WorkerRegistrationResponse(
        worker_id=worker_id,
        registered_at=registration_time,
    )


@app.put("/workers/{worker_id}")
async def worker_heartbeat(
    request: Request, worker_id: str, heartbeat: WorkerHeartbeat
) -> WorkerHeartbeatResponse:
    """Process worker heartbeat and status update"""
    workers: dict[str, WorkerInfo] = request.state.workers

    if worker_id not in workers:
        raise HTTPException(status_code=404, detail="Worker not registered")

    # Update worker information
    heartbeat_time = time.time()
    worker_id = heartbeat.worker_id
    workers[worker_id]["last_heartbeat"] = heartbeat_time
    if logging.getLogger().level <= logging.INFO:
        new = set(heartbeat.active_tasks)
        old = set(workers[worker_id]["active_tasks"])
        if new != old:
            new, old = new - old, old - new
            for task_id in new:
                logging.info("Worker %s started task %s", worker_id, task_id)
            for task_id in old:
                logging.info("Worker %s stopped task %s", worker_id, task_id)
    workers[worker_id]["active_tasks"] = heartbeat.active_tasks

    return WorkerHeartbeatResponse(acknowledged=True, server_time=heartbeat_time)


@app.delete("/workers/{worker_id}")
async def unregister_worker(
    request: Request, worker_id: str
) -> WorkerUnregisterResponse:
    """Unregister a worker from the coordinator"""
    task_queue: redis.Redis = request.state.task_queue
    workers: dict[str, WorkerInfo] = request.state.workers

    if worker_id not in workers:
        raise HTTPException(status_code=404, detail="Worker not registered")

    await remove_worker(task_queue, workers, worker_id)
    return WorkerUnregisterResponse(worker_id=worker_id, unregistered_at=time.time())


@app.get("/workers/{worker_id}/tasks")
async def worker_get_task(
    request: Request, worker_id: str
) -> WorkerTasksGetResponseSuccess | WorkerTasksGetResponseNoTask:
    """Assign a task to a worker"""
    task_queue: redis.Redis = request.state.task_queue
    workers: dict[str, WorkerInfo] = request.state.workers

    if worker_id not in workers:
        raise HTTPException(status_code=404, detail="Worker not registered")

    # Only assign tasks to idle workers
    if (
        len(workers[worker_id]["active_tasks"])
        >= workers[worker_id]["capabilities"]["capacity"]
    ):
        return WorkerTasksGetResponseNoTask(status="no_task", reason="worker_busy")

    # Get the oldest pending task
    task_ids: list[tuple[str, float]] = task_queue.zrange(
        "pending_tasks",
        0,
        10,
        withscores=True,
    )
    if not task_ids:
        return WorkerTasksGetResponseNoTask(status="no_task", reason="queue_empty")

    reason: Literal["queue_empty", "no_compatible_method"] = "queue_empty"
    for task_id, _ in task_ids:
        if not task_queue.exists(f"task:{task_id}") or task_queue.hget(
            f"task:{task_id}", "assigned_to"
        ):
            # Task metadata missing / Task assigned, remove from pending set
            continue
        if (
            task_queue.hget(f"task:{task_id}", "method")
            in workers[worker_id]["capabilities"]["methods"]
        ):
            break
        reason = "no_compatible_method"
    else:
        return WorkerTasksGetResponseNoTask(status="no_task", reason=reason)

    # Update task with worker assignment
    lease_expires_at = time.time() + TASK_LEASE_TIME
    task_queue.hset(f"task:{task_id}", "assigned_to", worker_id)
    task_queue.hset(f"task:{task_id}", "lease_expires_at", str(lease_expires_at))

    # Remove from pending set
    task_queue.zrem("pending_tasks", task_id)
    # Add to in-progress set
    task_queue.zadd("in_progress_tasks", {task_id: lease_expires_at})

    # Update worker status
    workers[worker_id]["active_tasks"].append(task_id)

    [method, parameters] = task_queue.hmget(f"task:{task_id}", ["method", "parameters"])
    return WorkerTasksGetResponseSuccess(
        status="task_assigned",
        task_id=task_id,
        method=method,
        parameters=parameters,
        lease_expires_at=lease_expires_at,
    )


@app.put("/workers/{worker_id}/tasks/{task_id}")
async def worker_complete_task(
    request: Request,
    worker_id: str,
    task_id: str,
    result: WorkerTaskCompleteTaskRequest,
):
    """Process task completion from a worker"""
    task_queue: redis.Redis = request.state.task_queue
    result_cache: redis.Redis = request.state.result_cache
    workers: dict[str, WorkerInfo] = request.state.workers

    if worker_id not in workers:
        raise HTTPException(status_code=404, detail="Worker not registered")

    if not task_queue.exists(f"task:{task_id}"):
        raise HTTPException(status_code=404, detail="Task not found")

    if task_queue.hget(f"task:{task_id}", "assigned_to") != worker_id:
        raise HTTPException(status_code=403, detail="Task not assigned to this worker")

    if not result.success:
        # Task failed, retry if possible
        if await retry_task(task_queue, workers, task_id, update_worker=False):
            return WorkerTaskCompleteResponse(status="retried", task_id=task_id)
        return WorkerTaskCompleteResponse(status="failed", task_id=task_id)

    # Store result in cache
    received_time = time.time()
    result_cache.hset(
        f"result:{task_id}",
        mapping=dict(
            CachedResult(
                worker_id=worker_id,
                completed_at=result.completed_at or received_time,
                execution_time=(
                    result.execution_time
                    if result.execution_time is not None
                    else (
                        received_time
                        - float(task_queue.hget(f"task:{task_id}", "created_at"))
                    )
                ),
                result=result.result,
            )
        ),
    )

    # Set cache expiration if needed (e.g., 1 day)
    # result_cache.expire(f"result:{task_id}", 86400)

    # Remove task from in-progress set
    task_queue.zrem("in_progress_tasks", task_id)

    # Delete task metadata (optional, might keep for audit)
    task_queue.delete(f"task:{task_id}")

    # Update worker status
    if task_id in workers[worker_id]["active_tasks"]:
        workers[worker_id]["active_tasks"].remove(task_id)

    return WorkerTaskCompleteResponse(status="success", task_id=task_id)


if __name__ == "__main__":
    uvicorn.run("app.coordinator.main:app", host="0.0.0.0", port=8000, reload=True)
