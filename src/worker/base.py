from __future__ import annotations

import abc
import asyncio
import dataclasses
import json
import logging
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, TypedDict

import redis
import requests
from fastapi import FastAPI, Request

from ..cache.types import CachedResult, WorkerCapability
from ..coordinator.types import (
    WorkerHeartbeat,
    WorkerRegistrationRequest,
    WorkerTaskCompleteResponse,
    WorkerTaskCompleteTaskRequest,
    WorkerTasksGetResponseNoTask,
    WorkerTasksGetResponseSuccess,
)
from .types import CancelTaskResponse, HealthCheckResponse
from .utils import format_error

if TYPE_CHECKING:
    from concurrent.futures import Future
    from types import FrameType
    from typing import Callable, Iterable, Never, ParamSpec, Protocol

    P = ParamSpec("P")

    class IndexableContainer[KeyType, ReturnType](Protocol):
        def __getitem__(self, key: KeyType) -> ReturnType: ...
        def keys(self) -> Iterable[KeyType]: ...


# Configure logging
logger = logging.getLogger("worker")


# Configuration from environment variables
@dataclasses.dataclass
class Arguments:
    COORDINATOR_URL: str = os.environ.get("COORDINATOR_URL", "http://coordinator:8000")
    CACHE_HOST: str = os.environ.get("CACHE_HOST", "cache")
    CACHE_PORT: int = int(os.environ.get("CACHE_PORT", 6379))
    WORKER_ID: str = os.environ.get("WORKER_ID", "auto")
    HEARTBEAT_INTERVAL: int = int(os.environ.get("HEARTBEAT_INTERVAL", 10))
    POOL_SIZE: int = int(os.environ.get("POOL_SIZE", 5))

    def __post_init__(self):
        # Loop through the fields
        for field in dataclasses.fields(self):
            # If there is a default and the value of the field is none we can assign a value
            if (
                not isinstance(field.default, dataclasses._MISSING_TYPE)
                and getattr(self, field.name) is None
            ):
                setattr(self, field.name, field.default)

        # Generate a unique worker ID if not provided
        if self.WORKER_ID == "auto":
            self.WORKER_ID = f"worker-{uuid.uuid4()}"


@dataclasses.dataclass
class TaskStatus:
    running: bool
    start_time: float = dataclasses.field(default_factory=time.time)
    future: Future[bool] | None = dataclasses.field(default=None)


class Task(TypedDict):
    task_id: str
    method: str
    parameters: str
    lease_expires_at: float


class BaseWorker:
    def __init__(self, args: Arguments | None = None):
        if args is None:
            args = Arguments()
        self.args = args

        logger.info(f"Worker ID: {self.args.WORKER_ID}")

        # Worker start time
        self.start_time: float = time.time()

        # Tracking currently executing tasks
        self.active_tasks: dict[str, TaskStatus] = {}
        self.running: bool = False

        # Initialize Redis connection for result caching
        self.cache = redis.Redis(
            host=self.args.CACHE_HOST, port=self.args.CACHE_PORT, decode_responses=True
        )

        # Initialize thread pool executor
        self.executor = ThreadPoolExecutor(max_workers=self.args.POOL_SIZE)

    @abc.abstractmethod
    def supported_methods(self) -> IndexableContainer[str, Callable[[str], str]]:
        """Return a list of supported computation methods."""
        raise NotImplementedError

    def register_worker(self) -> bool:
        """Register this worker with the coordinator."""
        try:
            response = requests.post(
                f"{self.args.COORDINATOR_URL}/workers",
                json=WorkerRegistrationRequest(
                    worker_id=self.args.WORKER_ID,
                    capabilities=WorkerCapability(
                        cpu=self.args.POOL_SIZE,
                        methods=list(self.supported_methods().keys()),
                    ),
                ),
            )
            if response.status_code == 200:
                logger.info(f"Worker {self.args.WORKER_ID} registered successfully")
                return True
            else:
                logger.error(f"Failed to register worker: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error registering worker: {str(e)}")
            return False

    def unregister_worker(self) -> bool:
        """Deregister this worker from the coordinator."""
        try:
            response = requests.delete(
                f"{self.args.COORDINATOR_URL}/workers/{self.args.WORKER_ID}"
            )
            if response.status_code == 200:
                logger.info(f"Worker {self.args.WORKER_ID} deregistered successfully")
                return True
            else:
                logger.warning(f"Failed to deregister worker: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error deregistering worker: {str(e)}")
            return False

    def send_heartbeat(self) -> bool:
        """Send a heartbeat to the coordinator."""
        try:
            response = requests.put(
                f"{self.args.COORDINATOR_URL}/workers/{self.args.WORKER_ID}",
                json=WorkerHeartbeat(
                    worker_id=self.args.WORKER_ID,
                    active_tasks=list(self.active_tasks.keys()),
                ),
            )
            if response.status_code == 200:
                logger.debug(f"Heartbeat sent successfully")
                return True
            else:
                logger.warning(f"Failed to send heartbeat: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error sending heartbeat: {str(e)}")
            return False

    def fetch_task(self) -> Task | None:
        """Fetch a task from the coordinator."""
        try:
            response = requests.get(
                f"{self.args.COORDINATOR_URL}/workers/{self.args.WORKER_ID}/tasks"
            )
            if response.status_code == 200:
                task: WorkerTasksGetResponseNoTask | WorkerTasksGetResponseSuccess = (
                    response.json()
                )
                if task and "task_id" in task:
                    logger.info(f"Received task {task['task_id']}")
                    return Task(
                        task_id=task["task_id"],
                        method=task["method"],
                        parameters=task["parameters"],
                        lease_expires_at=task["lease_expires_at"],
                    )
                else:
                    logger.debug(f"No tasks available: {task['reason']}")
                    return None
            else:
                logger.warning(f"Failed to fetch task: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching task: {str(e)}")
            return None

    def report_result(self, task_id: str, result: CachedResult, success: bool) -> bool:
        """Report the task result back to the coordinator."""
        try:
            response = requests.put(
                f"{self.args.COORDINATOR_URL}/workers/{self.args.WORKER_ID}/tasks/{task_id}",
                json=WorkerTaskCompleteTaskRequest(
                    task_id=task_id,
                    success=success,
                    worker_id=result.get("worker_id"),
                    result=result.get("result"),
                    execution_time=result.get("execution_time"),
                    completed_at=result.get("completed_at"),
                ),
            )
            if response.status_code == 200:
                res: WorkerTaskCompleteResponse = response.json()
                logger.log(
                    logging.INFO if res["status"] == "success" else logging.WARNING,
                    f"Reported result for task {task_id}: {res["status"]}",
                )
                return True
            else:
                logger.warning(
                    f"Failed to report result for task {task_id}: {response.status_code}"
                )
                return False
        except Exception as e:
            logger.error(f"Error reporting result for task {task_id}: {str(e)}")
            return False

    def check_cache(self, task_id: str) -> CachedResult | None:
        """Check if the task result is already in the cache."""
        try:
            cached_result: CachedResult | dict[str, Never] = self.cache.hgetall(
                f"result:{task_id}"
            )
            if cached_result:
                logger.info(f"Cache hit for task {task_id}")
                return cached_result
            return None
        except Exception as e:
            logger.error(f"Error checking cache: {str(e)}")
            return None

    def store_result(self, task_id: str, result: CachedResult) -> bool:
        """Store the task result in the cache."""
        try:
            self.cache.hset(f"result:{task_id}", mapping=result)

            # Set cache expiration if needed (e.g., 1 day)
            # result_cache.expire(f"result:{task_id}", 86400)

            logger.info(f"Stored result for task {task_id} in cache")
            return True
        except Exception as e:
            logger.error(f"Error storing result: {str(e)}")
            return False

    def wrap_result(self, task_id: str, result: str) -> CachedResult:
        finish_time = time.time()
        return CachedResult(
            worker_id=self.args.WORKER_ID,
            completed_at=finish_time,
            execution_time=finish_time - self.active_tasks[task_id].start_time,
            result=result,
        )

    def process_task(self, task: Task) -> bool:
        """Process a task and return the result."""
        task_id = task["task_id"]
        task_method = task["method"]
        task_params = task.get("parameters", "")

        # Track this task as active
        self.active_tasks.setdefault(task_id, TaskStatus(running=True))

        try:
            # Check if the result is already in the cache
            result = self.check_cache(task_id)
            if result is not None:
                logger.info(f"Using cached result for task {task_id}")
                # Remove from active tasks
                self.active_tasks.pop(task_id, None)
                return self.report_result(task_id, result, True)

            # Process the task
            try:
                handler = self.supported_methods()[task_method]
            except KeyError:
                raise ValueError(f"Unsupported method: {task_method}")
            result = self.wrap_result(task_id, handler(task_params))

            # Store the result in the cache
            self.store_result(task_id, result)

            # Report the result to the coordinator
            success = self.report_result(task_id, result, True)

            return success
        except Exception as e:
            exc = RuntimeError(f"Error processing task {task_id}")
            exc.__cause__ = e
            logger.exception(exc)
            # Report the failure
            self.report_result(
                task_id,
                self.wrap_result(task_id, json.dumps({"error": format_error(exc)})),
                False,
            )
            return False
        finally:
            # Remove from active tasks
            self.active_tasks.pop(task_id, None)

    def health_check(self) -> HealthCheckResponse:
        """Report the health status of the worker."""
        return HealthCheckResponse(
            status="healthy",
            worker_id=self.args.WORKER_ID,
            active_tasks=len(self.active_tasks),
            uptime=time.time() - self.start_time,
        )

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a task that is currently executing."""
        task_status = self.active_tasks.get(task_id, None)
        if task_status is None:
            logger.warning(f"Task {task_id} not found in active tasks")
            return False
        logger.info(f"Cancelling task {task_id}")
        task_status.running = False
        if task_status.future is None:
            logger.warning(f"Future not found for task {task_id}")
            return False
        if task_status.future.done():
            logger.info(f"Task {task_id} already completed")
        elif not task_status.future.cancel():
            logger.warning(f"Failed to cancel task {task_id}")
            return False
        # Remove from active tasks
        self.active_tasks.pop(task_id, None)
        return True

    def handle_shutdown(self, sig: int = 0, frame: FrameType | None = None):
        """Handle graceful shutdown."""
        logger.info("Shutdown signal received, stopping worker...")
        self.running = False
        for task_id in self.active_tasks:
            self.cancel_task(task_id)
        # Wait for active tasks to complete (with a timeout)
        shutdown_wait = 30  # seconds
        start_wait = time.time()
        while self.active_tasks and time.time() - start_wait < shutdown_wait:
            logger.info(
                f"Waiting for {len(self.active_tasks)} active tasks to complete..."
            )
            time.sleep(1)

        # Deregister from coordinator
        self.unregister_worker()

        # Shutdown executor
        logger.info("Shutting down executor...")
        self.executor.shutdown(wait=False)

        if sig != 0:
            sys.exit(0)

    async def run(self):
        """Main worker loop."""
        self.start_time = time.time()
        self.running = True

        # Register with coordinator
        if not self.register_worker():
            logger.error("Failed to register worker, exiting")
            sys.exit(1)

        # Main loop
        last_heartbeat = 0.0

        while self.running:
            # Send heartbeat if needed
            current_time = time.time()
            if current_time - last_heartbeat >= self.args.HEARTBEAT_INTERVAL:
                self.send_heartbeat()
                last_heartbeat = current_time

            # Check for new tasks if we have capacity
            if len(self.active_tasks) < self.args.POOL_SIZE:
                task = self.fetch_task()
                if task is not None:
                    task_status = self.active_tasks.setdefault(
                        task["task_id"], TaskStatus(running=False)
                    )
                    # Submit task for execution
                    task_status.future = self.executor.submit(self.process_task, task)

            # Sleep to avoid hammering the coordinator
            await asyncio.sleep(1)

    @staticmethod
    def _get_fastapi(
        constructor: Callable[P, BaseWorker], *args: P.args, **kwargs: P.kwargs
    ) -> FastAPI:
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            worker = constructor(*args, **kwargs)
            try:
                asyncio.create_task(worker.run())
                yield {"worker": worker}
            finally:
                worker.handle_shutdown()

        app = FastAPI(lifespan=lifespan)

        @app.get("/health")
        async def health_check(request: Request) -> HealthCheckResponse:
            return request.state.worker.health_check()

        @app.post("/tasks/{task_id}/cancel")
        async def cancel_task(request: Request, task_id: str) -> CancelTaskResponse:
            if request.state.worker.cancel_task(task_id):
                return CancelTaskResponse(
                    status="cancelled",
                    message=f"Task {task_id} cancelled",
                )
            return CancelTaskResponse(
                status="error",
                message=f"Failed to cancel task {task_id}",
            )

        return app

    @classmethod
    def get_fastapi(cls, *args, **kwargs) -> FastAPI:
        return cls._get_fastapi(cls, *args, **kwargs)
