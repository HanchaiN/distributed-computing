from __future__ import annotations

import abc
import asyncio
import dataclasses
import json
import logging
import os
import random
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, asynccontextmanager
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
    WorkerTasksGetResponse,
    worker_task_get_response_is_success,
)
from .types import CancelTaskResponse, HealthCheckResponse
from .utils import catch_signal, format_error

if TYPE_CHECKING:
    from concurrent.futures import Future
    from types import FrameType
    from typing import Callable, Iterable, Never, ParamSpec, Protocol

    from typing_extensions import TypeIs

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
    PREFETCH: int = int(os.environ.get("PREFETCH", 2))
    MAX_RETRY: int = int(os.environ.get("MAX_RETRY", 3))

    def __post_init__(self):
        # Loop through the fields
        for field in dataclasses.fields(self):
            # If there is a default and the value of the field is none we can assign a value
            if (
                not isinstance(
                    field.default,
                    dataclasses._MISSING_TYPE,  # pylint: disable=protected-access
                )
                and getattr(self, field.name) is None
            ):
                setattr(self, field.name, field.default)

        # Generate a unique worker ID if not provided
        if self.WORKER_ID == "auto":
            self.WORKER_ID = f"worker-{uuid.uuid4()}"

    @property
    def CAPACITY(self) -> int:
        return self.POOL_SIZE + self.PREFETCH


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


class BaseWorker:  # pylint: disable=too-many-instance-attributes
    def __init__(self, args: Arguments | None = None):
        if args is None:
            args = Arguments()
        self.args = args

        logger.info("Worker ID: %s", self.args.WORKER_ID)

        # Worker start time
        self.start_time: float = time.time()

        # Tracking currently executing tasks
        self.active_tasks: dict[str, TaskStatus] = {}
        self.running: bool = False

        # Initialize Redis connection for result caching
        self.cache = redis.Redis(
            host=self.args.CACHE_HOST, port=self.args.CACHE_PORT, decode_responses=True
        )

        self._stack: ExitStack | None = None
        self.session: requests.Session
        self.executor: ThreadPoolExecutor

    def __enter__(self):
        import signal

        # Worker start time
        self.start_time = time.time()
        self.running = True

        self._stack = ExitStack()
        self._stack.enter_context(catch_signal(signal.SIGINT, self.handle_shutdown))
        self._stack.enter_context(catch_signal(signal.SIGTERM, self.handle_shutdown))
        # Initialize Request session
        self.session = self._stack.enter_context(requests.Session())
        self.session.mount(
            "http://",
            requests.adapters.HTTPAdapter(max_retries=self.args.MAX_RETRY),
        )
        # Initialize thread pool executor
        self.executor = self._stack.enter_context(
            ThreadPoolExecutor(max_workers=self.args.POOL_SIZE)
        )

        # Register with coordinator
        if not self.register_worker():
            logger.error("Failed to register worker, exiting")
            sys.exit(1)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        res = None
        if self._stack is not None:
            res = self._stack.__exit__(exc_type, exc_val, exc_tb)
        self.handle_shutdown()
        return res

    @abc.abstractmethod
    def supported_methods(self) -> IndexableContainer[str, Callable[[str], str]]:
        """Return a list of supported computation methods."""
        raise NotImplementedError

    def register_worker(self) -> bool:
        """Register this worker with the coordinator."""
        try:
            response = self.session.post(
                f"{self.args.COORDINATOR_URL}/workers",
                json=WorkerRegistrationRequest(
                    worker_id=self.args.WORKER_ID,
                    capabilities=WorkerCapability(
                        concurrency=self.args.POOL_SIZE,
                        capacity=self.args.CAPACITY,
                        methods=list(self.supported_methods().keys()),
                    ),
                ),
            )
            if response.status_code != 200:
                logger.error("Failed to register worker: %d", response.status_code)
                return False
            logger.info("Worker %s registered successfully", self.args.WORKER_ID)
            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            exc = RuntimeError(f"Error registering worker: {str(e)}")
            exc.__cause__ = e
            logger.exception(exc)
            return False

    def unregister_worker(self) -> bool:
        """Deregister this worker from the coordinator."""
        try:
            response = self.session.delete(
                f"{self.args.COORDINATOR_URL}/workers/{self.args.WORKER_ID}"
            )
            if response.status_code != 200:
                logger.error("Failed to deregister worker: %d", response.status_code)
                return False
            logger.info("Worker %s deregistered successfully", self.args.WORKER_ID)
            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            exc = RuntimeError(f"Error deregistering worker: {str(e)}")
            exc.__cause__ = e
            logger.exception(exc)
            return False

    def send_heartbeat(self) -> bool:
        """Send a heartbeat to the coordinator."""
        try:
            response = self.session.put(
                f"{self.args.COORDINATOR_URL}/workers/{self.args.WORKER_ID}",
                json=WorkerHeartbeat(
                    worker_id=self.args.WORKER_ID,
                    active_tasks=list(self.active_tasks.keys()),
                ),
            )
            if response.status_code != 200:
                logger.warning("Failed to send heartbeat: %d", response.status_code)
                return False
            logger.debug("Heartbeat sent successfully")
            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            exc = RuntimeError(f"Error sending heartbeat: {str(e)}")
            exc.__cause__ = e
            logger.exception(exc)
            return False

    def fetch_task(self) -> Task | None:
        """Fetch a task from the coordinator."""
        try:
            response = self.session.get(
                f"{self.args.COORDINATOR_URL}/workers/{self.args.WORKER_ID}/tasks"
            )
            if response.status_code != 200:
                logger.warning("Failed to fetch task: %d", response.status_code)
                return None
            res_: WorkerTasksGetResponse = response.json()
            if not worker_task_get_response_is_success(res := res_):
                logger.debug("No tasks available: %s", res["reason"])
                return None
            logger.info("Received task %s", res["task_id"])
            return Task(
                task_id=res["task_id"],
                method=res["method"],
                parameters=res["parameters"],
                lease_expires_at=res["lease_expires_at"],
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            exc = RuntimeError(f"Error fetching task: {str(e)}")
            exc.__cause__ = e
            logger.exception(exc)
            return None

    def report_result(self, task_id: str, result: CachedResult, success: bool) -> bool:
        """Report the task result back to the coordinator."""
        try:
            response = self.session.put(
                f"{self.args.COORDINATOR_URL}/workers/{self.args.WORKER_ID}/tasks/{task_id}",
                json=WorkerTaskCompleteTaskRequest(
                    task_id=task_id,
                    success=success,
                    worker_id=result["worker_id"],
                    result=result["result"],
                    execution_time=result["execution_time"],
                    completed_at=result["completed_at"],
                ),
            )
            if response.status_code != 200:
                logger.warning(
                    "Failed to report result for task %s: %d",
                    task_id,
                    response.status_code,
                )
                return False
            res: WorkerTaskCompleteResponse = response.json()
            logger.log(
                logging.INFO if res["status"] == "success" else logging.WARNING,
                "Reported result for task %s: %s",
                task_id,
                res["status"],
            )
            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            exc = RuntimeError(f"Error reporting result for task {task_id}: {str(e)}")
            exc.__cause__ = e
            logger.exception(exc)
            return False

    def check_cache(self, task_id: str) -> CachedResult | None:
        """Check if the task result is already in the cache."""

        def is_map(
            result: CachedResult | dict[str, Never],
        ) -> TypeIs[CachedResult]:
            return bool(result)

        try:
            cached_result: CachedResult | dict[str, Never]
            cached_result = self.cache.hgetall(f"result:{task_id}")  # type: ignore[assignment]
            if is_map(cached_result):
                logger.info("Cache hit for task %s", task_id)
                return cached_result
            return None
        except Exception as e:  # pylint: disable=broad-exception-caught
            exc = RuntimeError(f"Error checking cache for task {task_id}: {str(e)}")
            exc.__cause__ = e
            logger.exception(exc)
            return None

    def store_result(self, task_id: str, result: CachedResult) -> bool:
        """Store the task result in the cache."""
        try:
            self.cache.hset(f"result:{task_id}", mapping=dict(result))

            # Set cache expiration if needed (e.g., 1 day)
            # result_cache.expire(f"result:{task_id}", 86400)

            logger.info("Stored result for task %s in cache", task_id)
            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            exc = RuntimeError(f"Error storing result for task {task_id}: {str(e)}")
            exc.__cause__ = e
            logger.exception(exc)
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
                logger.info("Using cached result for task %s", task_id)
                # Remove from active tasks
                self.active_tasks.pop(task_id, None)
                return self.report_result(task_id, result, True)

            # Process the task
            try:
                handler = self.supported_methods()[task_method]
            except KeyError as e:
                raise ValueError(f"Unsupported method: {task_method}") from e
            result = self.wrap_result(task_id, handler(task_params))

            # Store the result in the cache
            self.store_result(task_id, result)

            # Report the result to the coordinator
            success = self.report_result(task_id, result, True)

            return success
        except Exception as e:  # pylint: disable=broad-exception-caught
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

    def cancel_task(self, task_id: str, pop: bool = True) -> bool:
        """Cancel a task that is currently executing."""
        task_status = self.active_tasks.get(task_id, None)
        if task_status is None:
            logger.warning("Task %s not found in active tasks", task_id)
            return False
        logger.info("Cancelling task %s", task_id)
        task_status.running = False
        if task_status.future is None:
            logger.warning("Future not found for task %s", task_id)
            return False
        if task_status.future.done():
            logger.info("Task %s already completed", task_id)
        elif not task_status.future.cancel():
            logger.warning("Failed to cancel task %s", task_id)
            return False
        # Remove from active tasks
        if pop:
            self.active_tasks.pop(task_id, None)
        return True

    def handle_shutdown(
        self,
        sig: int = 0,
        frame: FrameType | None = None,  # pylint: disable=unused-argument
    ):
        """Handle graceful shutdown."""
        logger.info("Shutdown signal received, stopping worker...")
        if self.running:
            self.running = False
            pop = []
            for task_id in self.active_tasks:
                if self.cancel_task(task_id, pop=False):
                    pop.append(task_id)
            for task_id in pop:
                self.active_tasks.pop(task_id, None)
            del pop
            # Wait for active tasks to complete (with a timeout)
            shutdown_wait = 30  # seconds
            start_wait = time.time()
            while self.active_tasks and time.time() - start_wait < shutdown_wait:
                logger.info(
                    "Waiting for %d active tasks to complete...",
                    len(self.active_tasks),
                )
                time.sleep(1)

            # Deregister from coordinator
            self.unregister_worker()

            # Shutdown executor
            logger.info("Shutting down executor...")
            self.executor.shutdown(wait=False)
            if self._stack:
                self._stack.close()

        if sig != 0:
            sys.exit(0)

    async def _heartbeat(self):
        """Send a heartbeat to the coordinator."""
        last_heartbeat = 0.0

        while self.running:
            # Send heartbeat if needed
            current_time = time.time()
            if current_time - last_heartbeat >= self.args.HEARTBEAT_INTERVAL:
                self.send_heartbeat()
                last_heartbeat = current_time
            await asyncio.sleep(self.args.HEARTBEAT_INTERVAL / 3)

    async def _fetch_task(self):
        """Fetch a task from the coordinator."""
        wait_duration = 1

        while self.running:
            task: Task | None = None
            # Check for new tasks if we have capacity
            if (
                len(self.active_tasks) < self.args.CAPACITY
                and (task := self.fetch_task()) is not None
            ):
                task_status = self.active_tasks.setdefault(
                    task["task_id"], TaskStatus(running=False)
                )
                # Submit task for execution
                task_status.future = self.executor.submit(self.process_task, task)
                wait_duration = 1
            # Sleep to avoid hammering the coordinator
            await asyncio.sleep(0.5 * random.uniform(0, wait_duration - 1))
            wait_duration = max(wait_duration * 2, 64)

    async def loop(self):
        """Main worker loop."""
        await asyncio.gather(
            self._heartbeat(),
            self._fetch_task(),
        )

    def run(self):
        """Run the worker without FastAPI."""

        with self:
            asyncio.run(self.loop())

    @classmethod
    def get_fastapi(cls, *args, **kwargs) -> FastAPI:
        @asynccontextmanager
        async def lifespan(app: FastAPI):  # pylint: disable=unused-argument
            with cls(*args, **kwargs) as worker:
                task = asyncio.create_task(worker.loop())
                yield {"worker": worker}
                task.cancel()

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
