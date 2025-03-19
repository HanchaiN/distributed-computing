from __future__ import annotations

import asyncio
import random

import requests

from ..coordinator.types import (
    CreateTaskRequest,
    CreateTaskResponse,
    GetTasksResponseComplete,
    GetTasksResponsePending,
)


def _create_task(
    addr: str,
    method: str,
    param: str,
    session: requests.Session,
) -> str | None:
    response = session.post(
        f"{addr}/tasks",
        json=CreateTaskRequest(
            method=method,
            parameters=param,
        ),
        timeout=60.0,
    )
    if response.status_code != 201:
        response.raise_for_status()
        return None
    data: CreateTaskResponse = response.json()
    return data["task_id"]


def _check_task(
    addr: str,
    task_id: str,
    session: requests.Session,
) -> str | None:
    response = session.get(f"{addr}/tasks/{task_id}", timeout=60.0)
    if response.status_code != 200:
        response.raise_for_status()
        return None
    data: GetTasksResponseComplete | GetTasksResponsePending = response.json()
    if data["status"] == "completed":
        return data["result"]
    return None


async def execute_task_async(
    addr: str,
    method: str,
    param: str,
    session: requests.Session | None = None,
) -> str:
    if session is None:
        session = requests.Session()
    with session:
        task_id = _create_task(addr, method, param, session=session)
        if task_id is None:
            raise RuntimeError("Failed to create task")
        wait_duration = 1
        while True:
            result = _check_task(addr, task_id, session=session)
            if result is not None:
                return result
            await asyncio.sleep(0.5 * random.uniform(0, wait_duration - 1))
            wait_duration = min(wait_duration * 2, 64)


def execute_task(
    addr: str,
    method: str,
    param: str,
    session: requests.Session | None = None,
) -> str:
    return asyncio.run(execute_task_async(addr, method, param, session))
