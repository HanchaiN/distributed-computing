from __future__ import annotations

import time
from typing import TypedDict

import uvicorn

from .base import BaseWorker
from .utils import method_wrapper


class _TestWorker(BaseWorker):
    class TaskParams(TypedDict):
        duration: int
        value: str

    class ResultType(TypedDict):
        status: str
        message: str

    def supported_methods(self):
        return {
            "ping": self._ping,
        }

    @staticmethod
    @method_wrapper
    def _ping(params: _TestWorker.TaskParams) -> _TestWorker.ResultType:
        """Example function for performing a computation task.

        This is a placeholder - replace with actual computation logic.
        """

        time.sleep(params.get("duration", 1))  # Simulate work
        return _TestWorker.ResultType(
            status="completed",
            message=params.get("value", "Task executed successfully"),
        )


app = _TestWorker.get_fastapi()

if __name__ == "__main__":
    uvicorn.run("app.worker.example:app", host="0.0.0.0", port=8000, reload=True)
