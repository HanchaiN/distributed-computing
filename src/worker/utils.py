from __future__ import annotations

import signal
from contextlib import contextmanager
from functools import wraps
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable, TypeVar


@contextmanager
def catch_signal(signalnum: signal._SIGNUM, handler: signal._HANDLER):
    original_handler = signal.getsignal(signalnum)
    signal.signal(signalnum, handler)
    try:
        yield
    finally:
        signal.signal(signalnum, original_handler)


if TYPE_CHECKING:
    RequestType = TypeVar("RequestType")  # bound=dict | TypedDict
    ReturnType = TypeVar("ReturnType")  # bound=dict | TypedDict


def method_wrapper(method: Callable[[RequestType], ReturnType]) -> Callable[[str], str]:
    import json

    @wraps(method)
    def wrapper(json_str: str) -> str:
        return json.dumps(method(json.loads(json_str)), sort_keys=True)

    return wrapper


def format_error(exc: Exception) -> str:
    import traceback

    return "".join(traceback.format_exception(None, value=exc, tb=None))
