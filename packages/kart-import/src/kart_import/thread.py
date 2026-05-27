import contextlib
import logging
import os
import signal
import threading
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

from .log import _log_context, log_context

logger = logging.getLogger("kart_import")

T = TypeVar("T")
R = TypeVar("R")


def register_sigint_handler() -> None:
    def handle_sigint(signum: Any, frame: Any) -> None:
        # Forcefully exit immediately on Ctrl+C to prevent ThreadPoolExecutor / Dask hangs
        os._exit(1)

    with contextlib.suppress(ValueError):
        signal.signal(signal.SIGINT, handle_sigint)


# Register automatically when the module is imported
register_sigint_handler()


_thread_indices: dict[int, int] = {}
_thread_indices_lock = threading.Lock()


def _get_clean_thread_id() -> int:
    ident = threading.get_ident()
    with _thread_indices_lock:
        if ident not in _thread_indices:
            _thread_indices[ident] = len(_thread_indices)
        return _thread_indices[ident]


def run_in_thread_pool(
    func: Callable[[T], R],
    items: Iterable[T],
    thread_count: int = 4,
) -> list[R]:
    """Runs a function in parallel over an iterable of items using a ThreadPoolExecutor.

    Bypasses KeyboardInterrupt / SIGINT hangs by polling futures individually and
    forcefully aborting the thread pool upon receiving Ctrl+C.
    """

    parent_context: dict[Any, Any] = _log_context.get() or {}

    def worker_wrapper(item: T) -> R:
        thread_id = _get_clean_thread_id()
        with log_context(**parent_context, threadId=thread_id):
            return func(item)

    executor = ThreadPoolExecutor(max_workers=thread_count)
    futures = [executor.submit(worker_wrapper, item) for item in items]
    results = []
    try:
        # Resolving futures individually keeps the main thread active to receive KeyboardInterrupt
        for future in futures:
            results.append(future.result())
        return results
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt (Ctrl+C) received. Aborting thread pool...")
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    except Exception:
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    finally:
        executor.shutdown(wait=True)
