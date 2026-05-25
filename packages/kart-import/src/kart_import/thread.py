import contextlib
import os
import signal
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

from dagster import AssetExecutionContext

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


def run_in_thread_pool(
    context: AssetExecutionContext,
    func: Callable[[T], R],
    items: Iterable[T],
    thread_count: int = 4,
    description: str = "",
) -> list[R]:
    """Runs a function in parallel over an iterable of items using a ThreadPoolExecutor.

    Bypasses KeyboardInterrupt / SIGINT hangs by polling futures individually and
    forcefully aborting the thread pool upon receiving Ctrl+C.
    """
    if description:
        context.log.info(f"{description} using {thread_count} threads.")

    executor = ThreadPoolExecutor(max_workers=thread_count)
    futures = [executor.submit(func, item) for item in items]
    results = []
    try:
        # Resolving futures individually keeps the main thread active to receive KeyboardInterrupt
        for future in futures:
            results.append(future.result())
        return results
    except KeyboardInterrupt:
        context.log.warn("KeyboardInterrupt (Ctrl+C) received. Aborting thread pool...")
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    except Exception:
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    finally:
        executor.shutdown(wait=True)
