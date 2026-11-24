import contextlib
import logging
import os
import signal
import threading
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

from opentelemetry.context import attach, detach, get_current
from opentelemetry.trace import SpanKind

from .env import env_thread_count
from .log import flush_telemetry, log_context

logger = logging.getLogger("kart_import")

T = TypeVar("T")
R = TypeVar("R")


def register_signal_handlers() -> None:
    def handle_signal(signum: Any, frame: Any) -> None:
        # Forcefully exit immediately to prevent ThreadPoolExecutor / Dask hangs (on Ctrl+C) or a
        # slow shutdown eating into Kubernetes/Argo's termination grace period (on SIGTERM). This
        # bypasses atexit, so flush whatever telemetry is buffered ourselves before exiting.
        flush_telemetry()
        os._exit(1)

    with contextlib.suppress(ValueError):
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)


# Register automatically when the module is imported
register_signal_handlers()


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
    thread_count: int | None = None,
    trace_items: bool = True,
    span_kind: SpanKind = SpanKind.INTERNAL,
) -> list[R]:
    """Runs a function in parallel over an iterable of items using a ThreadPoolExecutor.

    Bypasses KeyboardInterrupt / SIGINT hangs by polling futures individually and
    forcefully aborting the thread pool upon receiving Ctrl+C.

    Each item gets its own span/log line named after `func`, unless `trace_items` is False - set
    that when `func` already opens its own properly-named `log_context` per item, so the item
    isn't traced twice under a redundant, generically-named span (e.g. the function's name, or
    `<lambda>` for an inline one). A span under `OTEL_MIN_SPAN_DURATION_SECONDS` (default 1s) is
    dropped before export rather than skipped here, so it's still fully visible locally/on error.

    Pass `span_kind=SpanKind.CLIENT` when `func` is itself a call out to another system (e.g.
    downloading a file) rather than local processing -- see `log_context`.

    `thread_count` defaults to `env_thread_count()` (KART_IMPORT_THREAD_COUNT) when not given.
    """

    if thread_count is None:
        thread_count = env_thread_count()

    parent_otel_context = get_current()

    def worker_wrapper(item: T) -> R:
        thread_id = _get_clean_thread_id()
        token = attach(parent_otel_context)

        try:
            if not trace_items:
                return func(item)

            ctx_kwargs: dict[str, Any] = {"action": getattr(func, "__name__", "worker"), "threadId": thread_id}
            commit = getattr(item, "commit", None)
            if isinstance(commit, str):
                ctx_kwargs["commit"] = commit[:8]
            elif isinstance(item, (str, int)):
                ctx_kwargs["item"] = str(item)[:12]

            with log_context(kind=span_kind, **ctx_kwargs):
                return func(item)
        finally:
            detach(token)

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
