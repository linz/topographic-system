import contextlib
import json
import logging
import os
import sys
import time
from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.baggage import get_all, set_baggage
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.context import attach, detach, get_current
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import (
    BatchLogRecordProcessor,
    LogExporter,
    LogExportResult,
    SimpleLogRecordProcessor,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.trace import get_tracer_provider, set_tracer_provider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator


def _default_service_name() -> str:
    """Derive a service name from how this process was invoked, e.g. "clone" for
    `python -m kart_import.assets.extract.clone` or "snakemake" for the `snakemake` CLI."""
    # When invoked via `python -m <module>`, runpy sets __main__'s __spec__ before
    # executing its code, so it's already available while this module is imported.
    main_spec = getattr(sys.modules.get("__main__"), "__spec__", None)
    if main_spec and main_spec.name:
        return main_spec.name.split(".")[-1]

    if sys.argv and sys.argv[0] and not sys.argv[0].startswith("-"):
        return Path(sys.argv[0]).stem

    return "kart-import"


def format_span_name(kwargs: dict[str, Any]) -> str:
    """Build a span name from the action and the primary entity it targets, e.g. "transform: fence [r5]"."""
    action = kwargs.get("action")
    entity = (
        kwargs.get("dataset")
        or kwargs.get("theme")
        or kwargs.get("repo")
        or kwargs.get("lookup")
        or kwargs.get("entry")
        or kwargs.get("commit")
    )
    release = kwargs.get("release")

    name = f"{action}: {entity}" if action and entity else action or entity or "task"
    if release is not None:
        name = f"{name} [r{release}]"
    return name


@contextmanager
def log_context(**kwargs):
    """Attach key/value pairs to all log records emitted within this context block.

    Uses OTel baggage for propagation, creates a dedicated Span for the block, and
    logs the block's duration when it exits - callers don't need their own timing.
    """
    ctx = get_current()
    for key, value in kwargs.items():
        ctx = set_baggage(key, str(value), context=ctx)
    token = attach(ctx)

    tracer = get_tracer_provider().get_tracer("kart_import")
    span_name = format_span_name(kwargs)

    span_attrs = {k: v if isinstance(v, (bool, int, float, str, bytes)) else str(v) for k, v in kwargs.items()}

    start_time = time.perf_counter()
    try:
        with tracer.start_as_current_span(span_name) as span:
            span.set_attributes(span_attrs)
            yield
    finally:
        logging.getLogger("kart_import").info(span_name, extra={"duration": round(time.perf_counter() - start_time, 4)})
        detach(token)


class _DurationFilteringSpanProcessor(SpanProcessor):
    """Wraps another `SpanProcessor` and drops a span before it reaches it, unless the span ran
    at least `min_duration_seconds` or is a top-level job span.

    A "job" span is one whose parent is either absent or a *remote* context: the root span
    Snakemake itself opens for the whole run has no parent at all, and every rule's `python -m
    ...` subprocess inherits that run's trace via `TRACEPARENT` in its environment (see
    `setup_logging`), so the first span *it* opens is parented by that remote context rather than
    a local one. Anything nested under a job span -- a per-release/per-commit/per-phase child
    opened in the same process via `log_context` -- has a local parent instead, and is exactly
    the high-cardinality, usually-fast detail this exists to filter out: a full pipeline run can
    open tens of thousands of these, which is what made the trace too slow to render as a
    waterfall. Set `OTEL_MIN_SPAN_DURATION_SECONDS=0` to disable filtering, e.g. for local
    debugging where you want to see everything.
    """

    def __init__(self, wrapped: SpanProcessor, min_duration_seconds: float) -> None:
        self._wrapped = wrapped
        self._min_duration_seconds = min_duration_seconds

    def on_start(self, span, parent_context=None) -> None:
        self._wrapped.on_start(span, parent_context=parent_context)

    def on_end(self, span) -> None:
        is_job_span = span.parent is None or span.parent.is_remote
        if is_job_span:
            self._wrapped.on_end(span)
            return

        duration_seconds = (span.end_time - span.start_time) / 1_000_000_000
        if duration_seconds >= self._min_duration_seconds:
            self._wrapped.on_end(span)

    def shutdown(self) -> None:
        self._wrapped.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self._wrapped.force_flush(timeout_millis)


class _JsonLineExporter(LogExporter):
    """Writes one compact JSON object per log record to stdout."""

    def __init__(self, out=None):
        self._out = out or sys.stdout

    def export(self, batch: Sequence):
        baggage_attrs = get_all()

        for r in batch:
            log_record = r.log_record if hasattr(r, "log_record") else r
            attrs = dict(log_record.attributes or {})

            if baggage_attrs:
                for k, v in baggage_attrs.items():
                    if k not in attrs:
                        attrs[k] = v

            attrs.pop("code.file.path", None)
            attrs.pop("code.function.name", None)
            attrs.pop("code.line.number", None)

            record = {
                "Timestamp": int(log_record.timestamp / 1_000_000) if log_record.timestamp else None,
                "SeverityText": log_record.severity_text,
                "SeverityNumber": log_record.severity_number.value if log_record.severity_number else None,
                "Body": log_record.body,
                "Attributes": attrs,
                "TraceId": format(log_record.trace_id, "032x") if log_record.trace_id else None,
                "SpanId": format(log_record.span_id, "016x") if log_record.span_id else None,
            }
            self._out.write(json.dumps(record, default=str) + "\n")
        self._out.flush()
        return LogExportResult.SUCCESS

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        self._out.flush()
        return True

    def shutdown(self):
        pass


class _PathSanitizingFilter(logging.Filter):
    """Sanitize Path objects to strings in log record attributes before OTel processes them."""

    def filter(self, record: logging.LogRecord) -> bool:
        for k, v in list(record.__dict__.items()):
            if isinstance(v, Path):
                setattr(record, k, str(v))
        return True


_initialized = False
# Set by setup_logging(): kept as module-level SDK references (not looked up via
# get_tracer_provider()/get_logger_provider(), which are typed to the API's provider classes and
# don't expose force_flush) so flush_telemetry() can call it with a known-good type.
_sdk_tracer_provider: TracerProvider | None = None
_sdk_logger_provider: LoggerProvider | None = None


def setup_logging():
    global _initialized, _sdk_tracer_provider, _sdk_logger_provider
    if _initialized:
        return
    _initialized = True

    # Extract trace and baggage contexts passed down by the parent process (e.g. Snakemake)
    ctx = get_current()
    env_headers = {k.lower(): v for k, v in os.environ.items() if k in ("TRACEPARENT", "TRACESTATE", "BAGGAGE")}
    if env_headers:
        ctx = TraceContextTextMapPropagator().extract(env_headers, context=ctx)
        ctx = W3CBaggagePropagator().extract(env_headers, context=ctx)
        attach(ctx)

    resource_attrs = {
        "service.name": os.environ.get("OTEL_SERVICE_NAME") or _default_service_name(),
        "service.version": "0.1.0",
        "service.namespace": "linz.topography",
    }
    if os.environ.get("GITHUB_RUN_ID"):
        resource_attrs.update(
            {
                "github.run_id": os.environ["GITHUB_RUN_ID"],
                "github.run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
                "github.repository": os.environ.get("GITHUB_REPOSITORY", ""),
                "github.workflow": os.environ.get("GITHUB_WORKFLOW", ""),
            }
        )

    resource = Resource.create(resource_attrs)
    provider = LoggerProvider(resource=resource)
    set_logger_provider(provider)
    _sdk_logger_provider = provider

    # Configure tracer provider so spans can be generated for each run
    tracer_provider = TracerProvider(resource=resource)
    set_tracer_provider(tracer_provider)
    _sdk_tracer_provider = tracer_provider

    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    traces_endpoint = otlp_endpoint or os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "").strip()
    logs_endpoint = otlp_endpoint or os.environ.get("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "").strip()
    is_disabled = os.environ.get("OTEL_SDK_DISABLED", "").lower() in ("true", "1")
    console_export = os.environ.get("OTEL_TRACES_CONSOLE", "").lower() in ("true", "1") or (
        os.environ.get("OTEL_TRACES_EXPORTER", "").lower() == "console"
    )

    min_span_duration_seconds = float(os.environ.get("OTEL_MIN_SPAN_DURATION_SECONDS", "1"))

    def filtered(processor: SpanProcessor) -> SpanProcessor:
        if min_span_duration_seconds <= 0:
            return processor
        return _DurationFilteringSpanProcessor(processor, min_span_duration_seconds)

    if not is_disabled:
        if traces_endpoint or os.environ.get("OTEL_TRACES_EXPORTER") == "otlp":
            tracer_provider.add_span_processor(filtered(BatchSpanProcessor(OTLPSpanExporter())))
        elif console_export:
            tracer_provider.add_span_processor(filtered(SimpleSpanProcessor(ConsoleSpanExporter())))

        if logs_endpoint or os.environ.get("OTEL_LOGS_EXPORTER") == "otlp":
            provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))

    # Always written locally too: a Snakemake rule can redirect this to a per-rule log file
    # via its `log:` directive, for tools that need the raw JSON lines on disk.
    provider.add_log_record_processor(SimpleLogRecordProcessor(_JsonLineExporter()))

    log_level = getattr(logging, os.environ.get("LOG_LEVEL", "DEBUG").upper(), logging.DEBUG)

    handler = LoggingHandler(level=logging.NOTSET, logger_provider=provider)

    app_logger = logging.getLogger("kart_import")
    app_logger.setLevel(log_level)
    app_logger.propagate = False
    if app_logger.hasHandlers():
        app_logger.handlers.clear()
    app_logger.addFilter(_PathSanitizingFilter())
    app_logger.addHandler(handler)


def get_tracer(name: str = "kart_import"):
    setup_logging()
    return trace.get_tracer(name)


def flush_telemetry(timeout_millis: int = 2000) -> None:
    """Force-export any buffered spans/logs immediately.

    `BatchSpanProcessor`/`BatchLogRecordProcessor` normally export on their own schedule, and
    whatever's still buffered is flushed by the SDK's own `atexit` shutdown hook on a clean exit.
    But a signal handler that exits via `os._exit` (see `thread.register_signal_handlers`) skips
    `atexit` entirely, and the default handling of SIGTERM (sent by Kubernetes/Argo on pod
    termination) doesn't run it either -- both would otherwise silently drop everything buffered.
    Call this from a signal handler before exiting so the run's telemetry still makes it out.
    """
    if _sdk_tracer_provider is not None:
        with contextlib.suppress(Exception):
            _sdk_tracer_provider.force_flush(timeout_millis)
    if _sdk_logger_provider is not None:
        with contextlib.suppress(Exception):
            _sdk_logger_provider.force_flush(timeout_millis)


setup_logging()
