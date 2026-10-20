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
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.trace import get_tracer_provider, set_tracer_provider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator


def _default_service_name() -> str:
    """Derive a service name from how this process was invoked, e.g. "clone" for
    `python -m kart_import.assets.clone` or "snakemake" for the `snakemake` CLI."""
    # When invoked via `python -m <module>`, runpy sets __main__'s __spec__ before
    # executing its code, so it's already available while this module is imported.
    main_spec = getattr(sys.modules.get("__main__"), "__spec__", None)
    if main_spec and main_spec.name:
        return main_spec.name.split(".")[-1]

    if sys.argv and sys.argv[0] and not sys.argv[0].startswith("-"):
        return Path(sys.argv[0]).stem

    return "kart-import"


def _format_span_name(kwargs: dict[str, Any]) -> str:
    """Build a span name from the action and the primary entity it targets, e.g. "transform: fence [r5]"."""
    action = kwargs.get("action")
    entity = (
        kwargs.get("dataset")
        or kwargs.get("theme")
        or kwargs.get("repo")
        or kwargs.get("lookup")
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
    span_name = _format_span_name(kwargs)

    span_attrs = {k: v if isinstance(v, (bool, int, float, str, bytes)) else str(v) for k, v in kwargs.items()}

    start_time = time.perf_counter()
    try:
        with tracer.start_as_current_span(span_name) as span:
            span.set_attributes(span_attrs)
            yield
    finally:
        logging.getLogger("kart_import").info(span_name, extra={"duration": round(time.perf_counter() - start_time, 4)})
        detach(token)


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


def setup_logging():
    global _initialized
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

    # Configure tracer provider so spans can be generated for each run
    tracer_provider = TracerProvider(resource=resource)
    set_tracer_provider(tracer_provider)

    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    traces_endpoint = otlp_endpoint or os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "").strip()
    logs_endpoint = otlp_endpoint or os.environ.get("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "").strip()
    is_disabled = os.environ.get("OTEL_SDK_DISABLED", "").lower() in ("true", "1")
    console_export = os.environ.get("OTEL_TRACES_CONSOLE", "").lower() in ("true", "1") or (
        os.environ.get("OTEL_TRACES_EXPORTER", "").lower() == "console"
    )

    if not is_disabled:
        if traces_endpoint or os.environ.get("OTEL_TRACES_EXPORTER") == "otlp":
            tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        elif console_export:
            tracer_provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))

        if logs_endpoint or os.environ.get("OTEL_LOGS_EXPORTER") == "otlp":
            provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))

    # Always written locally too: Snakemake redirects this to a per-dataset log file
    # (see the `bundle` rule), which `grep`/`ls -t logs/bundle/$RUN_ID/` rely on.
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


setup_logging()
