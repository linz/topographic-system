import json
import logging
import os
import sys
from collections.abc import Sequence
from contextlib import contextmanager

from opentelemetry._logs import set_logger_provider
from opentelemetry.baggage import get_all, set_baggage
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.context import attach, detach, get_current
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import LogExporter, LogExportResult, SimpleLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import get_tracer_provider, set_tracer_provider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator


@contextmanager
def log_context(**kwargs):
    """Attach key/value pairs to all log records emitted within this context block.

    Uses OTel baggage for propagation and creates a dedicated Span for the script run.
    """
    ctx = get_current()
    for key, value in kwargs.items():
        ctx = set_baggage(key, value, context=ctx)
    token = attach(ctx)

    tracer = get_tracer_provider().get_tracer("kart_import")
    span_name = kwargs.get("action", "script_run")

    try:
        with tracer.start_as_current_span(span_name) as span:
            span.set_attributes(kwargs)
            yield
    finally:
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

    def shutdown(self):
        pass


def setup_logging():
    # Extract trace and baggage contexts passed down by the parent process (e.g. Snakemake)
    ctx = get_current()
    env_headers = {k.lower(): v for k, v in os.environ.items() if k in ("TRACEPARENT", "TRACESTATE", "BAGGAGE")}
    if env_headers:
        ctx = TraceContextTextMapPropagator().extract(env_headers, context=ctx)
        ctx = W3CBaggagePropagator().extract(env_headers, context=ctx)
        attach(ctx)

    resource = Resource.create({"service.name": "kart-import", "service.version": "0.1.0"})
    provider = LoggerProvider(resource=resource)
    set_logger_provider(provider)

    # Configure tracer provider so spans can be generated for each run
    set_tracer_provider(TracerProvider(resource=resource))

    provider.add_log_record_processor(SimpleLogRecordProcessor(_JsonLineExporter()))

    log_level = getattr(logging, os.environ.get("LOG_LEVEL", "DEBUG").upper(), logging.DEBUG)

    handler = LoggingHandler(level=logging.NOTSET, logger_provider=provider)

    app_logger = logging.getLogger("kart_import")
    app_logger.setLevel(log_level)
    app_logger.propagate = False
    if app_logger.hasHandlers():
        app_logger.handlers.clear()
    app_logger.addHandler(handler)


setup_logging()
