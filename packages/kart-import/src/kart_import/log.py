import json
import logging
import os
import sys
from collections.abc import Sequence
from contextlib import contextmanager

from opentelemetry._logs import set_logger_provider
from opentelemetry.baggage import set_baggage
from opentelemetry.context import attach, detach, get_current
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import LogExporter, LogExportResult, SimpleLogRecordProcessor
from opentelemetry.sdk.resources import Resource


@contextmanager
def log_context(**kwargs):
    """Attach key/value pairs to all log records emitted within this context block.

    Uses OTel baggage for propagation — compatible with threads when the context
    token is passed via :func:`opentelemetry.context.attach`.
    """
    ctx = get_current()
    for key, value in kwargs.items():
        ctx = set_baggage(key, value, context=ctx)
    token = attach(ctx)
    try:
        yield
    finally:
        detach(token)


class _JsonLineExporter(LogExporter):
    """Writes one compact JSON object per log record to stdout."""

    def __init__(self, out=None):
        self._out = out or sys.stdout

    def export(self, batch: Sequence):
        for r in batch:
            log_record = r.log_record if hasattr(r, "log_record") else r
            attrs = dict(log_record.attributes or {})
            attrs.pop("code.file.path", None)
            attrs.pop("code.function.name", None)
            attrs.pop("code.line.number", None)

            record = {
                "Timestamp": int(log_record.timestamp / 1_000_000) if log_record.timestamp else None,
                "SeverityText": log_record.severity_text,
                "SeverityNumber": log_record.severity_number.value if log_record.severity_number else None,
                "Body": log_record.body,
                "Attributes": attrs,
            }
            self._out.write(json.dumps(record, default=str) + "\n")
        self._out.flush()
        return LogExportResult.SUCCESS

    def shutdown(self):
        pass


def setup_logging():
    resource = Resource.create({"service.name": "kart-import", "service.version": "0.1.0"})
    provider = LoggerProvider(resource=resource)
    set_logger_provider(provider)

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
