import json
import logging
from contextlib import contextmanager
from contextvars import ContextVar

_log_context = ContextVar("log_context", default=None)


@contextmanager
def log_context(**kwargs):
    old_context = _log_context.get() or {}
    new_context = {**old_context, **kwargs}
    token = _log_context.set(new_context)
    try:
        yield
    finally:
        _log_context.reset(token)


# Capture any custom extra attributes passed to the logger
reserved_keys = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
}


class OTelJsonFormatter(logging.Formatter):
    def format(self, record):
        severity_text = record.levelname
        if severity_text == "WARNING":
            severity_text = "WARN"

        severity_number = 9
        if record.levelname == "DEBUG":
            severity_number = 5
        elif record.levelname in ("INFO", "WARNING", "WARN"):
            severity_number = 9 if record.levelname == "INFO" else 13
        elif record.levelname == "ERROR":
            severity_number = 17
        elif record.levelname == "CRITICAL":
            severity_number = 21

        attributes = {}

        # Merge active logging context attributes from ContextVar
        context = _log_context.get()
        if context:
            attributes.update(context)

        for key, value in record.__dict__.items():
            if key not in reserved_keys and not key.startswith("_"):
                attributes[key] = value

        time_ms = int(record.created * 1_000)

        log_record = {
            "Timestamp": time_ms,
            "SeverityText": severity_text,
            "SeverityNumber": severity_number,
            "level": severity_text.lower(),
            "severity": severity_text,
            "Body": record.getMessage(),
            "Resource": {"service.name": "kart-import", "service.version": "0.1.0"},
            "Attributes": attributes,
        }
        if record.exc_info:
            log_record["Attributes"]["exception.type"] = record.exc_info[0].__name__
            log_record["Attributes"]["exception.message"] = str(record.exc_info[1])
            log_record["Attributes"]["exception.stacktrace"] = self.formatException(record.exc_info)

        def default_serializer(obj):
            if hasattr(obj, "isoformat"):
                return obj.isoformat()
            return str(obj)

        return json.dumps(log_record, default=default_serializer)


def setup_logging():
    import os
    import sys

    root_logger = logging.getLogger()
    log_level = os.environ.get("LOG_LEVEL", "DEBUG").upper()
    root_logger.setLevel(getattr(logging, log_level, logging.DEBUG))

    # Clear existing handlers to prevent duplicate output
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(OTelJsonFormatter())
    root_logger.addHandler(handler)


# Run automatically on import
setup_logging()
