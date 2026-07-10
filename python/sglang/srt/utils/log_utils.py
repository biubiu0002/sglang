from __future__ import annotations

import json
import logging
import os
import socket
import sys
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from typing import List, Optional, Union

import torch.distributed as dist


def create_log_targets(
    *, targets: Optional[List[str]], name_prefix: str
) -> List[logging.Logger]:
    if not targets:
        return [_create_log_target_stdout(name_prefix)]
    return [_create_log_target(t, name_prefix) for t in targets]


def _create_log_target(target: str, name_prefix: str) -> logging.Logger:
    if target.lower() == "stdout":
        return _create_log_target_stdout(name_prefix)
    return _create_log_target_file(target, name_prefix)


def _create_log_target_stdout(name_prefix: str) -> logging.Logger:
    return _create_logger_with_handler(
        f"{name_prefix}.stdout", logging.StreamHandler(sys.stdout)
    )


def _create_log_target_file(directory: str, name_prefix: str) -> logging.Logger:
    os.makedirs(directory, exist_ok=True)
    hostname = socket.gethostname()
    rank = dist.get_rank() if dist.is_initialized() else 0
    filename = os.path.join(directory, f"{hostname}_{rank}.log")
    handler = TimedRotatingFileHandler(
        filename, when="H", backupCount=0, encoding="utf-8"
    )
    return _create_logger_with_handler(
        f"{name_prefix}.file.{directory}.{hostname}_{rank}", handler
    )


def _create_logger_with_handler(name: str, handler: logging.Handler) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        handler.setFormatter(
            logging.Formatter("[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        )
        logger.addHandler(handler)
    return logger


def log_json(
    loggers: Union[logging.Logger, List[logging.Logger]], event: str, data: dict
) -> None:
    log_data = {
        "timestamp": datetime.now().isoformat(),
        "event": event,
        **data,
    }
    msg = json.dumps(log_data, ensure_ascii=False)

    if not isinstance(loggers, list):
        loggers = [loggers]

    for logger in loggers:
        logger.info(msg)


class SLSJsonFormatter(logging.Formatter):
    """Structured JSON log formatter for SLS (阿里云日志服务) Logtail pickup.

    Emits each log record as a single-line JSON object with trace_id and
    request_id fields, enabling cross-service log correlation across the
    中转站 (relay-stack) → SGLang Router → SGLang Worker chain.

    Logtail collects stdout; this formatter ensures every log line is
    parseable JSON with consistent field names.
    """

    def __init__(self, service_name: str = "sglang-worker"):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname.lower(),
            "service_name": self.service_name,
            "message": record.getMessage(),
        }

        if record.exc_info and record.exc_info[1] is not None:
            log_entry["error"] = str(record.exc_info[1])

        # Extract trace_id / request_id from LogRecord extra fields
        # (set by SLSLogContextFilter or directly via logger.info(..., extra={...}))
        for field in ("trace_id", "request_id", "rid", "model", "component"):
            value = getattr(record, field, None)
            if value:
                log_entry[field] = str(value)

        return json.dumps(log_entry, ensure_ascii=False)


class SLSLogContextFilter(logging.Filter):
    """Inject trace_id and request_id into every log record.

    When used as a logging.Filter, this extracts trace_id / request_id
    from the current request context (if available) and attaches them
    to each LogRecord so the formatter can include them in the output.
    """

    def __init__(self):
        super().__init__()
        self._trace_id = None
        self._request_id = None

    def set_context(self, trace_id: Optional[str] = None, request_id: Optional[str] = None):
        """Set the current request's trace context.

        Called by the FastAPI middleware at the start of each request.
        """
        self._trace_id = trace_id
        self._request_id = request_id

    def clear(self):
        """Clear the trace context after the request completes."""
        self._trace_id = None
        self._request_id = None

    def filter(self, record: logging.LogRecord) -> bool:
        if self._trace_id:
            if not hasattr(record, "trace_id"):
                record.trace_id = self._trace_id
        if self._request_id:
            if not hasattr(record, "request_id"):
                record.request_id = self._request_id
        return True


# Global filter instance — shared between middleware and logging handlers
_global_sls_filter = SLSLogContextFilter()


def get_sls_log_filter() -> SLSLogContextFilter:
    """Return the global SLS log context filter singleton."""
    return _global_sls_filter


def configure_sls_logging(service_name: str = "sglang-worker"):
    """Configure root logger with SLS-compatible JSON formatting.

    This should be called at worker startup to ensure all log output
    (including uvicorn and framework logs) is structured JSON suitable
    for SLS Logtail collection.

    Set SGLANG_SLS_LOGGING=true in the environment to enable.
    """
    if not os.environ.get("SGLANG_SLS_LOGGING", "").lower() in ("true", "1", "yes"):
        return

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Remove existing handlers to avoid duplicate output
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(SLSJsonFormatter(service_name=service_name))
    handler.addFilter(_global_sls_filter)
    root_logger.addHandler(handler)
