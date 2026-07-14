from __future__ import annotations

import json
import logging
import os
import queue
import socket
import sys
import threading
import time
from contextvars import ContextVar, Token
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
    """Structured JSON log formatter for SLS (阿里云日志服务).

    Emits each log record as a single-line JSON object with trace_id and
    request_id fields, enabling cross-service log correlation across the
    中转站 (relay-stack) → SGLang Router → SGLang Worker chain.

    Used both for stdout output and as the payload builder for the SLS SDK
    direct-push handler, ensuring consistent field names across both paths.
    """

    def __init__(self, service_name: str = "sglang-worker"):
        super().__init__()
        self.service_name = service_name

    def build_dict(self, record: logging.LogRecord) -> dict:
        """Build the structured log dict from a LogRecord.

        Shared between format() (stdout) and the SLS SDK handler so that
        field names stay consistent.
        """
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

        return log_entry

    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(self.build_dict(record), ensure_ascii=False)


class SLSLogContextFilter(logging.Filter):
    """Inject trace_id and request_id into every log record.

    When used as a logging.Filter, this extracts trace_id / request_id
    from the current request context (if available) and attaches them
    to each LogRecord so the formatter can include them in the output.
    """

    def __init__(self):
        super().__init__()
        self._trace_id: ContextVar[Optional[str]] = ContextVar(
            "sls_trace_id", default=None
        )
        self._request_id: ContextVar[Optional[str]] = ContextVar(
            "sls_request_id", default=None
        )

    def set_context(
        self, trace_id: Optional[str] = None, request_id: Optional[str] = None
    ) -> tuple[Token, Token]:
        """Set the current request's trace context.

        Return reset tokens so nested and concurrent contexts can be restored.
        """
        return self._trace_id.set(trace_id), self._request_id.set(request_id)

    def reset_context(self, tokens: tuple[Token, Token]) -> None:
        """Restore the context that preceded the matching set_context call."""
        trace_token, request_token = tokens
        self._trace_id.reset(trace_token)
        self._request_id.reset(request_token)

    def filter(self, record: logging.LogRecord) -> bool:
        trace_id = self._trace_id.get()
        request_id = self._request_id.get()
        if trace_id:
            if not hasattr(record, "trace_id"):
                record.trace_id = trace_id
        if request_id:
            if not hasattr(record, "request_id"):
                record.request_id = request_id
        return True


# Global filter instance — shared between middleware and logging handlers
_global_sls_filter = SLSLogContextFilter()


def get_sls_log_filter() -> SLSLogContextFilter:
    """Return the global SLS log context filter singleton."""
    return _global_sls_filter


def _bool_env(key: str) -> bool:
    return os.environ.get(key, "").lower() in ("true", "1", "yes")


class SLSLogHandler(logging.Handler):
    """Async batching logging handler that pushes logs to阿里云 SLS via SDK.

    Uses a background thread to batch and send logs via PutLogs, so the
    main request path is never blocked. Falls back gracefully: if the SDK
    is not installed or credentials are missing, it silently no-ops and
    logs go only to stdout.
    """

    def __init__(self, service_name: str = "sglang-worker"):
        super().__init__()
        self.service_name = service_name
        self.formatter = SLSJsonFormatter(service_name=service_name)
        self._queue: queue.Queue[Optional[dict]] = queue.Queue(maxsize=10000)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._client = None
        self._project = ""
        self._logstore = ""

        self._init_client()

        if self._client is not None:
            self._thread = threading.Thread(
                target=self._run, daemon=True, name="sls-log-pusher"
            )
            self._thread.start()

    def _init_client(self):
        """Create the SLS LogClient from environment variables.

        Reads SLS_ENDPOINT / SLS_ACCESS_KEY_ID / SLS_ACCESS_KEY_SECRET /
        SLS_PROJECT / SLS_LOGSTORE. If any required value is missing or the
        SDK is not installed, the handler stays inert (logs go to stdout only).
        """
        endpoint = os.environ.get("SLS_ENDPOINT", "").strip()
        ak = os.environ.get("SLS_ACCESS_KEY_ID", "").strip()
        sk = os.environ.get("SLS_ACCESS_KEY_SECRET", "").strip()
        self._project = os.environ.get("SLS_PROJECT", "macaron-log").strip()
        self._logstore = os.environ.get("SLS_LOGSTORE", "sglang-worker").strip()

        if not endpoint or not ak or not sk:
            return

        try:
            from aliyun.log import LogClient
        except ImportError:
            return

        self._client = LogClient(endpoint, ak, sk)

    def emit(self, record: logging.LogRecord):
        """Enqueue a log record dict for async batch push."""
        if self._client is None:
            return
        try:
            log_dict = self.formatter.build_dict(record)
            self._queue.put_nowait(log_dict)
        except queue.Full:
            pass  # drop on overflow to avoid blocking the request

    def _run(self):
        """Background loop: batch up to 200 logs and push every 1s."""
        from aliyun.log import LogItem, PutLogsRequest

        batch: list[dict] = []
        batch_size = 200
        flush_interval = 1.0

        while not self._stop.is_set():
            try:
                item = self._queue.get(timeout=flush_interval)
            except queue.Empty:
                if batch:
                    self._flush(batch, LogItem, PutLogsRequest)
                    batch = []
                continue

            if item is None:
                self._flush(batch, LogItem, PutLogsRequest)
                break

            batch.append(item)
            if len(batch) >= batch_size:
                self._flush(batch, LogItem, PutLogsRequest)
                batch = []

        # Final drain on stop
        self._drain_and_flush(batch, LogItem, PutLogsRequest)

    def _drain_and_flush(self, batch: list[dict], LogItem, PutLogsRequest):
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if item is not None:
                batch.append(item)
        if batch:
            self._flush(batch, LogItem, PutLogsRequest)

    def _flush(self, batch: list[dict], LogItem, PutLogsRequest):
        """Send a batch of logs to SLS via PutLogs."""
        if not batch or self._client is None:
            return
        now = int(time.time())
        items = []
        for log_dict in batch:
            contents = [(k, str(v)) for k, v in log_dict.items()]
            items.append(LogItem(timestamp=now, contents=contents))
        try:
            req = PutLogsRequest(
                self._project, self._logstore, topic="", logitems=items
            )
            self._client.put_logs(req)
        except Exception:
            pass  # swallow SLS errors; logging should never crash the worker

    def close(self):
        self._stop.set()
        self._queue.put(None)  # signal the background thread to drain and exit
        if self._thread is not None:
            self._thread.join(timeout=5)
        super().close()


def configure_sls_logging(service_name: str = "sglang-worker"):
    """Configure root logger for SLS unified log tracing.

    When SGLANG_SLS_LOGGING=true:
    - Always adds a stdout handler with structured JSON (for local debugging)
    - If SLS_ENDPOINT / SLS_ACCESS_KEY_ID / SLS_ACCESS_KEY_SECRET are set,
      also adds an SLSLogHandler that pushes logs directly to阿里云 SLS via
      the Python SDK (async batching, no Logtail agent required).

    This makes the Worker self-contained: it works on Azure / Railway /
    any platform without a Logtail sidecar.
    """
    if not _bool_env("SGLANG_SLS_LOGGING"):
        return

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Remove existing handlers to avoid duplicate output
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    formatter = SLSJsonFormatter(service_name=service_name)

    # stdout handler — always present for local debugging / container logs
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    stdout_handler.addFilter(_global_sls_filter)
    root_logger.addHandler(stdout_handler)

    # SLS SDK direct-push handler — only if credentials are configured
    sls_handler = SLSLogHandler(service_name=service_name)
    sls_handler.addFilter(_global_sls_filter)
    root_logger.addHandler(sls_handler)
