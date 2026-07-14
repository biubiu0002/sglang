import asyncio
import io
import json
import logging
import re
import tempfile
import unittest
import uuid
from contextlib import redirect_stdout
from pathlib import Path

from sglang.srt.utils.log_utils import (
    SLSLogContextFilter,
    create_log_targets,
    log_json,
)
from sglang.test.ci.ci_register import register_cpu_ci

register_cpu_ci(est_time=6, suite="base-a-test-cpu")
register_cpu_ci(est_time=7, suite="base-c-test-cpu")

_LOG_PREFIX_RE = re.compile(r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] ")


class TestLogUtils(unittest.TestCase):
    def test_stdout(self):
        for targets in [["stdout"], None]:
            with self.subTest(targets=targets):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    loggers = create_log_targets(
                        targets=targets, name_prefix=f"test_stdout_{uuid.uuid4()}"
                    )
                    self.assertEqual(len(loggers), 1)
                    log_json(loggers[0], "test.event", {"key": "value"})
                data = _parse_log_json(buf.getvalue().strip())
                self.assertIn("timestamp", data)
                self.assertEqual(data["event"], "test.event")
                self.assertEqual(data["key"], "value")

    def test_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            loggers = create_log_targets(
                targets=[temp_dir], name_prefix=f"test_file_{uuid.uuid4()}"
            )
            self.assertEqual(len(loggers), 1)
            log_json(loggers, "file.event", {"data": 123})
            _flush_all(loggers)
            data = _read_log_file(temp_dir)
            self.assertIn("timestamp", data)
            self.assertEqual(data["event"], "file.event")
            self.assertEqual(data["data"], 123)

    def test_multiple_targets(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            buf = io.StringIO()
            with redirect_stdout(buf):
                loggers = create_log_targets(
                    targets=["stdout", temp_dir],
                    name_prefix=f"test_multi_{uuid.uuid4()}",
                )
                self.assertEqual(len(loggers), 2)
                log_json(loggers, "multi.event", {"x": 1})
            _flush_all(loggers)
            stdout_data = _parse_log_json(buf.getvalue().strip())
            file_data = _read_log_file(temp_dir)
            self.assertEqual(stdout_data["event"], "multi.event")
            self.assertEqual(file_data["event"], "multi.event")
            self.assertEqual(stdout_data["x"], file_data["x"])


class TestSLSLogContextFilter(unittest.TestCase):
    @staticmethod
    def _record() -> logging.LogRecord:
        return logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="test",
            args=(),
            exc_info=None,
        )

    def test_nested_context_is_restored(self):
        context_filter = SLSLogContextFilter()
        outer_tokens = context_filter.set_context("outer-trace", "outer-request")
        inner_tokens = context_filter.set_context("inner-trace", "inner-request")

        inner_record = self._record()
        context_filter.filter(inner_record)
        self.assertEqual(inner_record.trace_id, "inner-trace")

        context_filter.reset_context(inner_tokens)
        outer_record = self._record()
        context_filter.filter(outer_record)
        self.assertEqual(outer_record.trace_id, "outer-trace")
        self.assertEqual(outer_record.request_id, "outer-request")
        context_filter.reset_context(outer_tokens)

    def test_concurrent_contexts_are_isolated(self):
        context_filter = SLSLogContextFilter()

        async def capture(trace_id: str, request_id: str) -> tuple[str, str]:
            tokens = context_filter.set_context(trace_id, request_id)
            try:
                await asyncio.sleep(0)
                record = self._record()
                context_filter.filter(record)
                await asyncio.sleep(0)
                return record.trace_id, record.request_id
            finally:
                context_filter.reset_context(tokens)

        async def run_concurrently():
            return await asyncio.gather(
                capture("trace-a", "request-a"),
                capture("trace-b", "request-b"),
            )

        self.assertEqual(
            asyncio.run(run_concurrently()),
            [("trace-a", "request-a"), ("trace-b", "request-b")],
        )


def _parse_log_json(line: str) -> dict:
    """Strip the ``[YYYY-MM-DD HH:MM:SS] `` prefix added by the formatter."""
    return json.loads(_LOG_PREFIX_RE.sub("", line))


def _flush_all(loggers: list) -> None:
    for logger in loggers:
        for handler in logger.handlers:
            handler.flush()


def _read_log_file(temp_dir: str) -> dict:
    log_files = list(Path(temp_dir).glob("*.log"))
    assert len(log_files) == 1
    return _parse_log_json(log_files[0].read_text().strip())


if __name__ == "__main__":
    unittest.main()
