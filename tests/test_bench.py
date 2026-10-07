"""Benchmark helpers: percentiles, accounting and result files."""
import json
from pathlib import Path
import tempfile
import time
import unittest

from receiver.bench import Bench, percentile, write_result


class BenchTests(unittest.TestCase):
    def test_percentile(self):
        self.assertIsNone(percentile([], 0.5))
        values = list(range(1, 101))
        self.assertEqual(percentile(values, 0.0), 1)
        self.assertEqual(percentile(values, 0.5), 51)
        self.assertEqual(percentile(values, 0.99), 99)
        self.assertEqual(percentile(values, 1.0), 100)

    def test_accounting_ignores_unstamped_events(self):
        bench = Bench()
        for _ in range(3):
            bench.processed(time.perf_counter())
        bench.emitted(None, 5)
        bench.emitted(time.time_ns(), 2)
        summary = bench.summary()
        self.assertEqual(summary["events"], 3)
        self.assertEqual(summary["latency_ms"]["samples"], 2)
        self.assertGreater(summary["processor_events_per_second"], 0)

    def test_empty_run(self):
        summary = Bench().summary()
        self.assertEqual(summary["events"], 0)
        self.assertIsNone(summary["events_per_second"])
        self.assertIsNone(summary["latency_ms"]["p50"])

    def test_write_result_appends(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sub" / "bench.jsonl"
            write_result(path, {"a": 1})
            write_result(path, {"a": 2})
            self.assertEqual([json.loads(l)["a"] for l in path.read_text().splitlines()], [1, 2])


if __name__ == "__main__":
    unittest.main()
