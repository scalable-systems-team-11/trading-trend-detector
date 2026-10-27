"""Lightweight run benchmark: throughput, CPU, peak memory and emit latency.

Throughput is measured from the first to the last received event, so idle
time while the replayer scans filtered-out CSV rows before the first or after
the last event is excluded. Pacing pauses in between are included.

Latency uses the replayer's `sent_ns` wall-clock stamp on the event that
finalized a window and ends when the row has been written and flushed.
Producer and receiver must share a clock (same host, or Docker on one machine).
"""

import json
import os
from pathlib import Path
import platform
import sys
import time

try:
    import resource
except ImportError:  # Windows: peak memory is not reported.
    resource = None


def peak_rss_mb():
    if resource is None:
        return None
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux KiB.
    return rss / (1024 * 1024 if sys.platform == "darwin" else 1024)


def percentile(sorted_values, q):
    """Nearest-rank percentile of an already sorted list, None if empty."""
    if not sorted_values:
        return None
    return sorted_values[min(len(sorted_values) - 1, round(q * (len(sorted_values) - 1)))]


def host_info():
    return {"platform": platform.platform(), "python": platform.python_version(),
            "cpu_count": os.cpu_count()}


class Bench:
    def __init__(self):
        self.cpu_begin = None
        self.first = None
        self.started_at_ns = None
        self.last = None
        self.events = 0
        self.process_seconds = 0.0
        self.latencies_ms = []

    def processed(self, began):
        """Account one processor call that started at perf_counter() `began`."""
        now = time.perf_counter()
        if self.first is None:
            # CPU and wall time both start at the first event.
            self.first = began
            self.started_at_ns = time.time_ns()
            self.cpu_begin = time.process_time()
        self.last = now
        self.events += 1
        self.process_seconds += now - began

    def emitted(self, sent_ns, rows):
        """Record latency for `rows` window rows finalized by an event sent at `sent_ns`."""
        if rows and isinstance(sent_ns, int):
            self.latencies_ms.extend([(time.time_ns() - sent_ns) / 1e6] * rows)

    def summary(self):
        wall = (self.last - self.first) if self.first is not None else 0.0
        cpu = time.process_time() - self.cpu_begin if self.cpu_begin is not None else 0.0
        latencies = sorted(self.latencies_ms)
        return {
            "events": self.events,
            "started_at_ns": self.started_at_ns,
            "active_seconds": wall,
            "events_per_second": self.events / wall if wall > 0 else None,
            "processor_seconds": self.process_seconds,
            "processor_events_per_second": self.events / self.process_seconds if self.process_seconds > 0 else None,
            "cpu_seconds": cpu,
            "cpu_utilization": cpu / wall if wall > 0 else None,
            "peak_rss_mb": peak_rss_mb(),
            "latency_ms": {
                "samples": len(latencies),
                "p50": percentile(latencies, 0.50),
                "p95": percentile(latencies, 0.95),
                "p99": percentile(latencies, 0.99),
                "max": latencies[-1] if latencies else None,
            },
        }


def write_result(path, record):
    """Append one benchmark record as a JSON line."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
