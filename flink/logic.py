"""Window and EMA logic of the Flink job, kept free of Flink imports so it can be unit tested.

The Flink job (flink/job.py) keys events by symbol and keeps one SymbolWindows per
key as Flink state. Flink's event-time timers replace the processor's heap and
watermark: each open window registers a timer that fires when the watermark reaches
the window end, and Flink computes the watermark as the minimum over all Kafka
partitions. That minimum is what the plain Kafka worker lacks (see kafka_worker.py).

Event-time arithmetic is the same as receiver/processor.py, in integer microseconds
here and milliseconds for Flink. Window ends are multiples of 1000 us, so the two
agree exactly.
"""

from receiver.models import SymbolState
from receiver.processor import WINDOW, advance_ema, window_row

NEW, UPDATED, LATE = "new", "updated", "late"
MAX_WATERMARK = 2**63 - 1  # Flink's end-of-input watermark


class SymbolWindows:
    """Keyed state of one symbol: open windows {start: [close, last_timestamp, count]} and EMAs."""
    __slots__ = ("windows", "ema")

    def __init__(self):
        self.windows = {}
        self.ema = SymbolState()


def timer_ms(start):
    """Event-time timer (ms) of the window starting at `start` (us).

    Flink fires a timer when watermark >= timer. With a bounded-out-of-orderness
    watermark of max_ts - lateness - 1, the timer end - 1 fires exactly when
    max_ts - lateness >= end, the processor's rule for closing a window.
    """
    return (start + WINDOW) // 1000 - 1


def start_of_timer(timer):
    return (timer + 1) * 1000 - WINDOW


def add_event(sym, timestamp, price, watermark_ms):
    """Add one price event to its window. Returns (outcome, window_start).

    LATE: the window's timer has already fired, so it is final; nothing changes.
    NEW: the event opened a window; the caller must register timer_ms(start).
    UPDATED: the window was already open. Latest trading time wins, ties go to
    the later arrival, as in the processor.
    """
    start = timestamp // WINDOW * WINDOW
    if watermark_ms >= timer_ms(start):
        return LATE, start
    window = sym.windows.get(start)
    if window is None:
        sym.windows[start] = [price, timestamp, 1]
        return NEW, start
    window[2] += 1
    if timestamp >= window[1]:
        window[0] = price
        window[1] = timestamp
    return UPDATED, start


def finish_window(symbol, sym, start, end_of_input, max_timestamp, include_partial):
    """Finalize the window starting at `start`; returns its output row, or None if withheld.

    Called when the window's timer fires. At end of input Flink fires every remaining
    timer at once, including windows that never completed; as in StreamProcessor.finish,
    those (end > the newest event time) are withheld unless include_partial is set.
    """
    end = start + WINDOW
    partial = end_of_input and end > max_timestamp
    if partial and not include_partial:
        return None
    close, _, count = sym.windows.pop(start)
    previous38, previous100, advisory = advance_ema(sym.ema, close)
    return window_row(symbol, start, end, close, count, sym.ema, previous38, previous100, advisory, partial)


def aggregate_stats(records):
    """Combine the per-subtask stats records of one run into totals.

    A "parse" record describes a source-side subtask (reads and validates Kafka
    records), a "window" record a keyed subtask. Active time runs from the first
    record read to the last window closed.
    """
    parse = [r for r in records if r["stage"] == "parse"]
    window = [r for r in records if r["stage"] == "window"]
    started = [r["started_ns"] for r in parse if r["started_ns"] is not None]
    ended = [r["ended_ns"] for r in window if r["ended_ns"] is not None]
    wall = (max(ended) - min(started)) / 1e9 if started and ended else 0.0
    events = sum(r["events"] for r in parse)
    counters = {}
    for r in records:
        for key, value in r["counters"].items():
            counters[key] = counters.get(key, 0) + value
    # Several operators can share one Python worker process; count each process once.
    processes = {}
    for r in records:
        cpu, rss = processes.get(r["pid"], (0.0, 0.0))
        processes[r["pid"]] = (max(cpu, r["python_cpu_seconds"]), max(rss, r["python_peak_rss_mb"] or 0))
    return {
        "events": events,
        "active_seconds": wall,
        "events_per_second": events / wall if wall > 0 else None,
        "counters": counters,
        "python_processes": len(processes),
        "python_cpu_seconds": sum(cpu for cpu, _ in processes.values()),
        "python_peak_rss_mb_total": sum(rss for _, rss in processes.values()),
        "subtask_events": {f"{r['stage']}{r['subtask']}": r["events"] for r in records},
    }
