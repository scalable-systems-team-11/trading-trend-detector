"""Flink window logic against StreamProcessor, using a small simulation of Flink's
per-partition watermarks and event-time timers. Needs neither Flink nor Kafka."""
import heapq
import unittest

from flink.logic import (LATE, MAX_WATERMARK, NEW, UPDATED, SymbolWindows, add_event,
                         aggregate_stats, finish_window, start_of_timer, timer_ms)
from receiver.processor import WINDOW, StreamProcessor


def tick(symbol, price, time, date="08-11-2021"):
    return {"id": symbol, "sec_type": "E", "last": str(price), "trading_date": date,
            "trading_time": time, "date": date, "time": time}


def simulate(partitions, lateness_s=0.0, include_partial=False):
    """Run the Flink job's logic over `partitions` (lists of raw events).

    Events are consumed one at a time, round-robin over the partitions. Each partition
    has a bounded-out-of-orderness watermark (max_ts - lateness - 1 ms) and the operator's
    watermark is the minimum over the partitions, as in Flink. Watermarks are delivered
    right after each event; the real job delivers them every FLINK_WATERMARK_INTERVAL_MS.
    Returns (rows, counters).
    """
    parser = StreamProcessor()
    lateness_ms = round(lateness_s * 1000)
    keyed, timers, rows = {}, [], []
    counters = {"accepted": 0, "late": 0, "windows": 0, "pending_windows": 0}
    state = {"max_timestamp": 0, "watermark": -2**63}
    partition_max = [None] * len(partitions)
    done = [False] * len(partitions)

    def fire(watermark, end_of_input=False):
        while timers and timers[0][0] <= watermark:
            timer, symbol = heapq.heappop(timers)
            row = finish_window(symbol, keyed[symbol], start_of_timer(timer), end_of_input,
                                state["max_timestamp"], include_partial)
            if row is None:
                counters["pending_windows"] += 1
            else:
                counters["windows"] += 1
                rows.append(row)

    queues = [list(p) for p in partitions]
    while any(queues):
        for index, queue in enumerate(queues):
            if not queue:
                continue
            parsed = parser.parse(queue.pop(0))
            if parsed is None:
                continue
            symbol, price, timestamp, _ = parsed
            sym = keyed.setdefault(symbol, SymbolWindows())
            outcome, start = add_event(sym, timestamp, price, state["watermark"])
            if outcome == LATE:
                counters["late"] += 1
                continue
            counters["accepted"] += 1
            if outcome == NEW:
                heapq.heappush(timers, (timer_ms(start), symbol))
            state["max_timestamp"] = max(state["max_timestamp"], timestamp)
            ms = timestamp // 1000
            partition_max[index] = ms if partition_max[index] is None else max(partition_max[index], ms)
            marks = [m - lateness_ms - 1 if m is not None else -2**63
                     for m, finished in zip(partition_max, done) if not finished]
            state["watermark"] = max(state["watermark"], min(marks))
            fire(state["watermark"])
    fire(MAX_WATERMARK, end_of_input=True)
    return rows, counters


def processor_rows(events, lateness_s=0.0, include_partial=False):
    p = StreamProcessor(lateness_s)
    rows = []
    for e in events:
        rows.extend(p.process(e))
    rows.extend(p.finish(include_partial=include_partial))
    return rows, p.metrics


def by_key(rows):
    return {(r["symbol"], r["window_start"]): r for r in rows}


class TimerTests(unittest.TestCase):
    def test_timer_is_last_millisecond_of_window_and_invertible(self):
        start = 8 * 3600 * 1_000_000
        self.assertEqual(timer_ms(start), (start + WINDOW) // 1000 - 1)
        self.assertEqual(start_of_timer(timer_ms(start)), start)

    def test_add_event_outcomes(self):
        sym = SymbolWindows()
        start = 8 * 3600 * 1_000_000
        self.assertEqual(add_event(sym, start + 10, 5.0, -2**63), (NEW, start))
        self.assertEqual(add_event(sym, start + 20, 6.0, -2**63), (UPDATED, start))
        self.assertEqual(add_event(sym, start + 15, 7.0, -2**63), (UPDATED, start))  # older: close stays
        self.assertEqual(sym.windows[start], [6.0, start + 20, 3])
        # Watermark exactly at the timer: the window has been finalized.
        self.assertEqual(add_event(sym, start + 30, 8.0, timer_ms(start))[0], LATE)
        self.assertEqual(add_event(sym, start + 30, 8.0, timer_ms(start) - 1)[0], UPDATED)

    def test_end_of_input_withholds_incomplete_windows_unless_partial(self):
        for include_partial, expected in ((False, None), (True, True)):
            sym = SymbolWindows()
            start = 8 * 3600 * 1_000_000
            add_event(sym, start + 10, 5.0, -2**63)
            row = finish_window("A", sym, start, True, start + 10, include_partial)
            self.assertEqual(None if row is None else row["partial"], expected)
        sym = SymbolWindows()
        add_event(sym, start + 10, 5.0, -2**63)
        row = finish_window("A", sym, start, True, start + WINDOW, False)  # newer event exists: complete
        self.assertFalse(row["partial"])


class EquivalenceTests(unittest.TestCase):
    def events(self):
        out = []
        for minute in range(0, 130, 1):
            hh, mm = divmod(8 * 60 + minute, 60)
            for i, symbol in enumerate(("A.FR", "B.NL", "C.ETR")):
                out.append(tick(symbol, 100 + (minute * (i + 1)) % 17 - i, f"{hh:02d}:{mm:02d}:{(i * 7) % 60:02d}.000"))
        return out

    def test_single_partition_equals_stream_processor(self):
        events = self.events()
        for lateness in (0.0, 2.0):
            rows, counters = simulate([events], lateness)
            expected, metrics = processor_rows(events, lateness)
            self.assertEqual(rows, expected, f"lateness {lateness}")
            self.assertEqual((counters["accepted"], counters["late"], counters["windows"]),
                             (metrics.accepted, metrics.late, metrics.windows))

    def test_out_of_order_within_lateness_matches_processor(self):
        events = [tick("A.FR", 10, "08:04:59.000"), tick("A.FR", 11, "08:05:01.000"),
                  tick("A.FR", 12, "08:04:59.500"),  # arrives after the next window started
                  tick("A.FR", 13, "08:10:01.000")]
        for lateness in (0.0, 2.0):
            rows, counters = simulate([events], lateness)
            expected, metrics = processor_rows(events, lateness)
            self.assertEqual(rows, expected, f"lateness {lateness}")
            self.assertEqual(counters["late"], metrics.late)
        self.assertEqual(simulate([events], 0.0)[1]["late"], 1)
        self.assertEqual(simulate([events], 2.0)[1]["late"], 0)

    def test_subscribed_partitions_by_symbol_match_independent_symbols(self):
        events = self.events()
        parts = [[e for e in events if e["id"] == s] for s in ("A.FR", "B.NL", "C.ETR")]
        rows, counters = simulate(parts)
        expected, _ = processor_rows(events)
        self.assertEqual(counters["late"], 0)
        self.assertEqual(by_key(rows), by_key(expected))

    def test_uneven_partitions_lose_nothing_unlike_a_single_processor(self):
        # Partition 0 races ahead to 10:00 before partition 1 starts at 08:00. One processor
        # that reads them one after the other sees partition 1 as late; Flink's per-partition
        # watermarks (minimum over partitions) make the order of partitions irrelevant.
        ahead = [tick("A.FR", 100 + i, f"{8 + i // 12:02d}:{i % 12 * 5:02d}:00.000") for i in range(24)]
        behind = [tick("B.NL", 50 + i, f"{8 + i // 12:02d}:{i % 12 * 5:02d}:00.000") for i in range(24)]
        _, single = processor_rows(ahead + behind)
        self.assertGreater(single.late, 0)
        rows, counters = simulate([ahead, behind])
        expected_a, _ = processor_rows(ahead)
        expected_b, _ = processor_rows(behind)
        self.assertEqual(counters["late"], 0)
        self.assertEqual(by_key(rows), {**by_key(expected_a), **by_key(expected_b)})

    def test_partial_windows_flushed_only_on_request(self):
        events = [tick("A.FR", 10, "08:00:10.000"), tick("A.FR", 11, "08:05:10.000")]
        rows, counters = simulate([events])
        self.assertEqual(len(rows), 1)
        self.assertEqual(counters["pending_windows"], 1)
        rows, _ = simulate([events], include_partial=True)
        self.assertEqual([r["partial"] for r in rows], [False, True])
        expected, _ = processor_rows(events, include_partial=True)
        self.assertEqual(rows, expected)

    def test_invalid_events_do_not_advance_the_watermark(self):
        events = [tick("A.FR", 10, "08:00:10.000"), tick("A.FR", "abc", "09:00:00.000"),
                  tick("A.FR", 0, "09:00:00.000"), tick("A.FR", 11, "08:04:00.000"),
                  tick("A.FR", 12, "08:05:10.000")]
        rows, counters = simulate([events])
        expected, metrics = processor_rows(events)
        self.assertEqual(rows, expected)
        self.assertEqual(counters["late"], metrics.late)


class AggregateTests(unittest.TestCase):
    def record(self, stage, subtask, events, started, ended, pid=None, **counters):
        return {"stage": stage, "subtask": subtask, "events": events, "counters": counters,
                "pid": subtask * 10 + (stage == "window") if pid is None else pid,
                "started_ns": started, "ended_ns": ended,
                "python_cpu_seconds": 1.5, "python_peak_rss_mb": 20.0}

    def test_aggregate(self):
        total = aggregate_stats([
            self.record("parse", 0, 100, 10 * 10**9, 12 * 10**9, received=100, invalid=1),
            self.record("parse", 1, 50, 11 * 10**9, 12 * 10**9, received=50, invalid=2),
            self.record("window", 0, 140, 11 * 10**9, 14 * 10**9, accepted=140, windows=7),
            self.record("window", 1, 9, None, None, accepted=9, windows=0),
        ])
        self.assertEqual(total["events"], 150)
        self.assertEqual(total["active_seconds"], 4.0)
        self.assertEqual(total["events_per_second"], 37.5)
        self.assertEqual(total["counters"], {"received": 150, "invalid": 3, "accepted": 149, "windows": 7})
        self.assertEqual(total["python_cpu_seconds"], 6.0)  # four processes, 1.5 s each
        self.assertEqual(total["python_processes"], 4)
        self.assertEqual(total["subtask_events"], {"parse0": 100, "parse1": 50, "window0": 140, "window1": 9})

    def test_aggregate_counts_a_shared_python_process_once(self):
        total = aggregate_stats([
            self.record("parse", 0, 10, 1, 2, pid=7, received=10),
            self.record("window", 0, 10, 1, 3, pid=7, accepted=10),
        ])
        self.assertEqual((total["python_processes"], total["python_cpu_seconds"],
                          total["python_peak_rss_mb_total"]), (1, 1.5, 20.0))

    def test_aggregate_without_events(self):
        total = aggregate_stats([self.record("parse", 0, 0, None, None), self.record("window", 0, 0, None, None)])
        self.assertEqual((total["events"], total["active_seconds"], total["events_per_second"]), (0, 0.0, None))


if __name__ == "__main__":
    unittest.main()
