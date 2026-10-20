"""Kafka worker and sink logic against fakes; no broker or kafka-python needed."""
import csv
import json
from collections import namedtuple
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from receiver import kafka_worker
from receiver.bench import Bench
from receiver.processor import StreamProcessor
from replayer.replay import FIELDS, KafkaSink, parse_args, replay

TP = namedtuple("TP", "topic partition")
Record = namedtuple("Record", "value")
EOS = json.dumps({"type": "eos"}).encode()


def record(**event):
    return Record(json.dumps(event).encode())


def tick(symbol, price, time, **extra):
    return dict(id=symbol, sec_type="E", date="08-11-2021", time=time, last=str(price),
                trading_time=time, trading_date="08-11-2021", **extra)


class FakeConsumer:
    """poll() returns one prepared {partition: [records]} batch per call."""
    def __init__(self, batches):
        self.batches = list(batches)

    def poll(self, timeout_ms=0):
        if not self.batches:
            return {}
        return {TP("ticks", p): recs for p, recs in self.batches.pop(0).items()}


def run_consume(batches, partitions):
    rows = []
    processor = StreamProcessor()
    lines, malformed = kafka_worker.consume(FakeConsumer(batches), partitions, processor, rows.extend, Bench())
    return rows, processor, lines, malformed


class WorkerTests(unittest.TestCase):
    def test_partition_assignment_is_disjoint_and_complete(self):
        parts = list(range(8))
        owned = [kafka_worker.partitions_for_worker(parts, w, 3) for w in range(3)]
        self.assertEqual(owned, [[0, 3, 6], [1, 4, 7], [2, 5]])
        self.assertEqual(sorted(p for o in owned for p in o), parts)
        self.assertEqual(kafka_worker.partitions_for_worker(parts, 5, 6), [5])
        self.assertEqual(kafka_worker.partitions_for_worker([0, 1], 3, 4), [])

    def test_partitions_must_match_workers(self):
        kafka_worker.check_partitions({0, 1, 2, 3}, 4)
        for found, workers in [({0, 1, 2, 3}, 1), ({0, 1}, 4)]:
            with self.assertRaises(RuntimeError):
                kafka_worker.check_partitions(found, workers)
        kafka_worker.check_partitions({0, 1, 2, 3}, 1, allow_mismatch=True)

    def test_one_worker_over_uneven_partitions_drops_events_as_late(self):
        # Why one partition per worker is required: partition 0 races ahead in
        # event time, so partition 1's earlier events arrive behind the watermark.
        batches = [
            {0: [record(**tick("A.FR", 100, "09:00:00"))]},
            {1: [record(**tick("B.NL", 50, "08:01:00"))]},
            {0: [Record(EOS)], 1: [Record(EOS)]},
        ]
        rows, processor, *_ = run_consume(batches, [0, 1])
        self.assertEqual(processor.metrics.late, 1)

    def test_output_path_per_worker(self):
        self.assertEqual(kafka_worker.output_path("/output/windows.jsonl", 2), "/output/windows.w2.jsonl")

    def test_consume_waits_for_eos_on_every_partition(self):
        batches = [
            {0: [record(**tick("A.FR", 100, "08:00:00")), Record(EOS)]},
            {1: [record(**tick("B.NL", 50, "08:01:00"))]},        # partition 0 is done, 1 is not
            {1: [record(**tick("B.NL", 60, "08:06:00")), Record(EOS)]},
            {0: [record(**tick("A.FR", 1, "09:00:00"))]},         # must never be read
        ]
        rows, processor, lines, malformed = run_consume(batches, [0, 1])
        self.assertEqual((lines, malformed), (5, 0))
        self.assertEqual(processor.metrics.accepted, 3)
        # B's 08:06 event moves the watermark past both symbols' 08:00 windows.
        self.assertEqual([(r["symbol"], r["close"]) for r in rows], [("A.FR", 100.0), ("B.NL", 50.0)])

    def test_consume_counts_malformed_and_ignores_unowned_data(self):
        rows, processor, lines, malformed = run_consume(
            [{0: [Record(b"not json"), record(**tick("A.FR", 100, "08:00:00")), Record(EOS)]}], [0])
        self.assertEqual((lines, malformed, processor.metrics.accepted), (3, 1, 1))

    def test_no_partitions_finishes_immediately(self):
        rows, processor, lines, malformed = run_consume([], [])
        self.assertEqual((rows, lines), ([], 0))

    def test_partitioned_workers_match_single_processor(self):
        symbols = [f"S{i}.FR" for i in range(10)]
        events = [tick(s, 100 + i + 5 * m, f"08:{m * 5 + 1:02d}:00")
                  for m in range(12) for i, s in enumerate(symbols)]
        single = StreamProcessor()
        expected = [r for e in events for r in single.process(e)]

        owner = {s: i % 3 for i, s in enumerate(symbols)}           # stand-in for the key hash
        combined = []
        for w in range(3):
            batch = [record(**e) for e in events if owner[e["id"]] == w] + [Record(EOS)]
            rows, *_ = run_consume([{w: batch}], [w])
            combined.extend(rows)
        key = lambda r: (r["symbol"], r["window_start"])
        got = {key(r): r for r in combined}
        self.assertEqual(len(got), len(combined))
        self.assertTrue(expected)
        for row in expected:
            self.assertEqual(got[key(row)], row)

    def test_aggregate(self):
        def result(w, events, started, seconds, latencies):
            return {"worker": w, "events": events, "started_at_ns": started, "active_seconds": seconds,
                    "cpu_seconds": 1.0, "peak_rss_mb": 10, "latencies_ms": latencies,
                    "counters": {"windows": events // 10, "late": 0}}
        total = kafka_worker.aggregate(2, [result(0, 1000, 0, 2.0, [1, 2]), result(1, 3000, 1_000_000_000, 2.0, [3, 4])])
        self.assertEqual(total["events"], 4000)
        self.assertAlmostEqual(total["active_seconds"], 3.0)
        self.assertAlmostEqual(total["events_per_second"], 4000 / 3.0)
        self.assertEqual(total["counters"], {"windows": 400, "late": 0})
        self.assertEqual(total["latency_ms"]["samples"], 4)
        self.assertEqual(total["worker_events"], {"w0": 1000, "w1": 3000})
        self.assertIsNone(kafka_worker.aggregate(1, [])["events_per_second"])


class FakeProducer:
    def __init__(self, **kwargs):
        self.sent = []
        self.flushed = self.closed = False

    def partitions_for(self, topic):
        return {0, 1, 2}

    def send(self, topic, value=None, key=None, partition=None):
        self.sent.append({"topic": topic, "key": key, "value": value, "partition": partition})
        return types.SimpleNamespace(add_errback=lambda f: None)

    def flush(self):
        self.flushed = True

    def close(self):
        self.closed = True


class SinkTests(unittest.TestCase):
    def fake_kafka(self):
        kafka = types.ModuleType("kafka")
        kafka.KafkaProducer = FakeProducer
        return patch.dict(sys.modules, {"kafka": kafka})

    def test_replay_keys_events_by_symbol_and_close_sends_eos_to_every_partition(self):
        with tempfile.TemporaryDirectory() as tmp, self.fake_kafka():
            path = Path(tmp) / "fixture.csv"
            with path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(FIELDS)
                for symbol, price in [("A.FR", 10), ("B.NL", 20), ("A.FR", 11)]:
                    writer.writerow([symbol, "E", "08-11-2021", "08:00:00", price, "08:00:00", "08-11-2021"])
            sink = KafkaSink("kafka:9092", "ticks")
            args = parse_args([str(path), "--speed", "0", "--progress", "0"])
            self.assertEqual(replay(args, sink), (3, 0))
            sink.close()

            sent = sink.producer.sent
            events = [s for s in sent if s["partition"] is None]
            self.assertEqual([s["key"] for s in events], [b"A.FR", b"B.NL", b"A.FR"])
            self.assertTrue(all("sent_ns" in json.loads(s["value"]) for s in events))
            markers = [s for s in sent if s["partition"] is not None]
            self.assertEqual(sorted(s["partition"] for s in markers), [0, 1, 2])
            self.assertTrue(all(s["value"] == EOS for s in markers))
            self.assertTrue(sink.producer.flushed and sink.producer.closed)


if __name__ == "__main__":
    unittest.main()
