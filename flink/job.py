#!/usr/bin/env python3
"""PyFlink job: the stream processor on Flink, reading the replayer's Kafka topic.

Run from the repository root: python3 -m flink.job (docker compose --profile flink up).

    Kafka source -> parse/validate -> timestamps + watermarks -> key_by(symbol) -> windows + EMA

- The source reads the whole topic and stops at the offsets that exist when the job
  starts, so run it after the producer has finished (docker compose does).
- Each source subtask reads one partition, so Flink keeps one watermark per
  partition (bounded out of orderness = RECEIVER_ALLOWED_LATENESS_SECONDS) and the
  keyed operator uses their minimum. This is what makes the job independent of how
  partitions and parallelism relate; the plain Kafka worker has to use exactly one
  partition per worker. The source parallelism is therefore the partition count;
  FLINK_PARALLELISM sets the parallelism of the keyed window operator (default: same).
- The window operator keeps one SymbolWindows per symbol as keyed state and one
  event-time timer per open window (flink/logic.py), and writes rows to
  <RECEIVER_OUTPUT>/flink-windows-<time>.w<subtask>.jsonl like the Kafka workers do. Each subtask
  writes its own file, so this runs as a local mini cluster inside one container.
- Windows that are incomplete at the end of input are withheld, as elsewhere,
  unless RECEIVER_FLUSH_PARTIAL=1 (rows then carry partial: true).

Per-subtask statistics are collected in a temporary directory and merged into one
{"kind": "flink"} record file in the RECEIVER_BENCH_OUTPUT directory. Throughput runs from the first
record read to the last window closed and excludes JVM startup; the Python CPU and
memory figures cover only the Python worker processes, not the JVM (use
`docker stats` for the whole container). Latency is not measured: the topic is
pre-filled, so the send-to-emit time would only show how long records waited.
"""

import json
import os
from pathlib import Path
import shutil
import tempfile
import time

from pyflink.common import Configuration, Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (KeyedProcessFunction, ProcessFunction,
                                StreamExecutionEnvironment)
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.state import ValueStateDescriptor

from flink.logic import (LATE, MAX_WATERMARK, NEW, SymbolWindows, add_event, aggregate_stats,
                         finish_window, start_of_timer, timer_ms)
from receiver.bench import host_info, peak_rss_mb, run_stamp, write_result
from receiver.kafka_worker import output_path
from receiver.processor import StreamProcessor

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "ticks")
PARALLELISM = int(os.environ.get("FLINK_PARALLELISM", 0))  # 0 = one per partition
WATERMARK_INTERVAL_MS = int(os.environ.get("FLINK_WATERMARK_INTERVAL_MS", 50))
KAFKA_JAR = os.environ.get("FLINK_KAFKA_JAR", "/opt/flink-sql-connector-kafka.jar")
FLINK_CONF = os.environ.get("FLINK_CONF", "")  # extra Flink options, e.g. "python.state.cache-size=100000,key=value"
LATENESS = float(os.environ.get("RECEIVER_ALLOWED_LATENESS_SECONDS", 0))
OUTPUT = os.environ.get("RECEIVER_OUTPUT", "")
INCLUDE_PARTIAL = os.environ.get("RECEIVER_FLUSH_PARTIAL", "0") == "1"
SUBSCRIPTIONS = os.environ.get("RECEIVER_SYMBOLS", "")
BENCH_OUTPUT = os.environ.get("RECEIVER_BENCH_OUTPUT", "")
BENCH_LABEL = os.environ.get("RECEIVER_BENCH_LABEL", "")

PARSED = Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.LONG(), Types.BOOLEAN()])


def log(message):
    print(f"[flink-job] {message}", flush=True)


class Stats:
    """Time span, Python CPU and one JSON record per subtask, written on close().

    Times are CLOCK_MONOTONIC, which all processes on the machine share and which is
    not affected by clock adjustments (the Docker VM's wall clock jumps when the Mac sleeps).
    """

    def __init__(self, directory, stage, subtask, run_id):
        self.path = os.path.join(directory, f"{stage}-{subtask}.json")
        self.record = {"stage": stage, "subtask": subtask, "run_id": run_id, "pid": os.getpid()}
        self.started_ns = self.ended_ns = None
        self.cpu_begin = time.process_time()

    def touch(self):
        self.ended_ns = time.monotonic_ns()
        if self.started_ns is None:
            self.started_ns = self.ended_ns

    def write(self, events, counters):
        self.record.update(
            events=events, counters=counters,
            started_ns=self.started_ns, ended_ns=self.ended_ns,
            python_cpu_seconds=time.process_time() - self.cpu_begin,
            python_peak_rss_mb=peak_rss_mb(),
        )
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.record, f)


class ParseFunction(ProcessFunction):
    """JSON line -> (symbol, price, trading timestamp us, index_date_fallback), reusing StreamProcessor.parse."""

    def __init__(self, stats_dir, run_id):
        self.stats_dir, self.run_id = stats_dir, run_id

    def open(self, runtime_context):
        self.parser = StreamProcessor()
        self.lines = self.malformed = self.eos = 0
        self.stats = Stats(self.stats_dir, "parse", runtime_context.get_index_of_this_subtask(), self.run_id)

    def process_element(self, value, ctx):
        self.stats.touch()
        self.lines += 1
        try:
            event = json.loads(value)
        except ValueError:
            self.malformed += 1
            return
        if isinstance(event, dict) and event.get("type") == "eos":
            self.eos += 1
            return
        parsed = self.parser.parse(event)
        if parsed is not None:
            yield parsed

    def close(self):
        m = self.parser.metrics
        self.stats.write(m.received, {"lines": self.lines, "malformed": self.malformed, "eos": self.eos,
                                      "received": m.received, "invalid": m.invalid, "no_price": m.no_price})


class EventTime(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[2] // 1000  # trading timestamp us -> ms


class WindowFunction(KeyedProcessFunction):
    """Per-symbol five-minute windows and EMAs; a window closes when its event-time timer fires."""

    def __init__(self, stats_dir, run_id, output_base, include_partial, subscriptions):
        self.stats_dir, self.run_id = stats_dir, run_id
        self.output_base, self.include_partial = output_base, include_partial
        self.subscriptions = set(subscriptions) if subscriptions else None

    def open(self, runtime_context):
        self.index = runtime_context.get_index_of_this_subtask()
        self.state = runtime_context.get_state(ValueStateDescriptor("symbol", Types.PICKLED_BYTE_ARRAY()))
        self.stats = Stats(self.stats_dir, "window", self.index, self.run_id)
        self.output = open(output_path(self.output_base, self.index), "w", encoding="utf-8") if self.output_base else None
        self.max_timestamp = None  # newest accepted event time (us) seen by this subtask
        self.symbols = set()
        self.counters = dict.fromkeys(
            ("accepted", "index_date_fallback", "late", "windows", "buy", "sell", "pending_windows"), 0)

    def process_element(self, value, ctx):
        self.stats.touch()
        symbol, price, timestamp, index_date_fallback = value
        sym = self.state.value() or SymbolWindows()
        outcome, start = add_event(sym, timestamp, price, ctx.timer_service().current_watermark())
        if outcome == LATE:
            self.counters["late"] += 1
            return ()
        self.counters["accepted"] += 1
        self.counters["index_date_fallback"] += index_date_fallback
        self.symbols.add(symbol)
        self.state.update(sym)
        if outcome == NEW:
            ctx.timer_service().register_event_time_timer(timer_ms(start))
        if self.max_timestamp is None or timestamp > self.max_timestamp:
            self.max_timestamp = timestamp
        return ()

    def on_timer(self, timestamp, ctx):
        self.stats.touch()
        symbol = ctx.get_current_key()
        sym = self.state.value()
        end_of_input = ctx.timer_service().current_watermark() >= MAX_WATERMARK
        row = finish_window(symbol, sym, start_of_timer(timestamp), end_of_input,
                            self.max_timestamp if self.max_timestamp is not None else 0, self.include_partial)
        if row is None:
            self.counters["pending_windows"] += 1
            return ()
        self.state.update(sym)
        self.counters["windows"] += 1
        if row["advisory"]:
            self.counters[row["advisory"].lower()] += 1
        if self.output is not None and (self.subscriptions is None or symbol in self.subscriptions):
            self.output.write(json.dumps({"run_id": self.run_id, **row}, allow_nan=False) + "\n")
        return ()

    def close(self):
        if self.output is not None:
            self.output.close()
        self.counters["symbols"] = len(self.symbols)
        self.stats.write(self.counters["accepted"] + self.counters["late"], self.counters)


def partition_count():
    from kafka import KafkaConsumer  # only needed to size the job

    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP.split(","))
    try:
        deadline = time.monotonic() + 120
        while not (found := consumer.partitions_for_topic(TOPIC)):
            if time.monotonic() > deadline:
                raise RuntimeError(f"topic {TOPIC!r} not found")
            time.sleep(1)
        return len(found)
    finally:
        consumer.close()


def build_job(env, partitions, parallelism, stats_dir, run_id, output_base):
    subscriptions = [s.strip() for s in SUBSCRIPTIONS.split(",") if s.strip()]
    source = (KafkaSource.builder()
              .set_bootstrap_servers(BOOTSTRAP)
              .set_topics(TOPIC)
              .set_starting_offsets(KafkaOffsetsInitializer.earliest())
              .set_bounded(KafkaOffsetsInitializer.latest())
              .set_value_only_deserializer(SimpleStringSchema())
              .build())
    watermarks = (WatermarkStrategy
                  .for_bounded_out_of_orderness(Duration.of_millis(round(LATENESS * 1000)))
                  .with_timestamp_assigner(EventTime()))
    (env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-ticks")
        .process(ParseFunction(stats_dir, run_id), PARSED).name("parse")
        .assign_timestamps_and_watermarks(watermarks)
        .key_by(lambda v: v[0], key_type=Types.STRING())
        .process(WindowFunction(stats_dir, run_id, output_base, INCLUDE_PARTIAL, subscriptions), Types.STRING())
        .set_parallelism(parallelism).name("windows"))


def main():
    StreamProcessor(LATENESS)  # Validate configuration before starting Flink.
    partitions = partition_count()
    parallelism = PARALLELISM or partitions
    run_id = f"{time.time_ns()}-flink"
    stats_dir = tempfile.mkdtemp(prefix="flink-stats-")
    log(f"topic {TOPIC!r}: {partitions} partitions, window operator parallelism {parallelism}")

    # PyFlink's default for local runs is "loopback" mode, which runs every Python function as a
    # thread of this process (meant for debugging): one GIL for all subtasks. Use real worker processes.
    os.environ["_python_worker_execution_mode"] = "process"
    conf = Configuration()
    for pair in filter(None, (p.strip() for p in FLINK_CONF.split(","))):
        key, _, value = pair.partition("=")
        conf.set_string(key.strip(), value.strip())
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.set_parallelism(partitions)  # source and watermark operators: one subtask per partition
    env.get_config().set_auto_watermark_interval(WATERMARK_INTERVAL_MS)
    env.add_jars(f"file://{KAFKA_JAR}")
    # One set of files per run; every subtask gets the same name from here.
    output_base = os.path.join(OUTPUT, f"flink-windows-{run_stamp()}.jsonl") if OUTPUT else ""
    build_job(env, partitions, parallelism, stats_dir, run_id, output_base)

    begin = time.monotonic()
    env.execute("trading-trend-detector")
    total_seconds = time.monotonic() - begin

    records = [json.loads(p.read_text()) for p in sorted(Path(stats_dir).glob("*.json"))]
    shutil.rmtree(stats_dir, ignore_errors=True)
    if not records:
        raise SystemExit("job produced no statistics; see errors above")
    total = aggregate_stats(records)
    log("run complete; " + json.dumps({"run_id": run_id, "total_seconds": total_seconds, **total}))
    if BENCH_OUTPUT:
        path = write_result(BENCH_OUTPUT, {
            "kind": "flink", "run_id": run_id, "label": BENCH_LABEL,
            "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "host": host_info(), "topic": TOPIC,
            "config": {"allowed_lateness_seconds": LATENESS, "flush_partial": INCLUDE_PARTIAL,
                       "subscriptions": SUBSCRIPTIONS, "partitions": partitions, "parallelism": parallelism,
                       "watermark_interval_ms": WATERMARK_INTERVAL_MS},
            "total_seconds": total_seconds, **total,
            "per_subtask": [{k: v for k, v in r.items() if k != "run_id"} for r in records],
        })
        log(f"benchmark written to {path}")


if __name__ == "__main__":
    main()
