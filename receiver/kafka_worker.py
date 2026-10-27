#!/usr/bin/env python3
"""Kafka consumer workers running the stream processor, partitioned by symbol.

Run from the repository root: python3 -m receiver.kafka_worker.

KAFKA_WORKERS processes are started, and the topic must have exactly that many
partitions: worker i is statically assigned partition i (no consumer group, so
no rebalancing can move a symbol's state mid-stream). Every symbol maps to one
partition, so each worker owns a disjoint set of symbols and runs an
independent StreamProcessor. A worker finishes when it has seen the replayer's
{"type": "eos"} marker on its partition, then writes its results and benchmark.
Window finalization uses each worker's own watermark, which only advances from
its own symbols.

One partition per worker is required for correctness. A processor reading
several partitions sees them at uneven event-time progress (partitions differ
in density), so one partition racing ahead advances the single watermark and
the others' events are dropped as late. On a sample of 300k real events a
single worker over 4 partitions lost 1,003 windows and changed 9,997 more.
To run N workers use a topic with N partitions (KAFKA_WORKERS sets both in
docker-compose.yml); set KAFKA_ALLOW_PARTITION_MISMATCH=1 to override.

Every run must read a fresh topic: workers read from the beginning, so old
messages and end markers from a previous run would be mixed in. Use a new
KAFKA_TOPIC, or `docker compose down`, between runs.
"""

import json
import multiprocessing
import os
from pathlib import Path
import queue
import sys
import time

from .bench import Bench, host_info, percentile, run_stamp, write_result
from .processor import StreamProcessor

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "ticks")
WORKERS = int(os.environ.get("KAFKA_WORKERS", 4))
PRINT_EVERY = int(os.environ.get("RECEIVER_PRINT_EVERY", 100_000))
LATENESS = float(os.environ.get("RECEIVER_ALLOWED_LATENESS_SECONDS", 0))
OUTPUT = os.environ.get("RECEIVER_OUTPUT", "")
INCLUDE_PARTIAL = os.environ.get("RECEIVER_FLUSH_PARTIAL", "0") == "1"
SUBSCRIPTIONS = os.environ.get("RECEIVER_SYMBOLS", "")
BENCH_OUTPUT = os.environ.get("RECEIVER_BENCH_OUTPUT", "")
BENCH_LABEL = os.environ.get("RECEIVER_BENCH_LABEL", "")
ALLOW_MISMATCH = os.environ.get("KAFKA_ALLOW_PARTITION_MISMATCH", "0") == "1"


def log(message):
    print(f"[kafka-worker] {message}", file=sys.stderr, flush=True)


def partitions_for_worker(all_partitions, worker, workers):
    return [p for p in sorted(all_partitions) if p % workers == worker]


def check_partitions(found, workers, allow_mismatch=False):
    """Require one partition per worker; see the module docstring for why."""
    if len(found) != workers and not allow_mismatch:
        raise RuntimeError(
            f"topic has {len(found)} partitions but KAFKA_WORKERS={workers}; they must match "
            "(a worker reading several partitions drops events as late). Recreate the topic "
            "with the right partition count or set KAFKA_ALLOW_PARTITION_MISMATCH=1")


def output_path(base, worker):
    """windows.jsonl -> windows.w2.jsonl, so workers never share a file."""
    path = Path(base)
    return str(path.with_name(f"{path.stem}.w{worker}{path.suffix}"))


def consume(consumer, partitions, processor, emit, bench, tag="", print_every=0):
    """Process records until every partition in `partitions` has delivered its end marker.

    `consumer.poll()` must return {topic_partition: [records]} like kafka-python.
    Returns (lines, malformed).
    """
    waiting = set(partitions)
    lines = malformed = 0
    while waiting:
        for tp, records in consumer.poll(timeout_ms=1000).items():
            for record in records:
                lines += 1
                try:
                    event = json.loads(record.value)
                except (ValueError, UnicodeDecodeError):
                    malformed += 1
                    continue
                if isinstance(event, dict) and event.get("type") == "eos":
                    waiting.discard(tp.partition)
                    continue
                began = time.perf_counter()
                rows = processor.process(event)
                bench.processed(began)
                emit(rows)
                if rows and isinstance(event, dict):
                    bench.emitted(event.get("sent_ns"), len(rows))
                if print_every and lines % print_every == 0:
                    log(f"{tag} #{lines} {processor.summary()}; waiting for end of partitions {sorted(waiting)}")
    return lines, malformed


def run_worker(worker, results, output_base):
    """Process entry point: consume this worker's partitions, put the result in `results`."""
    from kafka import KafkaConsumer, TopicPartition  # imported lazily: only workers need it

    tag = f"w{worker}"
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP.split(","),
        enable_auto_commit=False, auto_offset_reset="earliest",
        max_poll_records=5000, max_partition_fetch_bytes=4 * 1024 * 1024,
    )
    deadline = time.monotonic() + 120
    while not (found := consumer.partitions_for_topic(TOPIC)):
        if time.monotonic() > deadline:
            raise RuntimeError(f"topic {TOPIC!r} not found")
        time.sleep(1)
    check_partitions(found, WORKERS, ALLOW_MISMATCH)
    mine = partitions_for_worker(found, worker, WORKERS)
    consumer.assign([TopicPartition(TOPIC, p) for p in mine])
    log(f"{tag} owns partitions {mine} of {len(found)}")

    subscriptions = set(filter(None, (s.strip() for s in SUBSCRIPTIONS.split(",")))) or None
    processor = StreamProcessor(LATENESS, subscriptions)
    bench = Bench()
    run_id = f"{time.time_ns()}-{tag}"
    output = open(output_path(output_base, worker), "w", encoding="utf-8") if output_base else sys.stdout

    def emit(rows):
        for row in rows:
            output.write(json.dumps({"run_id": run_id, **row}, allow_nan=False) + "\n")
        if rows:
            output.flush()

    try:
        lines, malformed = consume(consumer, mine, processor, emit, bench, tag, PRINT_EVERY)
        emit(processor.finish(include_partial=INCLUDE_PARTIAL))
        output.flush()
    finally:
        if output is not sys.stdout:
            output.close()
        consumer.close()
    summary = bench.summary()
    counters = {"lines": lines, "malformed": malformed, **processor.summary()}
    log(f"{tag} done; {json.dumps({'partitions': mine, **counters, 'events_per_second': summary['events_per_second']})}")
    results.put({
        "worker": worker, "run_id": run_id, "partitions": mine,
        "counters": counters, "latencies_ms": bench.latencies_ms, **summary,
    })


def aggregate(workers, results):
    """Combine per-worker results into one record for the whole run."""
    started = [r["started_at_ns"] for r in results if r["started_at_ns"] is not None]
    ended = [r["started_at_ns"] + r["active_seconds"] * 1e9 for r in results if r["started_at_ns"] is not None]
    wall = (max(ended) - min(started)) / 1e9 if started else 0.0
    events = sum(r["events"] for r in results)
    latencies = sorted(l for r in results for l in r["latencies_ms"])
    counters = {}
    for r in results:
        for key, value in r["counters"].items():
            counters[key] = counters.get(key, 0) + value
    return {
        "workers": workers,
        "events": events,
        "active_seconds": wall,
        "events_per_second": events / wall if wall > 0 else None,
        "worker_events": {f"w{r['worker']}": r["events"] for r in results},
        "cpu_seconds": sum(r["cpu_seconds"] for r in results),
        "peak_rss_mb_total": sum(r["peak_rss_mb"] or 0 for r in results),
        "latency_ms": {
            "samples": len(latencies),
            "p50": percentile(latencies, 0.50), "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99), "max": latencies[-1] if latencies else None,
        },
        "counters": counters,
    }


def main():
    StreamProcessor(LATENESS)  # Validate configuration before starting workers.
    if WORKERS < 1:
        raise SystemExit("KAFKA_WORKERS must be at least 1")
    log(f"starting {WORKERS} workers on {BOOTSTRAP} topic {TOPIC!r}")
    # One set of files per run; all workers share the start time in their names.
    output_base = os.path.join(OUTPUT, f"kafka-windows-{run_stamp()}.jsonl") if OUTPUT else ""
    results = multiprocessing.Queue()
    processes = [multiprocessing.Process(target=run_worker, args=(i, results, output_base))
                 for i in range(WORKERS)]
    for process in processes:
        process.start()
    collected = []
    while len(collected) < WORKERS:
        try:
            collected.append(results.get(timeout=2))
        except queue.Empty:  # fail fast if every worker died without a result
            if not any(p.is_alive() for p in processes) and results.empty():
                break
    for process in processes:
        process.join()
    if len(collected) < WORKERS:
        log(f"only {len(collected)} of {WORKERS} workers finished; see errors above")
        raise SystemExit(1)

    total = aggregate(WORKERS, collected)
    log("run complete; " + json.dumps(total))
    if BENCH_OUTPUT:
        per_worker = [{k: v for k, v in r.items() if k != "latencies_ms"} for r in collected]
        path = write_result(BENCH_OUTPUT, {
            "kind": "kafka", "label": BENCH_LABEL,
            "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "host": host_info(), "topic": TOPIC,
            "config": {"allowed_lateness_seconds": LATENESS, "flush_partial": INCLUDE_PARTIAL,
                       "subscriptions": SUBSCRIPTIONS},
            **total, "per_worker": per_worker,
        })
        log(f"benchmark written to {path}")


if __name__ == "__main__":
    main()
