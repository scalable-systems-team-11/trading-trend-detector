#!/usr/bin/env python3
"""Benchmark the stream processor in-process, without TCP or pacing.

Input is either a DEBS CSV (price rows only, as with the replayer's
--only-prices) or a JSON-lines event file extracted once with the replayer:

    python3 replayer/replay.py data/debs2022-gc-trading-day-08-11-21.csv \\
        --speed 0 --only-prices --no-stamp --progress 0 > data/prices-08-11-21.jsonl
    python3 bench/bench_processor.py data/prices-08-11-21.jsonl --label baseline

Input parsing (CSV/JSON decoding) is timed separately from the processor, so
processor_events_per_second is the upper bound of the single-process detector.
"""

import argparse
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from receiver.bench import Bench, host_info, write_result  # noqa: E402
from receiver.processor import StreamProcessor  # noqa: E402
from replayer.replay import read_events  # noqa: E402


def events(path):
    if path.endswith(".csv"):
        for ev in read_events(path):
            if ev["last"] is not None and float(ev["last"]) != 0:
                yield ev
    else:
        with open(path, encoding="utf-8") as f:
            for line in f:
                yield json.loads(line)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("file", help="DEBS CSV or JSON-lines events file")
    p.add_argument("--limit", type=int, default=0, help="stop after N events (0 = all)")
    p.add_argument("--lateness", type=float, default=0.0, help="allowed lateness in seconds")
    p.add_argument("--label", default="", help="free-text label stored with the result")
    p.add_argument("--output", default="output/benchmarks.jsonl", help="append the result here ('' = off)")
    p.add_argument("--progress", type=int, default=1_000_000, help="log every N events (0 = off)")
    args = p.parse_args()

    processor = StreamProcessor(args.lateness)
    bench = Bench()
    rows = 0
    wall_begin = time.perf_counter()
    for ev in events(args.file):
        began = time.perf_counter()
        rows += len(processor.process(ev))
        bench.processed(began)
        if args.progress and bench.events % args.progress == 0:
            print(f"[bench] {bench.events} events, "
                  f"{bench.events / bench.process_seconds:.0f} ev/s in processor", file=sys.stderr, flush=True)
        if args.limit and bench.events >= args.limit:
            break
    rows += len(processor.finish())
    wall = time.perf_counter() - wall_begin

    result = {
        "kind": "processor", "label": args.label, "input": args.file,
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "host": host_info(),
        "config": {"allowed_lateness_seconds": args.lateness, "limit": args.limit},
        "counters": {**processor.summary(), "rows": rows},
        **bench.summary(),
        "total_seconds_including_parsing": wall,
    }
    result.pop("latency_ms")  # No transport, so no end-to-end latency here.
    print(json.dumps(result, indent=2))
    if args.output:
        write_result(args.output, result)
        print(f"[bench] appended to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
