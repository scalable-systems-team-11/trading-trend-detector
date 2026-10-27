## CS-E4780 Scalable Systems and Data Management Course Project
### Detecting Trading Trends in Financial Tick Data

### Data

We use the [DEBS 2022 Grand Challenge Trading Data](https://doi.org/10.5281/zenodo.6382482)
(one week of tick data from 8-14 Nov 2021, one CSV per day, ~5 GB per weekday).
CSV files go in `data/` (git-ignored). (Currently have been using a subset of monday to test)


### Running

```
docker compose up --build
```

This starts two services:

- **replayer** ([replayer/replay.py](replayer/replay.py)): reads the CSV and replays the
  events as a paced stream (JSON lines), by default price events from 08:00 at 60x speed.
- **receiver** ([receiver/receiver.py](receiver/receiver.py)): TCP ingestion feeding
  [the processor](receiver/processor.py), which calculates per-symbol five-minute
  windows, EMA38/EMA100 and BUY/SELL crossovers. Diagnostics and rate summaries
  go to stderr; each run writes its result rows to a new
  `output/windows-<UTC time>.jsonl` on the host.

Both are configured with environment variables (`REPLAY_*`, `RECEIVER_*`); the defaults are in
[docker-compose.yml](docker-compose.yml). To change them, copy [.env.example](.env.example) to
`.env` (git-ignored) and uncomment what you need. Docker Compose reads `.env` automatically.
For example, a full day as fast as possible:

```
REPLAY_SPEED=0
REPLAY_START=
```

An empty `REPLAY_START=` means from midnight.

Parsing the CSV is the slowest part of a replay (~59k events/s). For faster runs, extract the
price events once and replay the `.jsonl` instead (~150k events/s, same events):

```
python3 replayer/replay.py data/debs2022-gc-trading-day-08-11-21.csv --speed 0 \
    --only-prices --no-stamp --progress 0 > data/prices-08-11-21.jsonl
```

Then set `REPLAY_FILES=/data/prices-08-11-21.jsonl` in `.env`.

The receiver keeps listening after the replay ends; stop it with Ctrl+C once it has logged its
`closed;` summary. Run `python3 replayer/replay.py --help` for all replayer options.

### Data quirks

- Events are only roughly ordered by `Time`; small backwards jumps occur.
- ~12k rows per day have no `Time` at all; the replayer skips them for now.
- Rows with `Last = 0.000000` (and `Trading time = 00:00:00.000`) are placeholders with
  arbitrary timestamps, not trades; `--only-prices` drops them.
- `--end` skips later rows instead of stopping, since rows can be out of order
  (so the whole file is always scanned).
- Index rows (`SecType=I`) have no `Trading date`; the processor uses the system `Date` for them.

### Stream processing

[receiver/processor.py](receiver/processor.py) groups events into 5-minute windows per symbol
(by `Trading date`/`Trading time`), takes the last price as the window's close, updates
EMA38/EMA100 (starting from 0) and emits `BUY`/`SELL` on crossovers. Each window produces one
JSON line with `symbol`, `window_start`, `window_end`, `close`, `ema38`, `ema100` and `advisory`.

Receiver options (in [docker-compose.yml](docker-compose.yml)):

- `RECEIVER_ALLOWED_LATENESS_SECONDS` (default 0): how long to wait for out-of-order events
  before closing a window.
- `RECEIVER_SYMBOLS`: only output these symbols (all symbols are still calculated).
- `RECEIVER_FLUSH_PARTIAL=1`: also output the last unfinished window on disconnect.

```bash
# Linux / Cloud Shell
RECEIVER_ALLOWED_LATENESS_SECONDS=2 docker compose up --build
```

```powershell
# Windows PowerShell
$env:RECEIVER_ALLOWED_LATENESS_SECONDS = "2"
docker compose up --build
```

More details and open decisions: [docs/stream_processor.md](docs/stream_processor.md).

### Benchmarking

Every receiver run reports a `benchmark` block in its final `closed;` log line
and, under Docker, writes it to its own folder `results/benchmarks/<UTC time>-<kind>-<label>/result.json`
(set the `RECEIVER_BENCH_OUTPUT` directory for direct Python runs). Other files of a run, such as
`docker stats` samples, go in the same folder. These files are
committed: they are the evidence for the design choices in the report, so keep them.
Label runs to tell them apart:

```bash
BENCH_LABEL=speed0-fullday docker compose up --build
```

| Field | Meaning |
| --- | --- |
| `events_per_second` | Received events / time from first to last event (includes pacing waits) |
| `processor_events_per_second` | Events / time spent inside `StreamProcessor.process` only |
| `cpu_seconds`, `cpu_utilization` | Receiver CPU time and CPU/wall ratio (1.0 = one core busy) |
| `peak_rss_mb` | Peak receiver memory (not reported on Windows) |
| `latency_ms` | p50/p95/p99/max from replayer send (`sent_ns`) to window row written |

The replayer stamps `sent_ns` on each event (disable with `--no-stamp`), so
latency is only valid when replayer and receiver share a clock. All windows
closing at one boundary share one trigger event, so percentiles are coarse.
Measure throughput with `REPLAY_SPEED=0` and latency at a paced speed below
saturation, in separate runs.

To benchmark the processor alone (no TCP, no CSV re-scan), extract price
events once and run the in-process benchmark on that file:

```bash
python3 replayer/replay.py data/debs2022-gc-trading-day-08-11-21.csv \
    --speed 0 --only-prices --no-stamp --progress 0 > data/prices-08-11-21.jsonl
python3 bench/bench_processor.py data/prices-08-11-21.jsonl --label baseline
```

The result goes to `results/benchmarks/` with `"kind": "processor"`.

### Kafka pipeline

An alternative to the TCP receiver, partitioned by symbol so the processor can
scale across worker processes:

```
replayer --(key = symbol)--> Kafka topic, W partitions --> W worker processes --> output/kafka-windows-<time>.w<i>.jsonl
```

- **Broker:** Apache Kafka (KRaft, single node) from `docker-compose.yml`; the
  `kafka-init` service creates the topic with `KAFKA_WORKERS` partitions.
- **Producer:** `replay.py --sink kafka` (service `producer`). It keys events by
  symbol, so each symbol always lands on one partition, and finishes by sending an
  end-of-stream marker to every partition.
- **Workers:** [receiver/kafka_worker.py](receiver/kafka_worker.py) starts
  `KAFKA_WORKERS` processes in one container; worker *i* reads partition *i*
  with its own `StreamProcessor` (same code as the TCP path). **Workers must equal
  partitions**: a worker reading several partitions drops events as late because
  the partitions advance unevenly in event time. The worker refuses to start otherwise.
- Kafka services are in the `kafka` compose profile; plain `docker compose up` still
  runs the TCP pipeline.

```bash
# unpaced, 4 workers; add REPLAY_START= for the whole day
BENCH_LABEL=kafka-4w KAFKA_WORKERS=4 docker compose --profile kafka up --build
# Ctrl+C once the worker logs "run complete", then reset the broker:
docker compose --profile kafka down
```

Always run `down` (or use a new `KAFKA_TOPIC`) between runs: workers read the topic
from the beginning, so a topic reused from an earlier run would mix in old data.
Per-worker rows go to `output/kafka-windows-<time>.w<i>.jsonl`, and one `"kind": "kafka"`
record with totals, per-worker numbers and merged latency percentiles is written to
`results/benchmarks/`. Throughput there is first-event to last-event over all
workers, so it excludes startup but includes any wait on the producer; the producer
parsing the CSV is usually the limit. Latency here also includes time spent in Kafka.

Check that the results equal the TCP pipeline's, for the same input range:

```bash
python3 bench/compare_outputs.py output/windows-<run time>.jsonl "output/kafka-windows-<time>.w*.jsonl"
```

Each worker's watermark only advances from its own symbols, so a worker can close
windows later than the single processor. The output should match, but late-event
drops can differ slightly for out-of-order input.

### Flink pipeline

A third option on the same Kafka topic: a PyFlink job ([flink/job.py](flink/job.py))
instead of the worker processes.

```
replayer --(key = symbol)--> Kafka topic, P partitions --> Flink job --> output/flink-windows-<time>.w<i>.jsonl
```

- **Job:** Kafka source (one subtask per partition) -> parse/validate (reuses
  `StreamProcessor.parse`) -> timestamps and watermarks -> `key_by(symbol)` -> window
  operator. The window operator keeps each symbol's open windows and EMAs as Flink keyed
  state and closes a window with an event-time timer ([flink/logic.py](flink/logic.py)).
  EMA, advisory and output row come from the same functions as the TCP and Kafka paths.
- **Watermarks:** Flink keeps one watermark per partition (bounded out of orderness =
  `RECEIVER_ALLOWED_LATENESS_SECONDS`) and the window operator uses their minimum.
  A fast partition therefore cannot make slower partitions' events late, which is what
  forces the Kafka workers to use exactly one partition each. `FLINK_PARALLELISM` sets
  the window operator's parallelism (default: one per partition) and may differ from
  the partition count (`KAFKA_PARTITIONS`, default `KAFKA_WORKERS`).
- **Input:** the job reads the topic from the start up to the offsets that exist when it
  starts, so it runs after the producer has finished (compose enforces this) and the
  container exits when the topic is consumed. Throughput is therefore a consumer-only
  figure; latency is not measured, because records have been waiting in the topic.

```bash
BENCH_LABEL=flink-4p docker compose --profile flink run --rm flink
docker compose --profile flink down      # reset the broker before the next run
python3 bench/compare_outputs.py output/windows-<run time>.jsonl "output/flink-windows-<time>.w*.jsonl"
```

Add `REPLAY_LIMIT=300000` for a small test. The benchmark record has `"kind": "flink"` and
per-subtask details; its CPU and memory figures cover only the Python worker processes,
not the JVM (use `docker stats` for the whole container).

**Correctness.** On a 300k-event slice the output is identical to the TCP receiver's (same
rows, same counters). On the full Monday it has the same 296,216 rows with none missing,
but 2,617 rows in 47 symbols differ: the TCP receiver's single global watermark dropped
68 events as late, the Flink job dropped none (one watermark per partition), and EMA
carries the difference into later windows. In all 47 symbols the Flink row has the extra
events. The Kafka workers dropped 48 and lost 19 final-window rows on the same data.

**Speed** (8-core Docker VM, 4 partitions, unpaced, consumer only): ~21k events/s on the
full day (550 s, ~1,800 s of Python CPU in 4 worker processes plus the JVM), ~23k on
2M events. Each Python worker handles ~5-6k events/s because every record crosses the
JVM/Python boundary several times (parse, timestamp, key selector, window), so this is
slower than the plain-Python processor (~600k events/s on its own) and about as fast as the
producer-bound Kafka workers. Things that did not help: a larger Python state cache
(23.6k vs 23.3k), and `python.execution-mode=thread` (GIL-bound at ~1.5 cores, more than
7x slower). PyFlink's default for local runs is "loopback" mode, which runs all Python
functions as threads of one process (~6-8k events/s); the job switches to real worker
processes (`_python_worker_execution_mode=process`). Extra Flink options can be passed
with `FLINK_CONF="key=value,key=value"`.

Notes: PyFlink 1.20 needs Python 3.11 and a JVM, so the job has its own image
([flink/Dockerfile](flink/Dockerfile)). PyFlink has no Linux aarch64 wheels, so on Apple
Silicon the first build compiles it from source (a few minutes). The job runs as a local
mini cluster in one container: each subtask writes its own output file and statistics
file, which would not work across machines.

### Running without Docker / tests

Python 3.10+, no extra packages
From the repository root, run the tests:

```bash
python3 -m unittest discover -s tests -v
```

Start the receiver:

```bash
python3 -m receiver.receiver
```

In a second terminal, feed a real CSV file to the running receiver:

```bash
python3 replayer/replay.py data/debs2022-gc-trading-day-08-11-21.csv --speed 0 --only-prices --sink tcp
```
