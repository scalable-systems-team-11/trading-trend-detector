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
  go to stderr; result rows are appended to `output/windows.jsonl` on the host.

Both are configured with environment variables in [docker-compose.yml](docker-compose.yml)
(`REPLAY_*`, `RECEIVER_*`). Run `python3 replayer/replay.py --help` for all replayer
options. Useful overrides:

```
docker compose run --rm -e REPLAY_SPEED=0 replayer      # no pacing, as fast as possible
docker compose run --rm -e REPLAY_SYMBOLS=ENGI.FR replayer
```

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
