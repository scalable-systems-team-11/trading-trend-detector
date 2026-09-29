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
- **receiver** ([receiver/receiver.py](receiver/receiver.py)): minimal TCP server that
  receives the stream and logs every 10 000th event. Placeholder for the real processing.

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
