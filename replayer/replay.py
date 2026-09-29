#!/usr/bin/env python3
"""Replay DEBS 2022 trading data CSV files as a paced "real-time" event stream.

Reads the CSV row by row, paces emission according to the event `Time` column
(scaled by --speed) and writes one JSON object per event to a sink.

Every option can also be set via an environment variable (REPLAY_<OPTION>),
command line flags take precedence.

Examples:
    # real time, all events, JSON lines on stdout
    python replayer/replay.py data/debs2022-gc-trading-day-08-11-21.csv

    # price events only, 60x speed, starting at market open
    python replayer/replay.py data/*.csv --only-prices --speed 60 --start 08:00

    # as fast as possible into a TCP socket
    python replayer/replay.py data/*.csv --speed 0 --sink tcp --tcp-port 9999
"""

import argparse
import csv
import json
import os
import socket
import sys
import time

# Columns we forward (CSV header name -> output field).
FIELDS = {
    "ID": "id",
    "SecType": "sec_type",
    "Date": "date",
    "Time": "time",
    "Last": "last",
    "Trading time": "trading_time",
    "Trading date": "trading_date",
}


def env(name, default=None):
    return os.environ.get(f"REPLAY_{name.upper()}", default)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("files", nargs="*", default=env("files", "").split(), help="CSV file(s), replayed in order")
    p.add_argument("--speed", type=float, default=float(env("speed", 1.0)),
                   help="replay speed factor: 1 = real time, 60 = one minute per second, 0 = no pacing")
    p.add_argument("--only-prices", action="store_true", default=env("only_prices", "") == "1",
                   help="only emit events that carry a non-zero Last price")
    p.add_argument("--symbols", default=env("symbols", ""),
                   help="comma-separated symbol IDs to emit, e.g. RDSA.NL,SIE.ETR (default: all)")
    p.add_argument("--start", default=env("start", ""), help="skip events before this time (HH:MM[:SS])")
    p.add_argument("--end", default=env("end", ""), help="stop at events after this time (HH:MM[:SS])")
    p.add_argument("--limit", type=int, default=int(env("limit", 0)), help="stop after N emitted events (0 = no limit)")
    p.add_argument("--sink", choices=["stdout", "tcp"], default=env("sink", "stdout"))
    p.add_argument("--tcp-host", default=env("tcp_host", "localhost"))
    p.add_argument("--tcp-port", type=int, default=int(env("tcp_port", 9999)))
    p.add_argument("--progress", type=int, default=int(env("progress", 100_000)),
                   help="log progress to stderr every N emitted events (0 = off)")
    args = p.parse_args(argv)
    if not args.files:
        p.error("no input files given (argument or REPLAY_FILES)")
    return args


def to_seconds(hms):
    """'HH:MM[:SS[.fff]]' -> seconds since midnight."""
    parts = hms.split(":")
    h, m = int(parts[0]), int(parts[1])
    s = float(parts[2]) if len(parts) > 2 else 0.0
    return h * 3600 + m * 60 + s


def read_events(path):
    """Yield dicts with the FIELDS of each event row; empty values become None."""
    with open(path, newline="") as f:
        rows = csv.reader(line for line in f if not line.startswith("#"))
        header = [h.strip() for h in next(rows)]
        idx = {out: header.index(col) for col, out in FIELDS.items()}
        for row in rows:
            if len(row) < len(header):
                continue
            yield {out: (row[i] or None) for out, i in idx.items()}


class StdoutSink:
    def send(self, line):
        sys.stdout.write(line + "\n")

    def close(self):
        sys.stdout.flush()


class TcpSink:
    def __init__(self, host, port, retries=30):
        # The receiver may still be starting up (e.g. under docker compose).
        for attempt in range(retries):
            try:
                self.sock = socket.create_connection((host, port))
                break
            except OSError as e:
                if attempt == retries - 1:
                    raise
                log(f"cannot connect to {host}:{port} ({e}), retrying")
                time.sleep(1)
        self.out = self.sock.makefile("w", encoding="utf-8")

    def send(self, line):
        self.out.write(line + "\n")

    def close(self):
        self.out.close()
        self.sock.close()


def make_sink(args):
    if args.sink == "tcp":
        return TcpSink(args.tcp_host, args.tcp_port)
    return StdoutSink()


def log(msg):
    print(f"[replay] {msg}", file=sys.stderr, flush=True)


def replay(args, sink):
    symbols = set(filter(None, (s.strip() for s in args.symbols.split(","))))
    start = to_seconds(args.start) if args.start else None
    end = to_seconds(args.end) if args.end else None

    emitted = 0
    skipped = 0  # rows without a parsable Time
    clock_start = None  # (wall time, event time) of the first emitted event
    wall_begin = time.monotonic()

    for path in args.files:
        log(f"reading {path}")
        for ev in read_events(path):
            try:
                t = to_seconds(ev["time"])
            except (AttributeError, ValueError, IndexError):
                skipped += 1
                if skipped <= 3:
                    log(f"skipping row with bad time: {ev}")
                continue
            if start is not None and t < start:
                continue
            if end is not None and t > end:
                return emitted, skipped
            # Last == 0 rows are placeholders (Trading time 00:00:00) with
            # arbitrary timestamps, not trades.
            if args.only_prices and (ev["last"] is None or float(ev["last"]) == 0):
                continue
            if symbols and ev["id"] not in symbols:
                continue

            # Pace by event time. Events are only roughly ordered, so never
            # sleep for an event that is older than the replay clock.
            if args.speed > 0:
                if clock_start is None:
                    clock_start = (time.monotonic(), t)
                due = clock_start[0] + (t - clock_start[1]) / args.speed
                delay = due - time.monotonic()
                if delay > 0:
                    time.sleep(delay)

            sink.send(json.dumps(ev))
            emitted += 1
            if args.progress and emitted % args.progress == 0:
                rate = emitted / max(time.monotonic() - wall_begin, 1e-9)
                log(f"{emitted} events, event time {ev['date']} {ev['time']}, {rate:.0f} ev/s")
            if args.limit and emitted >= args.limit:
                return emitted, skipped
    return emitted, skipped


def main():
    args = parse_args()
    sink = make_sink(args)
    try:
        emitted, skipped = replay(args, sink)
        log(f"done, {emitted} events emitted, {skipped} rows skipped (bad time)")
    except (KeyboardInterrupt, BrokenPipeError):
        pass
    finally:
        try:
            sink.close()
        except BrokenPipeError:
            pass


if __name__ == "__main__":
    main()
