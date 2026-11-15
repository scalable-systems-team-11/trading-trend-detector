"""Live dashboard for the window rows written by the TCP receiver, Kafka workers or Flink job.

Follows the newest run in the output directory (``windows-*.jsonl``, ``kafka-windows-*.w<i>.jsonl``,
``flink-windows-*.w<i>.jsonl``) while the pipeline writes it, keeps per-symbol series in memory and
serves them to a browser page: a chart per subscribed symbol (close, EMA38, EMA100 with BUY/SELL
markers), a live advisory feed and per-exchange market breadth. The pipeline itself is unchanged:
the dashboard only reads the files it already writes, so it works the same for all three paths.

Usage:
    python3 -m dashboard.server                              # follow new runs in ./output
    python3 -m dashboard.server --load "output/windows-<time>.jsonl"   # show a finished run

Standard library only. Environment variables: DASHBOARD_OUTPUT, DASHBOARD_LOAD, DASHBOARD_PORT.
"""

import argparse
import glob
import json
import os
import queue
import re
import sys
import threading
import time
from array import array
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
RUN_FILE = re.compile(r"^((?:kafka-|flink-)?windows-.+?)(?:\.w\d+)?\.jsonl$")
EXCHANGES = ("FR", "NL", "ETR")
POLL_SECONDS = 0.25


def log(message):
    print(f"[dashboard] {message}", file=sys.stderr, flush=True)


def exchange_of(symbol):
    return symbol.rsplit(".", 1)[-1] if "." in symbol else "?"


class Series:
    """One symbol's finished windows, stored compactly (a full week is ~1.9M rows)."""
    __slots__ = ("end", "close", "ema38", "ema100", "signals")

    def __init__(self):
        self.end = array("q")  # window end, epoch seconds
        self.close = array("d")
        self.ema38 = array("d")
        self.ema100 = array("d")
        self.signals = []  # (index, "BUY" | "SELL")

    def as_json(self, symbol, offset):
        return {"symbol": symbol, "utc_offset": offset, "end": list(self.end), "close": list(self.close),
                "ema38": list(self.ema38), "ema100": list(self.ema100), "signals": self.signals}


class State:
    """Everything the page shows. Mutated by the reader thread under `lock`."""

    def __init__(self):
        self.lock = threading.Lock()
        self.clients = set()  # Client objects
        self.clients_lock = threading.Lock()
        self._ends = {}  # window_end string -> (epoch seconds, utc offset)
        self.reset(None)

    def reset(self, run):
        self.run = run
        self.series = {}
        self.bullish = {}  # symbol -> EMA38 > EMA100 after its latest window
        self.rows = self.buys = self.sells = self.late_rows = 0
        self.latest_end = None  # newest window end seen (event time)
        self.utc_offset = 7200  # seconds; taken from the rows
        self.advisories = deque(maxlen=300)
        self.breadth = []  # [window end, {exchange: [bullish, total]}] whenever the newest window end advances
        self.counts = {ex: [0, 0] for ex in EXCHANGES}

    # -- ingestion ---------------------------------------------------------
    def apply(self, row):
        """Add one window row; return the compact form sent to live clients."""
        symbol = row["symbol"]
        parsed = self._ends.get(row["window_end"])
        if parsed is None:
            if len(self._ends) > 100000:
                self._ends.clear()
            stamp = datetime.fromisoformat(row["window_end"])
            offset = stamp.utcoffset()
            parsed = self._ends[row["window_end"]] = (int(stamp.timestamp()),
                                                     int(offset.total_seconds()) if offset else None)
        end, offset = parsed
        if offset is not None:
            self.utc_offset = offset
        series = self.series.get(symbol)
        if series is None:
            series = self.series[symbol] = Series()
        if series.end and end <= series.end[-1]:
            self.late_rows += 1  # duplicate or out-of-order row (should not happen); keep series sorted
            return None
        series.end.append(end)
        series.close.append(row["close"])
        series.ema38.append(row["ema38"])
        series.ema100.append(row["ema100"])
        advisory = row.get("advisory")
        if advisory:
            series.signals.append((len(series.end) - 1, advisory))
            if advisory == "BUY":
                self.buys += 1
            else:
                self.sells += 1
            self.advisories.append({"symbol": symbol, "end": end, "close": row["close"], "advisory": advisory})
        self.rows += 1

        if self.latest_end is None or end > self.latest_end:
            if self.latest_end is not None:
                # Rows arrive grouped by window end, so the previous window is complete now.
                self.breadth.append([self.latest_end, {ex: list(c) for ex, c in self.counts.items()}])
            self.latest_end = end

        ex = exchange_of(symbol)
        bull = row["ema38"] > row["ema100"]
        before = self.bullish.get(symbol)
        if ex in self.counts:
            if before is None:
                self.counts[ex][1] += 1
            if bull and not before:
                self.counts[ex][0] += 1
            elif before and not bull:
                self.counts[ex][0] -= 1
        self.bullish[symbol] = bull

        return {"s": symbol, "e": end, "c": row["close"], "a": row["ema38"], "b": row["ema100"],
                "x": advisory}

    def status(self):
        return {"run": self.run, "rows": self.rows, "symbols": len(self.series), "buys": self.buys,
                "sells": self.sells, "latest_end": self.latest_end, "utc_offset": self.utc_offset,
                "breadth": {ex: list(c) for ex, c in self.counts.items()}}

    def symbols(self):
        out = []
        for symbol, s in self.series.items():
            buys = sum(1 for _, a in s.signals if a == "BUY")
            out.append({"symbol": symbol, "windows": len(s.end), "buys": buys, "sells": len(s.signals) - buys,
                        "close": s.close[-1], "bullish": self.bullish.get(symbol)})
        return out

    def publish(self, message):
        with self.clients_lock:
            clients = list(self.clients)
        for client in clients:
            client.offer(message)


class Client:
    """One open /events stream; receives its subscribed symbols' rows plus all advisories."""

    def __init__(self, symbols):
        self.symbols = set(symbols)
        self.queue = queue.Queue(maxsize=2000)
        self.dropped = False

    def offer(self, message):
        kind, payload = message
        if kind == "rows":
            rows = [r for r in payload if r["s"] in self.symbols]
            if not rows:
                return
            payload = rows
        try:
            self.queue.put_nowait((kind, payload))
        except queue.Full:
            self.dropped = True  # too slow: the handler closes it and the page reconnects


class Follower(threading.Thread):
    """Tails the files of the newest run in the output directory."""

    def __init__(self, state, directory, load=None):
        super().__init__(daemon=True)
        self.state = state
        self.directory = directory
        self.load = load
        self.files = {}  # path -> [file object, partial line]
        self.current = None  # run name being followed
        self.seen_at_start = set()

    def run_name(self, path):
        m = RUN_FILE.match(os.path.basename(path))
        return m.group(1) if m else None

    def newest_run(self):
        """Run name of the newest output file created after the dashboard started."""
        best = None
        for path in glob.glob(os.path.join(self.directory, "*windows-*.jsonl")):
            if path in self.seen_at_start or self.run_name(path) is None:
                continue
            mtime = os.path.getmtime(path)
            if best is None or mtime > best[0]:
                best = (mtime, self.run_name(path))
        return best[1] if best else None

    def switch(self, run, paths):
        for f, _ in self.files.values():
            f.close()
        self.files = {}
        self.current = run
        with self.state.lock:
            self.state.reset(run)
        self.state.publish(("reset", {"run": run}))
        log(f"following {run}")
        self.open_new(paths)

    def open_new(self, paths):
        for path in paths:
            if path not in self.files and self.run_name(path) == self.current:
                self.files[path] = [open(path, encoding="utf-8"), ""]

    def read_available(self, limit=20000):
        """Read complete new lines from all files of the run; apply them; publish."""
        batch, advisories = [], []
        for entry in self.files.values():
            f, partial = entry
            while len(batch) < limit:
                line = f.readline()
                if not line:
                    break
                if not line.endswith("\n"):  # writer is mid-line; keep for the next poll
                    partial += line
                    continue
                line, partial = partial + line, ""
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if row.get("type") != "window":
                    continue
                try:
                    with self.state.lock:
                        compact = self.state.apply(row)
                except (KeyError, TypeError, ValueError):
                    compact = None  # malformed row: skip
                if compact is None:
                    continue
                batch.append(compact)
                if compact["x"]:
                    advisories.append(compact)
            entry[1] = partial
        if batch:
            self.state.publish(("rows", batch))
        if advisories:
            self.state.publish(("advisories", advisories))
        return len(batch)

    def load_files(self):
        paths = sorted(p for pattern in self.load.split() for p in glob.glob(pattern))
        if not paths:
            log(f"nothing matches {self.load!r}")
            return
        self.switch(self.run_name(paths[0]) or os.path.basename(paths[0]), [])
        self.files = {p: [open(p, encoding="utf-8"), ""] for p in paths}
        began = time.monotonic()
        while self.read_available(limit=200000):
            log(f"loaded {self.state.rows} rows")
        log(f"loaded {self.state.rows} rows of {len(self.state.series)} symbols "
            f"in {time.monotonic() - began:.0f} s")

    def run(self):
        if self.load:
            self.load_files()
            return  # a finished run: nothing more to follow
        self.seen_at_start = set(glob.glob(os.path.join(self.directory, "*windows-*.jsonl")))
        log(f"waiting for a new run in {self.directory}")
        last_status = 0.0
        while True:
            newest = self.newest_run()
            paths = glob.glob(os.path.join(self.directory, "*windows-*.jsonl"))
            if newest and newest != self.current:
                self.switch(newest, paths)
            elif self.current:
                self.open_new(paths)  # e.g. a Kafka worker that opened its file later
            if self.files:
                while self.read_available():
                    pass
            now = time.monotonic()
            if now - last_status > 1.0:
                with self.state.lock:
                    status = self.state.status()
                self.state.publish(("status", status))
                last_status = now
            time.sleep(POLL_SECONDS)


def make_handler(state):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):  # quiet
            pass

        def send_json(self, payload):
            body = json.dumps(payload, separators=(",", ":")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            url = urlparse(self.path)
            query = parse_qs(url.query)
            if url.path in ("/", "/index.html"):
                return self.send_file("index.html", "text/html; charset=utf-8")
            if url.path == "/api/status":
                with state.lock:
                    payload = state.status()
                    payload["advisories"] = list(state.advisories)
                return self.send_json(payload)
            if url.path == "/api/breadth":
                with state.lock:
                    return self.send_json({"utc_offset": state.utc_offset, "points": list(state.breadth)})
            if url.path == "/api/symbols":
                with state.lock:
                    return self.send_json(state.symbols())
            if url.path == "/api/series":
                symbol = query.get("symbol", [""])[0]
                with state.lock:
                    series = state.series.get(symbol)
                    payload = series.as_json(symbol, state.utc_offset) if series else None
                if payload is None:
                    return self.send_error(404, "unknown symbol")
                return self.send_json(payload)
            if url.path == "/events":
                symbols = [s for s in query.get("symbols", [""])[0].split(",") if s]
                return self.stream(symbols)
            self.send_error(404)

        def send_file(self, name, content_type):
            with open(os.path.join(STATIC, name), "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def stream(self, symbols):
            """Server-sent events: rows of the subscribed symbols, all advisories, status every second."""
            client = Client(symbols)
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            with state.clients_lock:
                state.clients.add(client)
            try:
                while not client.dropped:
                    try:
                        kind, payload = client.queue.get(timeout=15)
                    except queue.Empty:
                        self.wfile.write(b": keep-alive\n\n")
                        self.wfile.flush()
                        continue
                    data = json.dumps(payload, separators=(",", ":"))
                    self.wfile.write(f"event: {kind}\ndata: {data}\n\n".encode())
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with state.clients_lock:
                    state.clients.discard(client)
                self.close_connection = True

    return Handler


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output", default=os.environ.get("DASHBOARD_OUTPUT", "output"),
                   help="directory the pipeline writes its window rows to (followed for new runs)")
    p.add_argument("--load", default=os.environ.get("DASHBOARD_LOAD", ""),
                   help="glob(s) of a finished run's window files to show instead of following")
    p.add_argument("--host", default=os.environ.get("DASHBOARD_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("DASHBOARD_PORT", 8050)))
    args = p.parse_args()

    state = State()
    Follower(state, args.output, args.load or None).start()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(state))
    server.daemon_threads = True
    log(f"open http://localhost:{args.port}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
