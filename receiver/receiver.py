#!/usr/bin/env python3
"""Receive one replay stream and emit window/advisory JSON lines.

Run from the repository root: python3 -m receiver.receiver.
Each connection is an independent replay with fresh state. Only one active
connection is accepted; reconnecting does not resume a previous replay.
"""

import json
import os
import socketserver
import sys
import threading
import time

from .processor import StreamProcessor

HOST = os.environ.get("RECEIVER_HOST", "0.0.0.0")
PORT = int(os.environ.get("RECEIVER_PORT", 9999))
PRINT_EVERY = int(os.environ.get("RECEIVER_PRINT_EVERY", 10_000))
LATENESS = float(os.environ.get("RECEIVER_ALLOWED_LATENESS_SECONDS", 0))
OUTPUT = os.environ.get("RECEIVER_OUTPUT", "")
INCLUDE_PARTIAL = os.environ.get("RECEIVER_FLUSH_PARTIAL", "0") == "1"
SUBSCRIPTIONS = os.environ.get("RECEIVER_SYMBOLS", "")


def log(message):
    print(f"[receiver] {message}", file=sys.stderr, flush=True)


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        peer = "%s:%s" % self.client_address
        if not self.server.stream_lock.acquire(blocking=False):
            log(f"rejecting {peer}: another replay is active")
            return
        try:
            self._process_stream(peer)
        finally:
            self.server.stream_lock.release()

    def _process_stream(self, peer):
        processor = StreamProcessor(
            LATENESS,
            set(filter(None, (s.strip() for s in SUBSCRIPTIONS.split(",")))) if SUBSCRIPTIONS else None,
        )
        log(f"connection from {peer}; fresh EMA state")
        begin = time.monotonic()
        run_id = f"{time.time_ns()}-{peer}"
        output = open(OUTPUT, "a", encoding="utf-8") if OUTPUT else sys.stdout
        n = malformed = 0

        def emit(rows):
            for row in rows:
                output.write(json.dumps({"run_id": run_id, **row}, allow_nan=False) + "\n")
            if rows:
                output.flush()  # Advisories become visible at window finalization.

        try:
            for line in self.rfile:
                n += 1
                try:
                    event = json.loads(line)
                except (ValueError, UnicodeDecodeError):
                    malformed += 1
                else:
                    emit(processor.process(event))
                if PRINT_EVERY > 0 and n % PRINT_EVERY == 0:
                    rate = n / max(time.monotonic() - begin, 1e-9)
                    output.flush()
                    log(f"#{n} ({rate:.0f} ev/s) {processor.summary()}; malformed={malformed}")
            emit(processor.finish(include_partial=INCLUDE_PARTIAL))
            output.flush()
            elapsed = time.monotonic() - begin
            log(f"{peer} closed; {json.dumps({'run_id': run_id, 'lines': n, 'malformed': malformed, 'elapsed_seconds': elapsed, 'events_per_second': n / max(elapsed, 1e-9), **processor.summary()})}")
        finally:
            if output is not sys.stdout:
                output.close()


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, *args, **kwargs):
        self.stream_lock = threading.Lock()
        super().__init__(*args, **kwargs)


if __name__ == "__main__":
    StreamProcessor(LATENESS)  # Validate configuration before accepting clients.
    with Server((HOST, PORT), Handler) as server:
        log(f"listening on {HOST}:{PORT}")
        server.serve_forever()
