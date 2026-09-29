#!/usr/bin/env python3
"""Minimal receiver: accepts JSON-lines event streams over TCP and logs them.

Placeholder for the real processing service. Prints every Nth event and a
per-connection summary.

Config via environment: RECEIVER_HOST (default 0.0.0.0), RECEIVER_PORT
(default 9999), RECEIVER_PRINT_EVERY (default 10000).
"""

import json
import os
import socketserver
import time

HOST = os.environ.get("RECEIVER_HOST", "0.0.0.0")
PORT = int(os.environ.get("RECEIVER_PORT", 9999))
PRINT_EVERY = int(os.environ.get("RECEIVER_PRINT_EVERY", 10_000))


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        peer = "%s:%s" % self.client_address
        print(f"[receiver] connection from {peer}", flush=True)
        n = 0
        begin = time.monotonic()
        for line in self.rfile:
            ev = json.loads(line)
            n += 1
            if n % PRINT_EVERY == 0:
                rate = n / max(time.monotonic() - begin, 1e-9)
                print(f"[receiver] #{n} ({rate:.0f} ev/s) {ev}", flush=True)
        print(f"[receiver] {peer} closed, {n} events received", flush=True)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    with Server((HOST, PORT), Handler) as server:
        print(f"[receiver] listening on {HOST}:{PORT}", flush=True)
        server.serve_forever()
