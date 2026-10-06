"""Real TCP ingestion plus the CSV replayer; no Docker required."""
import csv
import json
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

from receiver import receiver
from replayer.replay import FIELDS, TcpSink, parse_args, replay


class PipelineTests(unittest.TestCase):
    def test_csv_tcp_windows_and_malformed_json(self):
        done = threading.Event()
        messages = []

        def capture(message):
            messages.append(message)
            if " closed; " in message:
                done.set()

        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "fixture.csv"
            output = Path(tmp) / "windows.jsonl"
            with csv_path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(FIELDS)
                for price, timestamp in [(100, "08:00:00.000"), (110, "08:04:59.999"),
                                         (200, "08:05:00.000"), (0, "23:59:00.000")]:
                    writer.writerow(["SIE.ETR", "E", "08-11-2021", timestamp,
                                     price, timestamp, "08-11-2021"])
            with patch.object(receiver, "OUTPUT", str(output)), patch.object(receiver, "LATENESS", 0), patch.object(receiver, "INCLUDE_PARTIAL", False), patch.object(receiver, "SUBSCRIPTIONS", ""), patch.object(receiver, "PRINT_EVERY", 0), patch.object(receiver, "log", capture):
                with receiver.Server(("127.0.0.1", 0), receiver.Handler) as server:
                    thread = threading.Thread(target=server.serve_forever, daemon=True)
                    thread.start()
                    host, port = server.server_address
                    try:
                        # A malformed line must not kill the subsequent replay.
                        sink = TcpSink(host, port)
                        sink.send("not json")
                        args = parse_args([str(csv_path), "--speed", "0", "--progress", "0"])
                        emitted, skipped = replay(args, sink)
                        sink.close()
                        self.assertTrue(done.wait(5), messages)
                    finally:
                        server.shutdown()
                        thread.join(5)
            rows = [json.loads(line) for line in output.read_text().splitlines()]
            self.assertEqual((emitted, skipped), (4, 0))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["close"], 110)
            self.assertEqual(rows[0]["advisory"], "BUY")
            self.assertFalse(rows[0]["partial"])
            summary = json.loads(next(m.split(" closed; ", 1)[1] for m in messages if " closed; " in m))
            self.assertEqual(summary["malformed"], 1)
            self.assertEqual(summary["no_price"], 1)
            self.assertEqual(summary["pending_windows"], 1)
            self.assertEqual(summary["lines"], 5)


if __name__ == "__main__":
    unittest.main()
