"""Event-time five-minute windows, zero-seeded EMA38/100 and crossovers.

One instance owns one replay stream. Window boundaries use the dataset's
fixed CEST (+02:00) clock. Empty windows retain EMA; they produce no rows.
"""

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import heapq
import math

from .models import Metrics, SymbolState, Window

CEST = timezone(timedelta(hours=2), "CEST")
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
SECOND = 1_000_000
WINDOW = 300 * SECOND
ALPHA38 = 2 / 39
ALPHA100 = 2 / 101


class StreamProcessor:
    def __init__(self, allowed_lateness_seconds=0.0, subscribed_symbols=None):
        if not math.isfinite(allowed_lateness_seconds) or allowed_lateness_seconds < 0:
            raise ValueError("allowed lateness must be finite and non-negative")
        self.lateness = round(allowed_lateness_seconds * SECOND)
        # Subscriptions filter output only: calculate every symbol's state.
        self.subscriptions = None if subscribed_symbols is None else set(subscribed_symbols)
        self.metrics = Metrics()
        self.states = {}
        self.pending = {}
        self.ends = []
        self.max_timestamp = None
        self.watermark = None
        self._date_cache = {}
        self.finished = False

    def _timestamp(self, date, time):
        if not isinstance(date, str) or not isinstance(time, str):
            raise ValueError("missing trading date/time")
        if date not in self._date_cache:
            day = datetime.strptime(date, "%d-%m-%Y").replace(tzinfo=CEST)
            delta = day - EPOCH
            self._date_cache[date] = delta.days * 86400 * SECOND + delta.seconds * SECOND
        # Integer fractional seconds avoid floating-point boundary rounding.
        hms = time.split(":")
        if len(hms) != 3:
            raise ValueError("expected HH:MM:SS[.ffffff]")
        hour, minute = int(hms[0]), int(hms[1])
        sec = hms[2].split(".")
        second = int(sec[0])
        fraction = sec[1] if len(sec) == 2 else ""
        if len(sec) > 2 or not (0 <= hour < 24 and 0 <= minute < 60 and 0 <= second < 60):
            raise ValueError("invalid trading time")
        if len(fraction) > 6 or (len(sec) == 2 and not fraction.isdigit()):
            raise ValueError("invalid fractional seconds")
        microseconds = int(fraction.ljust(6, "0")) if fraction else 0
        return self._date_cache[date] + (hour * 3600 + minute * 60 + second) * SECOND + microseconds

    @staticmethod
    def _iso(timestamp):
        return (EPOCH + timedelta(microseconds=timestamp)).astimezone(CEST).isoformat(timespec="microseconds")

    def process(self, event):
        """Return finalized window rows; skip invalid/non-price/too-late rows."""
        if self.finished:
            raise RuntimeError("cannot process after finish")
        self.metrics.received += 1
        if not isinstance(event, dict):
            self.metrics.invalid += 1
            return []
        try:
            raw_price = event.get("last")
            if raw_price is None or raw_price == "":
                self.metrics.no_price += 1
                return []
            if isinstance(raw_price, bool):
                raise ValueError("boolean price")
            price = float(raw_price)
            if price == 0:
                self.metrics.no_price += 1
                return []
            if not math.isfinite(price) or price < 0:
                raise ValueError("invalid price")
            symbol = event.get("id")
            if not isinstance(symbol, str) or not symbol.strip():
                raise ValueError("missing symbol")
            sec_type = event.get("sec_type")
            if sec_type not in ("E", "I"):
                raise ValueError("unsupported security type")
            trading_date = event.get("trading_date")
            # Observed index rows carry Trading time and a value but no
            # Trading date. Explicitly infer their day from system Date.
            # Keep Trading time; do not replace a supplied invalid trade date
            # or infer a date for equities. This same-day policy is documented.
            index_date_fallback = sec_type == "I" and trading_date in (None, "")
            if index_date_fallback:
                trading_date = event.get("date")
            timestamp = self._timestamp(trading_date, event.get("trading_time"))
        except (ValueError, TypeError, OverflowError):
            self.metrics.invalid += 1
            return []

        start = timestamp // WINDOW * WINDOW
        end = start + WINDOW
        # Late events may update open windows. Finalized output is immutable.
        if self.watermark is not None and end <= self.watermark:
            self.metrics.late += 1
            return []
        self.metrics.accepted += 1
        if index_date_fallback:
            self.metrics.index_date_fallback += 1
        if symbol not in self.states:
            self.states[symbol] = SymbolState()
        key = (start, symbol)
        window = self.pending.get(key)
        if window is None:
            self.pending[key] = Window(price, timestamp)
            heapq.heappush(self.ends, (end, start, symbol))
        else:
            window.event_count += 1
            # Latest event time wins; ties use the last received event.
            if timestamp >= window.last_timestamp:
                window.close = price
                window.last_timestamp = timestamp
        if self.max_timestamp is None or timestamp > self.max_timestamp:
            self.max_timestamp = timestamp
        self.watermark = self.max_timestamp - self.lateness
        return self._close_until(self.watermark)

    def _close_until(self, cutoff, partial=False):
        rows = []
        while self.ends and self.ends[0][0] <= cutoff:
            end, start, symbol = heapq.heappop(self.ends)
            window = self.pending.pop((start, symbol))
            state = self.states[symbol]
            previous38, previous100 = state.ema38, state.ema100
            state.ema38 = ALPHA38 * window.close + (1 - ALPHA38) * previous38
            state.ema100 = ALPHA100 * window.close + (1 - ALPHA100) * previous100
            advisory = None
            if state.ema38 > state.ema100 and previous38 <= previous100:
                advisory = "BUY"
                self.metrics.buy += 1
            elif state.ema38 < state.ema100 and previous38 >= previous100:
                advisory = "SELL"
                self.metrics.sell += 1
            self.metrics.windows += 1
            if self.subscriptions is None or symbol in self.subscriptions:
                rows.append({
                    "type": "window", "symbol": symbol,
                    "window_start": self._iso(start), "window_end": self._iso(end),
                    "close": window.close, "event_count": window.event_count,
                    "ema38": state.ema38, "ema100": state.ema100,
                    "previous_ema38": previous38, "previous_ema100": previous100,
                    "advisory": advisory, "partial": partial,
                })
        return rows

    def finish(self, include_partial=False):
        """Release complete windows held for lateness at end-of-input.

        The final incomplete window is withheld by default. Opt-in preview
        rows are marked partial and must be excluded from correctness checks.
        """
        if self.finished:
            return []
        self.finished = True
        if self.max_timestamp is None:
            return []
        rows = self._close_until(self.max_timestamp)
        if include_partial and self.ends:
            rows.extend(self._close_until(max(end for end, _, _ in self.ends), partial=True))
        return rows

    def summary(self):
        return {**asdict(self.metrics), "symbols": len(self.states), "pending_windows": len(self.pending)}
