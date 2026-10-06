"""Small state records; timestamps are integer microseconds since the epoch."""

from dataclasses import dataclass


@dataclass(slots=True)
class Window:
    close: float
    last_timestamp: int
    event_count: int = 1


@dataclass(slots=True)
class SymbolState:
    ema38: float = 0.0
    ema100: float = 0.0


@dataclass(slots=True)
class Metrics:
    received: int = 0
    accepted: int = 0
    invalid: int = 0
    no_price: int = 0
    late: int = 0
    windows: int = 0
    buy: int = 0
    sell: int = 0
