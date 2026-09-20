from __future__ import annotations

from threading import Event
from typing import Callable, Protocol

from ..model import PressureEvent

Emit = Callable[[PressureEvent], None]
Ready = Callable[[], None]


class PressureBackend(Protocol):
    name: str

    def run(self, stop: Event, emit: Emit, ready: Ready) -> None:
        """Initialize, emit initial state, call ready, then monitor until stop.

        Exceptions propagate to the governor. Native resources must be released
        in finally blocks. Never treat a read/notification failure as NORMAL.
        """
        ...
