"""Public states. Levels express backend semantics, not universal RAM cutoffs."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from time import monotonic


class PressureLevel(IntEnum):
    UNKNOWN = -1
    NORMAL = 0
    PRESSURED = 1
    CRITICAL = 2


@dataclass(frozen=True)
class PressureEvent:
    level: PressureLevel
    backend: str
    reason: str
    source: str = "system"
    timestamp: float = field(default_factory=monotonic)


class PressureError(RuntimeError):
    """Base library exception."""


class BackendUnavailable(PressureError):
    """The native facility is absent, inaccessible, or unsupported."""


class MonitorFailed(PressureError):
    """A started monitor failed. Admission is closed, not assumed safe."""


class GovernorClosed(PressureError):
    """The governor was closed or reused after a fork."""


class ContainmentUnavailable(PressureError):
    """Requested whole-tree containment or limits cannot be provided."""
