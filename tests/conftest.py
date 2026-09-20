import time

import pytest

from resource_pressure import PressureGovernor, PressureLevel
from resource_pressure.testing import ManualBackend


@pytest.fixture
def normal_governor():
    backend = ManualBackend()
    with PressureGovernor(backend, max_in_flight=2) as governor:
        yield governor, backend


def wait_for_level(governor, level, timeout=2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if governor.level == level:
            return
        time.sleep(0.002)
    raise AssertionError(f"Expected {level}, got {governor.level}")


def wait_for(predicate, timeout=2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.002)
    raise AssertionError("Condition did not become true")
