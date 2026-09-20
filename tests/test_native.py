"""Opt-in initialization/shutdown tests; these do not deliberately exhaust RAM."""
import os

import pytest

from resource_pressure import PressureGovernor


@pytest.mark.native
@pytest.mark.skipif(os.environ.get("RESOURCE_PRESSURE_NATIVE") != "1",
                    reason="Set RESOURCE_PRESSURE_NATIVE=1 on a supported native host")
def test_real_native_monitor_initializes_and_closes():
    for _ in range(3):
        with PressureGovernor.auto(max_in_flight=1) as governor:
            governor.check_health()
            assert governor.event.backend == governor.backend.name
        assert not governor._thread.is_alive()
