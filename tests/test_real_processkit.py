"""Real dependency API test; native hard-limit testing is a separate deployment task."""
import sys
import pytest

pytest.importorskip("processkit")
from resource_pressure import PressureGovernor
from resource_pressure.testing import ManualBackend


@pytest.mark.integration
def test_real_contained_command():
    with PressureGovernor(ManualBackend(), max_in_flight=1) as governor:
        # This smoke test permits POSIX because many CI machines lack delegation;
        # the actual mechanism remains visible and no hard-limit claim is made.
        with governor.process_group(allow_process_group_fallback=True) as children:
            result = children.run(sys.executable, ["-c", "print('native-child-ok')"], timeout=10)
            assert result.code == 0
            assert "native-child-ok" in result.stdout
