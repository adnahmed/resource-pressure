"""Real public-API compatibility test, skipped when distributed is absent."""
import pytest

distributed = pytest.importorskip("distributed")
from resource_pressure import PressureGovernor
from resource_pressure.integrations.dask import DaskAdmission
from resource_pressure.testing import ManualBackend


@pytest.mark.integration
def test_real_local_dask_stream():
    with distributed.LocalCluster(n_workers=2, threads_per_worker=1, processes=False,
                                  dashboard_address=None) as cluster:
        with distributed.Client(cluster) as client:
            with PressureGovernor(ManualBackend(), max_in_flight=2) as governor:
                values = DaskAdmission(client, governor).map_unordered(abs, range(-10, 0))
                assert sorted(values) == list(range(1, 11))
                assert governor.in_flight == 0
