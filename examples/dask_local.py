"""Local producer-side admission; Dask's existing memory safeguards stay enabled."""
from contextlib import closing

from distributed import Client, LocalCluster
from resource_pressure import PressureGovernor
from resource_pressure.integrations.dask import DaskAdmission


def work(number: int) -> int:
    return sum(i * i for i in range(number))


def main() -> None:
    # These workers are NOT placed in a containment group by this example.
    with LocalCluster(n_workers=4, threads_per_worker=1) as cluster:
        with Client(cluster) as client, PressureGovernor.auto(max_in_flight=4) as governor:
            admitted = DaskAdmission(client, governor)
            with closing(admitted.map_unordered(work, range(1000, 1100))) as results:
                for result in results:
                    print(result)  # Consume/persist promptly; avoid accumulating large results.


if __name__ == "__main__":
    main()  # Required for spawn-based Windows/macOS multiprocessing.
