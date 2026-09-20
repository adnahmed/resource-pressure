"""Custom Dask coordinator that keeps draining while admission is unavailable.

DaskProducer owns admission, pending results and cleanup; the coordinator
only selects inputs and persists results.
"""
from __future__ import annotations

from collections import deque

from distributed import Client, LocalCluster, wait

from resource_pressure import PressureGovernor
from resource_pressure.integrations.dask import DaskProducer


def work(number: int) -> int:
    return sum(i * i for i in range(number))


def persist_result(result: int) -> None:
    print(result)


def main() -> None:
    items = deque(range(1000, 1100))

    with LocalCluster(n_workers=4, threads_per_worker=1) as cluster:
        with Client(cluster) as client, PressureGovernor.auto(
            max_in_flight=8,
            # Optional: spread starts so a high ceiling is not filled instantly.
            min_admission_interval=0.02,
        ) as governor:
            def drain(futures):
                if futures:
                    wait(futures)

            with DaskProducer(client.submit, governor, drain=drain) as pending:
                while items or pending:
                    while items:
                        if pending.try_submit(work, items[0], pure=False) is None:
                            break
                        items.popleft()
                    for future, _ in pending.completed(timeout=0.2):
                        persist_result(future.result())


if __name__ == "__main__":
    main()
