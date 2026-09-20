"""Custom Dask coordinator that keeps draining while admission is unavailable.

The pattern is generic: active Dask futures are kept separate from their
ManagedSubmission objects so public Dask utilities can operate on raw Futures.
"""
from __future__ import annotations

from collections import deque

from distributed import Client, LocalCluster, wait

from resource_pressure import PressureGovernor
from resource_pressure.integrations.dask import DaskAdmission, ManagedSubmission


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
            admitted = DaskAdmission(client, governor)
            active: dict[object, ManagedSubmission] = {}

            try:
                while items or active:
                    # Fill only while admission is immediately available. No
                    # blocking occurs here, so existing results can still drain.
                    while items:
                        submission = admitted.try_submit_managed(work, items[0])
                        if submission is None:
                            break
                        items.popleft()
                        active[submission.future] = submission

                    if not active:
                        # No completion exists to drain, so blocking for exactly
                        # one admission is safe and avoids a producer spin loop.
                        submission = admitted.submit_managed(work, items.popleft())
                        active[submission.future] = submission

                    done, _ = wait(active, return_when="FIRST_COMPLETED")
                    for future in done:
                        submission = active.pop(future)
                        try:
                            persist_result(submission.result())
                        finally:
                            submission.release()
            finally:
                for submission in active.values():
                    submission.cancel()


if __name__ == "__main__":
    main()
