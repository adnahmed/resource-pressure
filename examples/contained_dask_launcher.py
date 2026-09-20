"""Launch the Dask supervisor INSIDE containment before it creates workers.

Requires the all extra and Job Object/cgroup v2 support. No memory budget is
invented. Add a deployment-specific max_memory to process_group to enforce one.
The child in dask_local.py owns the per-task pressure admission gate.
"""
import sys
from pathlib import Path

from resource_pressure import PressureGovernor


def main() -> None:
    child_script = Path(__file__).with_name("dask_local.py")
    with PressureGovernor.auto(max_in_flight=1) as launch_governor:
        with launch_governor.process_group() as children:
            print("Dask supervisor containment:", children.mechanism)
            result = children.run(sys.executable, [str(child_script)], timeout=120,
                                  output_limit=1_048_576)
            print(result.stdout)
            if result.stderr:
                print(result.stderr, file=sys.stderr)
            if result.code != 0:
                raise SystemExit(result.code or 1)


if __name__ == "__main__":
    main()
