"""Native sensor plus explicit Job Object/cgroup containment; extra required."""
import sys

from resource_pressure import PressureGovernor


def main() -> None:
    with PressureGovernor.auto(max_in_flight=2) as governor:
        # Optional fixed deployment budget, not calculated from machine RAM.
        # Omit max_memory for lifetime containment without a hard memory cap.
        with governor.process_group(max_memory=512 * 1024 * 1024) as children:
            print("mechanism:", children.mechanism)
            result = children.run(sys.executable, ["-c", "print('contained child')"], timeout=20)
            print(result.stdout)
            if result.code != 0:
                raise SystemExit(result.code or 1)


if __name__ == "__main__":
    main()
