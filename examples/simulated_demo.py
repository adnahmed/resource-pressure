"""Runnable everywhere; deliberately injected events, not a native smoke test."""
import asyncio

from resource_pressure import PressureGovernor, PressureLevel
from resource_pressure.testing import ManualBackend


async def main() -> None:
    backend = ManualBackend()
    async with PressureGovernor(backend, max_in_flight=2) as governor:
        governor.subscribe(lambda event: print("pressure:", event.level.name))

        async def task(number: int) -> None:
            async with governor.slot():
                print("started", number)
                await asyncio.sleep(0.12)
                print("finished", number)

        async def signals() -> None:
            await asyncio.sleep(0.04)
            backend.set_level(PressureLevel.PRESSURED)
            await asyncio.sleep(0.18)
            backend.set_level(PressureLevel.CRITICAL)
            await asyncio.sleep(0.12)
            backend.set_level(PressureLevel.NORMAL)

        await asyncio.gather(signals(), *(task(i) for i in range(8)))


if __name__ == "__main__":
    asyncio.run(main())
