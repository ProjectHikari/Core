import asyncio
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk" / "python"))
from core_bridge import Context, run


async def start(ctx: Context) -> None:
    name = os.getenv("SERVICE_NAME", "test.remote")
    host = os.getenv("HOST_SERVICE", "test.host")
    file = os.getenv("RESOURCE_FILE")
    if file:
        resource = Path(file)
        resource.write_text(str(os.getpid()))
        attempt = 0

        async def cleanup() -> None:
            nonlocal attempt
            attempt += 1
            if os.getenv("CLEANUP_ATTEMPTS"):
                Path(os.environ["CLEANUP_ATTEMPTS"]).write_text(str(attempt))
            if os.getenv("SLOW_CLEANUP") == "1":
                try:
                    await asyncio.sleep(0.15)
                except asyncio.CancelledError:
                    await asyncio.sleep(0.15)
            if os.getenv("FAIL_CLEANUP") == "1" and attempt == 1:
                raise RuntimeError("cleanup failed once")
            resource.unlink(missing_ok=True)

        await ctx.defer("file", cleanup)
    start_value = await ctx.call(host, "init")
    seen: list[Any] = []
    ticks = 0
    late_ready = False

    async def tick() -> None:
        nonlocal ticks
        ticks += 1

    await ctx.on("test.changed", seen.append)
    await ctx.on("test.parallel", lambda _: asyncio.sleep(0.02))
    await ctx.after("tick", 0, tick)

    async def service(method: str, params: Any) -> Any:
        nonlocal late_ready
        if method == "echo":
            return params
        if method == "init":
            return start_value + 1
        if method == "info":
            return {"generation": ctx.generation, "pid": os.getpid(), "startValue": start_value, "seen": seen, "ticks": ticks, "lateReady": late_ready}
        if method == "late":
            async def acquire_late() -> None:
                nonlocal late_ready
                late_ready = True
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    file = Path(os.environ["LATE_FILE"])
                    file.write_text("late")
                    await ctx.defer("late file", lambda: file.unlink(missing_ok=True))
            ctx.go("late resource", acquire_late)
            return None
        if method == "emit":
            await ctx.emit("test.changed", params)
            return None
        if method == "parallel":
            await ctx.parallel("test.parallel", params)
            return None
        if method == "wrongMode":
            await ctx.parallel("test.changed", params)
            return None
        if method == "undeclared":
            return await ctx.lookup("not.declared")
        if method == "hostSlow":
            return await ctx.call(host, "slow")
        if method == "fail":
            raise RuntimeError("service error")
        if method == "crash":
            os._exit(3)
        if method == "fault":
            async def fail() -> None:
                await asyncio.sleep(0.01)
                raise RuntimeError("task failed")
            ctx.go("broken task", fail)
            return None
        if method == "cancelError":
            async def fail_cancel() -> None:
                await asyncio.sleep(0.01)
                raise asyncio.CancelledError()
            ctx.go("uncancelled task", fail_cancel)
            return None
        if method in ("slow", "ignore"):
            await asyncio.sleep(10)
            return None
        raise ValueError(f"unknown method {method}")

    await ctx.provide(name, service)
    if os.getenv("FAIL_START") == "1":
        raise RuntimeError("startup failed")
    if os.getenv("HANG_START") == "1":
        await asyncio.sleep(10)


run(start)
