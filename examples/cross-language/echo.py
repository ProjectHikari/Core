import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "bridge" / "sdk" / "python"))
from core_bridge import Context, run


async def start(ctx: Context) -> None:
    async def echo(method: str, params: Any) -> Any:
        if method != "echo":
            raise ValueError(f"unknown method {method}")
        return params

    await ctx.provide("demo.echo", echo)


run(start)
