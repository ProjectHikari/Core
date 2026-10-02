from __future__ import annotations

import asyncio
import inspect
import json
import sys
from collections.abc import Awaitable, Callable
from typing import Any

Handler = Callable[[Any], Any | Awaitable[Any]]
ServiceHandler = Callable[[str, Any], Any | Awaitable[Any]]
Cleanup = Callable[[], None | Awaitable[None]]


class RemoteError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


async def _invoke(fn: Callable[..., Any], *args: Any) -> Any:
    value = fn(*args)
    return await value if inspect.isawaitable(value) else value


class _Peer:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self.pending: dict[str, asyncio.Future[Any]] = {}
        self.active: dict[str, asyncio.Task[None]] = {}
        self.context: Context | None = None
        self.next = 0
        self.max_bytes = 1 << 20
        self.max_inflight = 128
        self.closed = False
        self.reader_task: asyncio.Task[None] | None = None
        self.handler: Callable[[str, Any, str], Awaitable[Any]] | None = None

    async def send(self, frame: dict[str, Any]) -> None:
        if self.closed:
            raise RuntimeError("bridge closed")
        data = json.dumps(frame, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
        if len(data) > self.max_bytes:
            raise ValueError("bridge message too large")
        self.writer.write(data + b"\n")
        await self.writer.drain()

    async def call(self, method: str, params: Any = None) -> Any:
        if self.closed:
            raise RuntimeError("bridge closed")
        if len(self.pending) >= self.max_inflight:
            raise RuntimeError("bridge inflight limit reached")
        self.next += 1
        request_id = f"c{self.next}"
        future = asyncio.get_running_loop().create_future()
        # 取消后仍可能收到错误响应，取出异常避免无人等待的Future告警
        future.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
        self.pending[request_id] = future
        try:
            await self.send({"type": "request", "id": request_id, "method": method, "params": params})
        except asyncio.CancelledError:
            try:
                await self.send({"type": "cancel", "id": request_id})
            except Exception as error:
                self.close(error)
            raise
        except Exception:
            self.pending.pop(request_id, None)
            future.cancel()
            raise
        try:
            return await asyncio.wait_for(asyncio.shield(future), 30)
        except (asyncio.CancelledError, TimeoutError):
            # 保留槽位直到真实响应，不能把任务取消当作对端释放完成
            try:
                await self.send({"type": "cancel", "id": request_id})
            except Exception as error:
                self.close(error)
            raise

    def close(self, error: Exception) -> None:
        if self.closed:
            return
        self.closed = True
        if self.reader_task and self.reader_task is not asyncio.current_task():
            self.reader_task.cancel()
        if self.context:
            self.context.abort()
        for future in self.pending.values():
            if not future.done():
                future.set_exception(error)
        self.pending.clear()
        for task in self.active.values():
            if task is not asyncio.current_task():
                task.cancel()
        self.writer.close()

    async def respond(self, frame: dict[str, Any]) -> None:
        try:
            await self.send(frame)
        except (ValueError, TypeError) as error:
            await self.send({"type": "response", "id": frame["id"], "error": {"code": "serialization", "message": str(error)}})

    async def request(self, frame: dict[str, Any]) -> None:
        request_id = frame["id"]
        method = frame.get("method", "")
        try:
            try:
                if self.handler is None:
                    raise RuntimeError("missing handler")
                value = await self.handler(method, frame.get("params"), request_id)
                response = {"type": "response", "id": request_id, "result": value}
            except asyncio.CancelledError:
                response = {"type": "response", "id": request_id, "error": {"code": "cancelled", "message": "call cancelled"}}
            except Exception as error:
                response = {"type": "response", "id": request_id, "error": {"code": "error", "message": str(error)}}
            await self.respond(response)
        except Exception as error:
            self.close(error)
        finally:
            self.active.pop(request_id, None)

    async def read(self) -> None:
        while not self.closed:
            line = await self.reader.readline()
            if not line:
                return
            if len(line.rstrip(b"\n")) > self.max_bytes:
                raise ValueError("bridge message too large")
            frame = json.loads(line, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
            request_id = frame.get("id")
            if not isinstance(request_id, str) or not request_id or len(request_id) > 128:
                raise ValueError("invalid request id")
            kind = frame.get("type")
            if kind == "response":
                future = self.pending.pop(request_id, None)
                if future and not future.done():
                    error = frame.get("error")
                    if error:
                        future.set_exception(RemoteError(error["code"], error["message"]))
                    else:
                        future.set_result(frame.get("result"))
            elif kind == "cancel":
                task = self.active.get(request_id)
                if task:
                    # 先让request进入try/finally，避免尚未运行就取消而丢失响应
                    asyncio.get_running_loop().call_soon(task.cancel)
            elif kind == "scope_cancel":
                if self.context:
                    self.context.abort()
            elif kind == "request":
                if request_id in self.active:
                    raise ValueError("duplicate request id")
                if len(self.active) >= self.max_inflight:
                    await self.respond({"type": "response", "id": request_id, "error": {"code": "busy", "message": "bridge inflight limit reached"}})
                else:
                    self.active[request_id] = asyncio.create_task(self.request(frame))
            else:
                raise ValueError("invalid message type")


class Context:
    def __init__(self, peer: _Peer, info: dict[str, Any]) -> None:
        self.plugin_id: str = info["plugin_id"]
        self.generation: int = info["generation"]
        self.closed = False
        self._peer = peer
        self._callbacks: dict[str, Callable[..., Any]] = {}
        self._disposed: set[str] = set()
        self._disposing: set[str] = set()
        self._services: dict[str, ServiceHandler] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._next = 0

    def _id(self) -> str:
        self._next += 1
        return f"f{self._next}"

    def abort(self) -> None:
        self.closed = True
        for task in self._tasks:
            task.cancel()

    async def _call(self, method: str, params: Any) -> Any:
        if self.closed:
            raise RuntimeError("context closed")
        return await self._peer.call(method, params)

    async def provide(self, name: str, handler: ServiceHandler) -> None:
        if name in self._services:
            raise ValueError(f"service {name} already provided")
        self._services[name] = handler
        try:
            await self._call("provide", {"name": name})
        except RemoteError:
            self._services.pop(name, None)
            raise

    async def lookup(self, name: str) -> bool:
        return await self._call("lookup", {"name": name})

    async def call(self, name: str, method: str, params: Any = None) -> Any:
        return await self._call("call", {"name": name, "method": method, "params": params})

    async def emit(self, name: str, payload: Any) -> None:
        await self._call("emit", {"name": name, "payload": payload})

    async def parallel(self, name: str, payload: Any) -> None:
        await self._call("parallel", {"name": name, "payload": payload})

    async def _control(self, method: str, params: dict[str, Any], handler: Handler, once: bool = False) -> Callable[[], Awaitable[None]]:
        callback = self._id()

        async def invoke(payload: Any) -> Any:
            try:
                return await _invoke(handler, payload)
            finally:
                if once:
                    self._callbacks.pop(callback, None)

        self._callbacks[callback] = invoke
        try:
            await self._call(method, {**params, "callback": callback})
        except RemoteError:
            self._callbacks.pop(callback, None)
            raise
        cancelled = False

        async def cancel() -> None:
            nonlocal cancelled
            if not cancelled:
                await self._peer.call("cancel", {"callback": callback})
                cancelled = True
                self._callbacks.pop(callback, None)

        return cancel

    async def on(self, name: str, handler: Handler) -> Callable[[], Awaitable[None]]:
        return await self._control("on", {"name": name}, handler)

    async def after(self, label: str, delay_ms: float, handler: Cleanup) -> Callable[[], Awaitable[None]]:
        return await self._control("after", {"label": label, "delay_ms": delay_ms}, lambda _: _invoke(handler), True)

    async def every(self, label: str, interval_ms: float, handler: Cleanup) -> Callable[[], Awaitable[None]]:
        return await self._control("every", {"label": label, "delay_ms": interval_ms}, lambda _: _invoke(handler))

    async def defer(self, label: str, dispose: Cleanup) -> None:
        callback = self._id()
        self._callbacks[callback] = dispose
        try:
            await self._peer.call("defer", {"label": label, "callback": callback})
        except BaseException:
            await _invoke(dispose)
            self._disposed.add(callback)
            self._callbacks.pop(callback, None)
            raise

    def go(self, label: str, task: Cleanup) -> Callable[[], None]:
        if self.closed:
            raise RuntimeError("context closed")

        async def invoke() -> None:
            try:
                await _invoke(task)
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current and current.cancelling() == 0:
                    try:
                        await self._call("fault", {"label": label, "message": "task cancelled without a cancellation request"})
                    except Exception:
                        pass
            except Exception as error:
                try:
                    await self._call("fault", {"label": label, "message": str(error)})
                except Exception:
                    pass

        work = asyncio.create_task(invoke())
        self._tasks.add(work)
        work.add_done_callback(self._tasks.discard)
        return work.cancel

    async def dispatch(self, method: str, params: dict[str, Any]) -> Any:
        if method == "service":
            handler = self._services.get(params["name"])
            if not handler:
                raise ValueError(f"unknown service {params['name']}")
            return await _invoke(handler, params["method"], params.get("params"))
        callback = params["callback"]
        if method == "dispose" and callback in self._disposed:
            return None
        handler = self._callbacks.get(callback)
        if not handler:
            raise ValueError(f"unknown callback {callback}")
        if method == "dispose":
            if callback in self._disposing:
                raise RuntimeError("disposer already running")
            self._disposing.add(callback)
            try:
                await _invoke(handler)
                self._disposed.add(callback)
                self._callbacks.pop(callback, None)
            finally:
                self._disposing.discard(callback)
            return None
        if method == "callback":
            return await _invoke(handler, params.get("payload"))
        raise ValueError(f"unknown method {method}")

    async def stop(self) -> None:
        self.abort()
        await asyncio.gather(*self._tasks, return_exceptions=True)


async def _run(start: Callable[[Context], None | Awaitable[None]]) -> None:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=(16 << 20) + 1)
    input_transport, _ = await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin.buffer)
    output = sys.stdout.buffer
    sys.stdout = sys.stderr
    transport, protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, output)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    peer = _Peer(reader, writer)

    async def handle(method: str, params: Any, request_id: str) -> Any:
        if method == "start":
            if peer.context or params.get("protocol") != 1:
                raise ValueError("invalid startup handshake")
            peer.max_bytes, peer.max_inflight = params["max_message_bytes"], params["max_inflight"]
            peer.context = Context(peer, params)
            try:
                await _invoke(start, peer.context)
            except asyncio.CancelledError:
                peer.context.abort()
                raise
            return None
        if not peer.context:
            raise RuntimeError("plugin not initialized")
        if method == "shutdown":
            await peer.context.stop()
            others = [task for key, task in peer.active.items() if key != request_id]
            for task in others:
                task.cancel()
            await asyncio.gather(*others, return_exceptions=True)
            return None
        return await peer.context.dispatch(method, params)

    peer.handler = handle
    try:
        peer.reader_task = asyncio.create_task(peer.read())
        try:
            await peer.reader_task
        except asyncio.CancelledError:
            if not peer.closed:
                raise
    finally:
        peer.close(RuntimeError("host disconnected"))
        input_transport.close()


def run(start: Callable[[Context], None | Awaitable[None]]) -> None:
    asyncio.run(_run(start))
