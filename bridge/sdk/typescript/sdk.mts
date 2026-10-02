import { stderr, stdin, stdout } from 'node:process'

export type JSONValue = null | boolean | number | string | JSONValue[] | { [key: string]: JSONValue }
type Handler = (payload: JSONValue, signal: AbortSignal) => JSONValue | void | Promise<JSONValue | void>
type ServiceHandler = (method: string, params: JSONValue, signal: AbortSignal) => JSONValue | Promise<JSONValue>
type Cleanup = (signal: AbortSignal) => void | Promise<void>
type Frame = { type: string; id: string; method?: string; params?: any; result?: JSONValue; error?: { code: string; message: string } }
type Pending = { resolve: (value: any) => void; reject: (error: Error) => void; detach: () => void }

export class RemoteError extends Error {
  code: string
  constructor(code: string, message: string) { super(message); this.code = code }
}

class Peer {
  pending = new Map<string, Pending>()
  active = new Map<string, { controller: AbortController; job: Promise<void> }>()
  next = 0
  maxBytes = 1 << 20
  maxInflight = 128
  closed = false
  context?: Context
  handler: (method: string, params: any, signal: AbortSignal, id: string) => Promise<any>

  constructor(handler: Peer['handler']) { this.handler = handler }

  async send(frame: Frame): Promise<void> {
    if (this.closed) throw new Error('bridge closed')
    const line = JSON.stringify(frame)
    if (Buffer.byteLength(line) > this.maxBytes) throw new Error('bridge message too large')
    await new Promise<void>((resolve, reject) => stdout.write(line + '\n', error => error ? reject(error) : resolve()))
  }

  call(method: string, params: any, signal?: AbortSignal): Promise<any> {
    if (this.closed || signal?.aborted) return Promise.reject(new Error('bridge call cancelled'))
    if (this.pending.size >= this.maxInflight) return Promise.reject(new Error('bridge inflight limit reached'))
    const id = `c${++this.next}`
    return new Promise((resolve, reject) => {
      const cancel = () => {
        reject(new Error('bridge call cancelled'))
        // 保留槽位直到响应，取消不等于对端执行已结束
        this.send({ type: 'cancel', id }).catch(error => this.close(error))
      }
      const timer = setTimeout(cancel, 30_000)
      const detach = () => { clearTimeout(timer); signal?.removeEventListener('abort', cancel) }
      this.pending.set(id, { resolve, reject, detach })
      signal?.addEventListener('abort', cancel, { once: true })
      this.send({ type: 'request', id, method, params }).catch(error => {
        this.pending.delete(id); detach(); reject(error)
      })
    })
  }

  close(error: Error): void {
    if (this.closed) return
    this.closed = true
    this.context?.abort()
    for (const pending of this.pending.values()) { pending.detach(); pending.reject(error) }
    this.pending.clear()
    for (const entry of this.active.values()) entry.controller.abort()
    stdin.destroy()
  }

  async read(): Promise<void> {
    let buffer = Buffer.alloc(0)
    for await (const chunk of stdin) {
      buffer = Buffer.concat([buffer, Buffer.from(chunk)])
      for (;;) {
        const end = buffer.indexOf(10)
        if (end < 0) break
        if (end > this.maxBytes) throw new Error('bridge message too large')
        const frame: Frame = JSON.parse(buffer.subarray(0, end).toString('utf8'))
        buffer = buffer.subarray(end + 1)
        if (typeof frame.id !== 'string' || !frame.id || frame.id.length > 128) throw new Error('invalid request id')
        if (frame.type === 'response') {
          const pending = this.pending.get(frame.id)
          if (pending) {
            this.pending.delete(frame.id); pending.detach()
            frame.error ? pending.reject(new RemoteError(frame.error.code, frame.error.message)) : pending.resolve(frame.result)
          }
        } else if (frame.type === 'cancel') {
          this.active.get(frame.id)?.controller.abort()
        } else if (frame.type === 'scope_cancel') {
          this.context?.abort()
        } else if (frame.type === 'request') {
          if (this.active.has(frame.id)) throw new Error('duplicate request id')
          if (this.active.size >= this.maxInflight) {
            await this.send({ type: 'response', id: frame.id, error: { code: 'busy', message: 'bridge inflight limit reached' } })
            continue
          }
          const controller = new AbortController()
          const job = Promise.resolve().then(async () => {
            let reply: Frame
            try {
              if (controller.signal.aborted) throw new DOMException('call cancelled', 'AbortError')
              const result = await this.handler(frame.method ?? '', frame.params, controller.signal, frame.id)
              reply = { type: 'response', id: frame.id, result: result ?? null }
            } catch (error) {
              reply = { type: 'response', id: frame.id, error: { code: 'error', message: String(error) } }
            }
            try { await this.send(reply) } catch (error) {
              await this.send({ type: 'response', id: frame.id, error: { code: 'serialization', message: String(error) } })
            }
          }).catch(error => this.close(error)).finally(() => this.active.delete(frame.id))
          this.active.set(frame.id, { controller, job })
        } else throw new Error('invalid message type')
      }
      if (buffer.length > this.maxBytes) throw new Error('bridge message too large')
    }
    if (buffer.length) throw new Error('incomplete bridge message')
  }
}

export class Context {
  pluginId: string
  generation: number
  signal: AbortSignal
  private lifetime = new AbortController()
  private peer: Peer
  private callbacks = new Map<string, Handler | Cleanup>()
  private disposed = new Set<string>()
  private disposing = new Set<string>()
  private services = new Map<string, ServiceHandler>()
  private tasks = new Map<string, { controller: AbortController; job: Promise<void> }>()
  private next = 0

  constructor(peer: Peer, info: any) {
    this.peer = peer; this.pluginId = info.plugin_id; this.generation = info.generation; this.signal = this.lifetime.signal
  }

  abort(): void { this.lifetime.abort(); for (const entry of this.tasks.values()) entry.controller.abort() }
  private id(): string { return `f${++this.next}` }

  async provide(name: string, handler: ServiceHandler): Promise<void> {
    if (this.services.has(name)) throw new Error(`service ${name} already provided`)
    this.services.set(name, handler)
    try { await this.peer.call('provide', { name }, this.signal) } catch (error) {
      if (error instanceof RemoteError) this.services.delete(name)
      throw error
    }
  }

  async lookup(name: string): Promise<boolean> { return this.peer.call('lookup', { name }, this.signal) }
  async call(name: string, method: string, params: JSONValue = null, signal = this.signal): Promise<JSONValue> {
    return this.peer.call('call', { name, method, params }, signal)
  }
  async emit(name: string, payload: JSONValue, signal = this.signal): Promise<void> { await this.peer.call('emit', { name, payload }, signal) }
  async parallel(name: string, payload: JSONValue, signal = this.signal): Promise<void> { await this.peer.call('parallel', { name, payload }, signal) }

  private async control(method: string, params: any, handler: Handler, once = false): Promise<() => Promise<void>> {
    const callback = this.id()
    this.callbacks.set(callback, async (payload, signal) => {
      try { return await handler(payload, signal) } finally { if (once) this.callbacks.delete(callback) }
    })
    try { await this.peer.call(method, { ...params, callback }, this.signal) } catch (error) {
      // 超时后登记可能已经完成，回调保留到本代回收
      if (error instanceof RemoteError) this.callbacks.delete(callback)
      throw error
    }
    let cancelled = false
    return async () => {
      if (cancelled) return
      await this.peer.call('cancel', { callback })
      cancelled = true; this.callbacks.delete(callback)
    }
  }

  on(name: string, handler: Handler): Promise<() => Promise<void>> { return this.control('on', { name }, handler) }
  after(label: string, delayMS: number, handler: Cleanup): Promise<() => Promise<void>> {
    return this.control('after', { label, delay_ms: delayMS }, (_, signal) => handler(signal), true)
  }
  every(label: string, intervalMS: number, handler: Cleanup): Promise<() => Promise<void>> {
    return this.control('every', { label, delay_ms: intervalMS }, (_, signal) => handler(signal))
  }

  async defer(label: string, dispose: Cleanup): Promise<void> {
    const callback = this.id()
    this.callbacks.set(callback, dispose)
    try { await this.peer.call('defer', { label, callback }) } catch (error) {
      // 登记被拒绝时，刚取得的资源仍要由调用方释放
      await dispose(new AbortController().signal)
      this.disposed.add(callback)
      this.callbacks.delete(callback); throw error
    }
  }

  go(label: string, task: Cleanup): () => void {
    if (this.signal.aborted) throw new Error('context closed')
    const id = this.id(), controller = new AbortController()
    const job = Promise.resolve().then(() => task(controller.signal)).catch(async error => {
      if (!(controller.signal.aborted && error instanceof Error && error.name === 'AbortError')) {
        await this.peer.call('fault', { label, message: String(error) }, this.signal).catch(() => {})
      }
    }).finally(() => this.tasks.delete(id))
    this.tasks.set(id, { controller, job })
    return () => controller.abort()
  }

  async dispatch(method: string, params: any, signal: AbortSignal): Promise<any> {
    if (method === 'service') {
      const handler = this.services.get(params.name)
      if (!handler) throw new Error(`unknown service ${params.name}`)
      return handler(params.method, params.params, signal)
    }
    if (method === 'dispose' && this.disposed.has(params.callback)) return null
    const handler = this.callbacks.get(params.callback)
    if (!handler) throw new Error(`unknown callback ${params.callback}`)
    if (method === 'dispose') {
      if (this.disposing.has(params.callback)) throw new Error('disposer already running')
      this.disposing.add(params.callback)
      try {
        await (handler as Cleanup)(signal)
        this.disposed.add(params.callback)
        this.callbacks.delete(params.callback)
      } finally { this.disposing.delete(params.callback) }
      return null
    }
    if (method === 'callback') return (handler as Handler)(params.payload, signal)
    throw new Error(`unknown method ${method}`)
  }

  async stop(): Promise<void> { this.abort(); await Promise.all([...this.tasks.values()].map(entry => entry.job)) }
}

export async function run(start: (ctx: Context) => void | Promise<void>): Promise<void> {
  // stdout只承载协议，日常日志写到stderr
  console.log = console.info = console.debug = (...args: unknown[]) => console.error(...args)
  const peer = new Peer(async (method, params, signal, id) => {
    if (method === 'start') {
      if (peer.context || params.protocol !== 1) throw new Error('invalid startup handshake')
      peer.maxBytes = params.max_message_bytes; peer.maxInflight = params.max_inflight
      const ctx = new Context(peer, params); peer.context = ctx
      signal.addEventListener('abort', () => ctx.abort(), { once: true })
      await start(ctx)
      return null
    }
    if (!peer.context) throw new Error('plugin not initialized')
    if (method === 'shutdown') {
      await peer.context.stop()
      const others = [...peer.active.entries()].filter(([key]) => key !== id)
      for (const [, entry] of others) entry.controller.abort()
      await Promise.all(others.map(([, entry]) => entry.job))
      return null
    }
    return peer.context.dispatch(method, params, signal)
  })
  try { await peer.read() } catch (error) {
    if (!peer.closed) { stderr.write(String(error) + '\n'); process.exitCode = 1 }
  } finally { peer.close(new Error('host disconnected')) }
}
