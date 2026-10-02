import { run } from '../sdk/typescript/sdk.mts'
import { existsSync, writeFileSync, unlinkSync } from 'node:fs'

await run(async ctx => {
  const name = process.env.SERVICE_NAME ?? 'test.remote'
  const host = process.env.HOST_SERVICE ?? 'test.host'
  const file = process.env.RESOURCE_FILE
  if (file) {
    writeFileSync(file, String(process.pid))
    let attempt = 0
    await ctx.defer('file', async () => {
      if (process.env.CLEANUP_ATTEMPTS) writeFileSync(process.env.CLEANUP_ATTEMPTS, String(++attempt))
      if (process.env.SLOW_CLEANUP === '1') await new Promise(resolve => setTimeout(resolve, 150))
      if (process.env.FAIL_CLEANUP === '1' && ++attempt === 1) throw new Error('cleanup failed once')
      if (existsSync(file)) unlinkSync(file)
    })
  }
  const startValue = await ctx.call(host, 'init')
  const seen: unknown[] = []
  let ticks = 0
  let lateReady = false
  await ctx.on('test.changed', payload => { seen.push(payload) })
  await ctx.on('test.parallel', async () => { await new Promise(resolve => setTimeout(resolve, 20)) })
  await ctx.after('tick', 0, () => { ticks++ })
  await ctx.provide(name, async (method, params, signal) => {
    if (method === 'echo') return params
    if (method === 'init') return Number(startValue) + 1
    if (method === 'info') return { generation: ctx.generation, pid: process.pid, startValue, seen: seen as any, ticks, lateReady }
    if (method === 'late') {
      ctx.go('late resource', async signal => {
        lateReady = true
        await new Promise<void>(resolve => {
          if (signal.aborted) resolve()
          else signal.addEventListener('abort', () => resolve(), { once: true })
        })
        const file = process.env.LATE_FILE!
        writeFileSync(file, 'late')
        await ctx.defer('late file', () => { if (existsSync(file)) unlinkSync(file) })
      })
      return null
    }
    if (method === 'emit') { await ctx.emit('test.changed', params, signal); return null }
    if (method === 'parallel') { await ctx.parallel('test.parallel', params, signal); return null }
    if (method === 'wrongMode') { await ctx.parallel('test.changed', params, signal); return null }
    if (method === 'undeclared') return ctx.lookup('not.declared')
    if (method === 'hostSlow') return ctx.call(host, 'slow', null, signal)
    if (method === 'fail') throw new Error('service error')
    if (method === 'crash') process.exit(3)
    if (method === 'fault') {
      ctx.go('broken task', async () => { await new Promise(resolve => setTimeout(resolve, 10)); throw new Error('task failed') })
      return null
    }
    if (method === 'slow') {
      await new Promise<void>(resolve => {
        if (signal.aborted) resolve()
        else signal.addEventListener('abort', () => resolve(), { once: true })
      })
      return null
    }
    if (method === 'ignore') { await new Promise(resolve => setTimeout(resolve, 10_000)); return null }
    throw new Error(`unknown method ${method}`)
  })
  if (process.env.FAIL_START === '1') throw new Error('startup failed')
  if (process.env.HANG_START === '1') await new Promise(resolve => setTimeout(resolve, 10_000))
})
