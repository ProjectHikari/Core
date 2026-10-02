import { run } from '../../bridge/sdk/typescript/sdk.mts'

await run(async ctx => {
  await ctx.provide('demo.echo', async (method, params) => {
    if (method !== 'echo') throw new Error(`unknown method ${method}`)
    return params
  })
})
