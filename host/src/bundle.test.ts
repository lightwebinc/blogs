/**
 * The shipped file rather than the tsc tree: bundle/blogs-module.js as the
 * bundle step leaves it, mounted the way the host mounts a module, admitting
 * the golden funding tree and carriers and answering ls_log. Every other
 * test runs on dist/, which resolves the library from node_modules, so only
 * this one proves the bundle works with the library inlined and nothing but
 * @bsv/sdk beside it.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { existsSync, readFileSync } from 'node:fs'
import type { Module, ModuleHost } from '@lightwebinc/bcommon'
import { FakeHost, beefOf, byName, txOf, txVectors } from './testutil.js'

const v = txVectors()
// dist/bundle.test.js -> host/. Imported by URL, so the type checker does not
// go looking for a file that exists only after the bundle step.
const bundleUrl = new URL('../bundle/blogs-module.js', import.meta.url)

function version(pkg: string): string {
  const url = new URL(`../node_modules/${pkg}/package.json`, import.meta.url)
  return (JSON.parse(readFileSync(url, 'utf8')) as { version: string }).version
}

async function mount(host: FakeHost): Promise<Module> {
  const { default: create } = (await import(bundleUrl.href)) as { default: (host: ModuleHost) => Module }
  const saved = { streams: process.env['BLOGS_STREAMS'], topics: process.env['OVERLAY_TOPICS'] }
  try {
    process.env['BLOGS_STREAMS'] = `${v.stream}=${v.identityKey}`
    process.env['OVERLAY_TOPICS'] = `tm_anytx,${v.topic}`
    return create(host)
  } finally {
    for (const [k, val] of [['BLOGS_STREAMS', saved.streams], ['OVERLAY_TOPICS', saved.topics]] as const) {
      if (val === undefined) delete process.env[k]
      else process.env[k] = val
    }
  }
}

test('the shipped bundle admits the golden funding tree and carriers, and ls_log answers them', async () => {
  const host = new FakeHost()
  const mod = await mount(host)
  const tm = mod.topics?.[v.topic]
  const ls = mod.lookups?.['ls_log']
  if (tm === undefined || ls === undefined) throw new Error('the bundle did not mount the stream and ls_log')

  assert.deepEqual(await tm.identifyAdmissibleOutputs(beefOf(byName(v, 'funding-tree')), []), { outputsToAdmit: [0, 1, 2, 3, 4], coinsToRetain: [] })
  for (const name of ['carrier-seq-1', 'carrier-seq-2', 'carrier-seq-3']) {
    assert.deepEqual(await tm.identifyAdmissibleOutputs(beefOf(byName(v, name)), []), { outputsToAdmit: [0], coinsToRetain: [] }, name)
  }
  assert.equal(host.count('blogs_admitted_total', { kind: 'batch' }), 3)
  assert.equal(host.count('blogs_admitted_total', { kind: 'funding' }), 1)

  // A refusal code comes from the inlined record layer and is the label the
  // tsc tree counts under.
  assert.deepEqual(await tm.identifyAdmissibleOutputs(beefOf(byName(v, 'carrier-mineable-before-unlock')), []), { outputsToAdmit: [], coinsToRetain: [] })
  assert.equal(host.count('blogs_refused_total', { reason: 'mineable', recorded: 'yes' }), 1)

  for (const name of ['carrier-seq-1', 'carrier-seq-2']) {
    await ls.outputAdmittedByTopic({ mode: 'whole-tx', atomicBEEF: txOf(byName(v, name)).toAtomicBEEF(), outputIndex: 0, topic: v.topic })
  }
  const answer = await ls.lookup({ service: 'ls_log', query: { stream: v.stream, producer: v.identityKey, seq: 2 } })
  assert.deepEqual(
    answer.map((f) => `${f.txid}.${f.outputIndex}`),
    [`${byName(v, 'carrier-seq-2').txid}.0`],
  )
  assert.equal(host.gauges.get('blogs_batches')?.(), 2)
})

test('the bundle names its versions in its banner and in the host log', async () => {
  const banner = readFileSync(bundleUrl, 'utf8').split('\n', 1)[0] ?? ''
  assert.ok(banner.startsWith('// @lightwebinc/blogs-host '), banner)
  assert.ok(banner.includes(`@lightwebinc/bcommon ${version('@lightwebinc/bcommon')} inlined`), banner)
  assert.ok(banner.includes(`imports @bsv/sdk (built against ${version('@bsv/sdk')})`), banner)
  const host = new FakeHost()
  await mount(host)
  const mounted = host.lines.filter((l) => l.msg === 'blogs module mounted')
  assert.equal(mounted.length, 1)
  assert.equal(`// ${String(mounted[0]?.extra?.['build'])}`, banner)
})

test('the bundle imports nothing but @bsv/sdk and the Node built-ins retention needs, and carries no source map', () => {
  const text = readFileSync(bundleUrl, 'utf8')
  const specifiers = new Set([...text.matchAll(/^\s*(?:import|export)\b[^'"]*?from\s*["']([^"']+)["']/gm)].map((m) => m[1]))
  assert.deepEqual([...specifiers].sort(), ['@bsv/sdk', 'node:fs', 'node:path'])
  assert.equal(/\bimport\s*\(|\brequire\s*\(/.test(text), false, 'no dynamic import or require')
  assert.equal(text.includes('sourceMappingURL'), false)
  assert.equal(existsSync(new URL('../bundle/blogs-module.js.map', import.meta.url)), false)
})
