/**
 * The compiled module as a reference host loads it: imported by absolute
 * path, its default export called with the host's logger and metrics, the
 * result held to the loader's shape rules, and its configuration refused
 * rather than guessed.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { fileURLToPath, pathToFileURL } from 'node:url'
import type { Module, ModuleHost } from '@lightwebinc/bcommon'
import { parseStreams } from './module.js'
import { FakeHost, txVectors } from './testutil.js'

const v = txVectors()
const key2 = `02${'79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798'}`

test('BLOGS_STREAMS: open or a producer set per stream, anything else refused', () => {
  assert.deepEqual(parseStreams(`${v.stream}=open`), [{ stream: v.stream, policy: { open: true } }])
  const [s] = parseStreams(` ${v.stream}=${v.identityKey}+${key2} `)
  assert.ok(s !== undefined && 'producers' in s.policy && s.policy.producers.length === 2)
  for (const bad of [
    undefined,
    '',
    v.stream,
    `${v.stream}=`,
    `${v.stream}=closed`,
    `${v.stream}=open,${v.stream}=open`,
    `example_app=open`,
    `${v.stream}=${v.identityKey.toUpperCase()}`,
    `${v.stream}=04${'00'.repeat(32)}`,
    `${v.stream}=${v.identityKey}+`,
  ]) {
    assert.throws(() => parseStreams(bad), JSON.stringify(bad))
  }
})

/** The reference loader's checks on what a factory returned. */
function loaderShape(m: unknown): Module {
  assert.ok(typeof m === 'object' && m !== null)
  const mod = m as Module
  assert.ok(Object.keys(mod.topics ?? {}).length + Object.keys(mod.lookups ?? {}).length > 0, 'mounts something')
  for (const tm of Object.values(mod.topics ?? {})) assert.equal(typeof tm.identifyAdmissibleOutputs, 'function')
  for (const ls of Object.values(mod.lookups ?? {})) {
    assert.equal(typeof ls.lookup, 'function')
    assert.equal(typeof ls.outputAdmittedByTopic, 'function')
    assert.equal(typeof ls.admissionMode, 'string')
    assert.equal(typeof ls.restore, 'function', 'the index is in memory, so the module must restore')
  }
  return mod
}

test('dist/module.js loads by absolute path, as OVERLAY_MODULES names it, and mounts each stream and ls_log', async () => {
  const path = fileURLToPath(new URL('./module.js', import.meta.url))
  const imported = (await import(pathToFileURL(path).href)) as { default: (h: ModuleHost) => Module }
  assert.equal(typeof imported.default, 'function')
  const saved = { streams: process.env['BLOGS_STREAMS'], topics: process.env['OVERLAY_TOPICS'] }
  try {
    process.env['BLOGS_STREAMS'] = `${v.stream}=open,audit_mkvbqzxtrw=${v.identityKey}`
    process.env['OVERLAY_TOPICS'] = `tm_anytx,${v.topic},tm_log_audit_mkvbqzxtrw`
    const host = new FakeHost()
    const mod = loaderShape(imported.default(host))
    assert.deepEqual(Object.keys(mod.topics!).sort(), ['tm_log_audit_mkvbqzxtrw', v.topic])
    assert.deepEqual(Object.keys(mod.lookups!), ['ls_log'])
    assert.ok(host.counters.has('blogs_refused_total{reason="anchor-input",recorded="no"}'), 'counters are preset')
    assert.ok(host.gauges.has('blogs_missing_positions'))

    // A tm_log_ topic the host carries but the module does not configure
    // would fall to the admit-everything default: refused.
    process.env['OVERLAY_TOPICS'] = `${v.topic},tm_log_other_mkvbqzxtrw`
    assert.throws(() => imported.default(new FakeHost()), /tm_log_other_mkvbqzxtrw/)
    delete process.env['BLOGS_STREAMS']
    assert.throws(() => imported.default(new FakeHost()), /BLOGS_STREAMS/)
  } finally {
    for (const [k, val] of [['BLOGS_STREAMS', saved.streams], ['OVERLAY_TOPICS', saved.topics]] as const) {
      if (val === undefined) delete process.env[k]
      else process.env[k] = val
    }
  }
})
