/**
 * One tag for both languages: the TypeScript library the codec is built on
 * is the version of the Go library go.mod requires, installed from the
 * vendored tarball of that version.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

const library = '@lightwebinc/bcommon'
const goModule = 'github.com/lightwebinc/bcommon'
const read = (path: string): string => readFileSync(new URL(path, import.meta.url), 'utf8')

test('the library package is the version of the Go library go.mod requires, from its vendored tarball', () => {
  const required = [...read('../../go.mod').matchAll(/^\s*(?:require\s+)?(\S+)\s+(v\S+)/gm)].filter((m) => m[1] === goModule).map((m) => m[2])
  assert.equal(required.length, 1, `go.mod requires ${goModule} ${required.length} times`)
  const installed = (JSON.parse(read(`../node_modules/${library}/package.json`)) as { version: string }).version
  assert.equal(required[0], `v${installed}`)
  const pkg = JSON.parse(read('../package.json')) as { dependencies: Record<string, string> }
  assert.equal(pkg.dependencies[library], `file:vendor/lightwebinc-bcommon-${installed}.tgz`)
})
