import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync, readdirSync } from 'node:fs'
import { file, generate } from './tsvectors.js'

test('the TypeScript-written vectors are what the codec writes, byte for byte', () => {
  assert.equal(readFileSync(file, 'utf8'), generate(), 'run: node dist/tsvectors.js')
  assert.equal(generate(), generate())
  assert.deepEqual(readdirSync(new URL('.', file)), ['ts-v1.json'])
})
