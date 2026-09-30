/**
 * The vectors the TypeScript codec writes, for the Go codec to read: the
 * other direction of the cross-language check. Every input is fixed, so a
 * run writes the same bytes every time; tsvectors.test.ts regenerates the
 * file and compares it byte for byte, and the Go tests decode and rebuild
 * each value.
 *
 *   node dist/tsvectors.js          # write testdata/ts/ts-v1.json
 *   node dist/tsvectors.js -check   # compare, write nothing
 */
import { readFileSync, writeFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { Hash } from '@bsv/sdk'
import { CborMap, encode } from '@lightwebinc/bcommon'
import { toHex } from './admit.js'
import { EncodingLines, ProfileDigest, ProfileFull, encodeAnchor, encodeBatch, encodeLocator, newStream, type Anchor, type Batch, type Locator } from './logrec.js'
import { intervalRoot, prove } from './merkle.js'
import { pushDropScript } from './script.js'

export const file = new URL('../../testdata/ts/ts-v1.json', import.meta.url)

const sha = (s: string): Uint8Array => Uint8Array.from(Hash.sha256(Array.from(new TextEncoder().encode(s))))

/** The fixed test key's identity key (32 bytes of 0x42). */
const producer = Uint8Array.from(Buffer.from('0324653eac434488002cc06bbfb7f10fe18991e35f9fe4302dbea6d2353dc0ab1c', 'hex'))
const t0 = 1767225600000

export function generate(): string {
  // A stream drawn from fixed bytes: the first sixteen hold seven values the
  // draw skips (234 and above), so the suffix needs a second read.
  const random = Uint8Array.from([
    0xff, 0x00, 0xea, 0x19, 0xfe, 0x1a, 0xeb, 0x33, 0xf0, 0x4e, 0xec, 0x67, 0xfa, 0x80, 0x9c, 0xb5, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
    0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
  ])
  let at = 0
  const stream = newStream('ts_written', (n) => random.subarray(at, (at += n)))

  const content = new TextEncoder().encode('{"level":"info","msg":"written by the TypeScript codec"}\nsecond line\n')
  const locator: Locator = {
    digest: sha('blogs/ts-vector/located'),
    length: 4096,
    urls: ['https://archive.example.org/blob/1', 'https://mirror.example.net/b'],
    extra: [{ key: 'region', val: 'example' }, { key: 7n, val: true }],
  }
  const batches: Array<{ name: string; b: Batch }> = [
    {
      name: 'ts-batch-full',
      b: {
        producer,
        stream,
        seq: 1,
        prev: new Uint8Array(32),
        timestamp: t0,
        count: 2,
        encoding: EncodingLines,
        profile: ProfileFull,
        content,
        size: content.length,
        extra: [{ key: 40n, val: [1n, 'future', new CborMap([{ key: 'x', val: null }])] }],
      },
    },
    {
      name: 'ts-batch-digest-unsupported-locator',
      b: {
        producer,
        stream,
        seq: 9007199254740991,
        prev: sha('blogs/ts-vector/prev'),
        timestamp: 9007199254740991,
        count: 70,
        encoding: EncodingLines,
        profile: ProfileDigest,
        locator,
        size: 4096,
        extra: [],
      },
    },
  ]
  const commitments = [1, 2, 3, 4, 5, 6, 7].map((i) => sha(`blogs/ts-vector/commitment/${i}`))
  const anchor: Anchor = {
    producer,
    stream,
    interval: 3,
    prev: sha('blogs/ts-vector/anchor-txid/2'),
    first: 101,
    count: commitments.length,
    root: intervalRoot(commitments),
    timestamp: t0 + 180000,
    extra: [{ key: 12n, val: -5n }],
  }
  // The rebuild does not read the signature, so opaque bytes of a
  // signature's length stand in for one.
  const sig = new Uint8Array(71).fill(0x5a)
  const big = new Uint8Array(300).fill(0xa5)
  const key = Uint8Array.from(Buffer.from('0324d6d6ee75173da1ca8d964d3889792c8911d66c3290a76c367413f5a68e5446', 'hex'))
  const v = {
    description:
      'Written by the TypeScript codec (host/src/tsvectors.ts) and read by the Go codec: each record decodes and re-encodes to the same bytes, the stream draw reproduces from the same random bytes, and the script and root rebuild identically.',
    streamDraw: { name: 'ts_written', random: toHex(random), stream },
    batches: batches.map(({ name, b }) => ({
      name,
      seq: b.seq,
      prev: toHex(b.prev),
      timestamp: b.timestamp,
      count: b.count,
      profile: b.profile,
      size: b.size,
      extraKeys: b.extra.map((p) => Number(p.key)),
      record: toHex(encodeBatch(b)),
    })),
    locator: { record: toHex(encodeLocator(locator)), unsupported: true },
    anchor: {
      interval: anchor.interval,
      first: anchor.first,
      count: anchor.count,
      root: toHex(anchor.root),
      commitments: commitments.map(toHex),
      path: prove(commitments, 5).map((s) => ({ hash: toHex(s.hash), left: s.left })),
      pathSeq: anchor.first + 5,
      extraKeys: [12],
      record: toHex(encodeAnchor(anchor)),
    },
    pushDrop: {
      key: toHex(key),
      fields: [toHex(Uint8Array.of(0x62, 0x6c, 0x01)), toHex(big), toHex(Uint8Array.of(5)), toHex(new Uint8Array(0))],
      signature: toHex(sig),
      script: toHex(pushDropScript(key, [Uint8Array.of(0x62, 0x6c, 0x01), big, Uint8Array.of(5), new Uint8Array(0)], sig)),
    },
    cborCheck: toHex(encode(new CborMap([{ key: 1n, val: 'a' }]))),
  }
  return JSON.stringify(v, null, 2) + '\n'
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const out = generate()
  if (process.argv.includes('-check')) {
    let have = ''
    try {
      have = readFileSync(file, 'utf8')
    } catch {}
    if (have !== out) {
      console.error(`differs: ${fileURLToPath(file)}`)
      process.exit(1)
    }
  } else {
    writeFileSync(file, out)
  }
}
