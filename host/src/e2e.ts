/**
 * One publish-and-lookup end to end on a local reference overlay host,
 * with this module loaded from the bundle (bundle/blogs-module.js) through
 * OVERLAY_MODULES.
 *
 *   node dist/e2e.js /path/to/reference-host
 *
 * The reference host directory holds its compiled dist/index.js and its
 * node_modules. The bundle is staged as a deployment places it: copied into
 * a modules/ directory whose only node_modules is the host's, so it loads
 * with nothing but what the host provides, and the SDK it imports is the
 * host's own copy. The run needs Docker, for a throwaway MySQL (the host's
 * only store), and serves the vector block headers itself in the host's
 * native header shape (/v1/root/<height>, /v1/tip). It submits the
 * transaction vectors in a scrambled order, interval 2's anchor before
 * interval 1's among them, asks ls_log over HTTP, restarts the host, and
 * asks again from the restored index. Everything it starts, it stops.
 */
import { execFileSync, spawn, type ChildProcess } from 'node:child_process'
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, symlinkSync } from 'node:fs'
import { createServer, type Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Transaction } from '@bsv/sdk'
import { byName, beefOf, txVectors } from './testutil.js'

const v = txVectors()
const password = 'blogs-e2e'
const bundle = fileURLToPath(new URL('../bundle/blogs-module.js', import.meta.url))

/**
 * Stages the bundle beside the reference host's node_modules and returns
 * the module's path and the staging root to remove.
 */
function stage(dir: string): { file: string; root: string } {
  const root = mkdtempSync(join(tmpdir(), 'blogs-e2e-'))
  symlinkSync(resolve(dir, 'node_modules'), join(root, 'node_modules'))
  mkdirSync(join(root, 'modules', 'blogs'), { recursive: true })
  const file = join(root, 'modules', 'blogs', 'blogs-module.js')
  copyFileSync(bundle, file)
  return { file, root }
}

function fail(msg: string): never {
  throw new Error(`e2e: ${msg}`)
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

async function headers(): Promise<Server> {
  const roots = new Map(v.headers.map((h) => [h.height, h.merkleRoot]))
  const tip = Math.max(...roots.keys())
  const server = createServer((req, res) => {
    const m = /^\/v1\/root\/(\d+)$/.exec(req.url ?? '')
    const body = req.url === '/v1/tip' ? { height: tip } : m !== null && roots.has(Number(m[1])) ? { merkleRoot: roots.get(Number(m[1])) } : undefined
    res.writeHead(body === undefined ? 404 : 200, { 'content-type': 'application/json' })
    res.end(JSON.stringify(body ?? {}))
  })
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
  return server
}

function docker(...args: string[]): string {
  return execFileSync('docker', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim()
}

async function mysql(): Promise<{ id: string; port: number }> {
  const id = docker('run', '-d', '--rm', '-e', `MYSQL_ROOT_PASSWORD=${password}`, '-e', 'MYSQL_DATABASE=overlay', '-p', '127.0.0.1::3306', '--tmpfs', '/var/lib/mysql', 'mysql:9')
  const port = Number(docker('port', id, '3306/tcp').split('\n')[0]!.split(':').pop())
  for (let i = 0; i < 120; i++) {
    try {
      // Over TCP, which the image's initialisation server does not listen on.
      docker('exec', id, 'mysql', '-h127.0.0.1', '-uroot', `-p${password}`, '-e', 'select 1')
      return { id, port }
    } catch {
      await sleep(1000)
    }
  }
  docker('rm', '-f', id)
  fail('MySQL did not come up')
}

interface Host {
  proc: ChildProcess
  url: string
  out: string[]
}

async function startHost(dir: string, moduleFile: string, db: number, hdr: number, port: number): Promise<Host> {
  const out: string[] = []
  const proc = spawn(process.execPath, [resolve(dir, 'dist/index.js')], {
    env: {
      PATH: process.env['PATH'] ?? '',
      OVERLAY_TOPICS: v.topic,
      OVERLAY_MODULES: moduleFile,
      BLOGS_STREAMS: `${v.stream}=open`,
      OVERLAY_KNEX_URL: `mysql://root:${password}@127.0.0.1:${db}/overlay`,
      OVERLAY_CHAIN_TRACKER_URL: `http://127.0.0.1:${hdr}`,
      OVERLAY_ADMIN_TOKEN: 'blogs-e2e',
      OVERLAY_LISTEN: '127.0.0.1',
      OVERLAY_PORT: String(port),
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  proc.stdout!.on('data', (b: Buffer) => out.push(b.toString()))
  proc.stderr!.on('data', (b: Buffer) => out.push(b.toString()))
  const url = `http://127.0.0.1:${port}`
  for (let i = 0; i < 60; i++) {
    if (proc.exitCode !== null) fail(`the host exited ${proc.exitCode}:\n${out.join('')}`)
    try {
      if ((await fetch(`${url}/readyz`)).status === 200) return { proc, url, out }
    } catch {
      // not listening yet
    }
    await sleep(500)
  }
  proc.kill('SIGKILL')
  fail(`the host did not become ready:\n${out.join('')}`)
}

async function stopHost(h: Host): Promise<void> {
  if (h.proc.exitCode !== null) return
  const done = new Promise((r) => h.proc.once('exit', r))
  h.proc.kill('SIGTERM')
  await done
}

async function submit(h: Host, name: string): Promise<number[]> {
  const res = await fetch(`${h.url}/submit`, {
    method: 'POST',
    headers: { 'content-type': 'application/octet-stream', 'x-topics': v.topic },
    body: Uint8Array.from(beefOf(byName(v, name))),
  })
  const body = (await res.json()) as Record<string, { outputsToAdmit?: number[] }>
  if (res.status !== 200) fail(`submit ${name}: ${res.status} ${JSON.stringify(body)}`)
  return body[v.topic]?.outputsToAdmit ?? []
}

async function ask(h: Host, query: Record<string, unknown>): Promise<string[]> {
  const res = await fetch(`${h.url}/lookup`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ service: 'ls_log', query }) })
  const body = (await res.json()) as { outputs?: Array<{ beef: number[] }>; error?: string }
  if (res.status !== 200) fail(`lookup ${JSON.stringify(query)}: ${res.status} ${body.error}`)
  return (body.outputs ?? []).map((o) => Transaction.fromBEEF(o.beef).id('hex'))
}

/** The tree class: each answered txid with its proof's block height (-1 with no proof). */
async function askTree(h: Host, txid: string): Promise<Array<[string, number]>> {
  const res = await fetch(`${h.url}/lookup`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ service: 'ls_log', query: { stream: v.stream, tree: txid } }) })
  const body = (await res.json()) as { outputs?: Array<{ beef: number[] }>; error?: string }
  if (res.status !== 200) fail(`lookup tree ${txid}: ${res.status} ${body.error}`)
  return (body.outputs ?? []).map((o) => {
    const tx = Transaction.fromBEEF(o.beef)
    return [tx.id('hex'), tx.merklePath?.blockHeight ?? -1]
  })
}

function expect(what: string, got: unknown, want: unknown): void {
  const g = JSON.stringify(got)
  const w = JSON.stringify(want)
  if (g !== w) fail(`${what}: got ${g}, want ${w}`)
  console.log(`ok  ${what}`)
}

async function main(): Promise<void> {
  const dir = process.argv[2]
  if (dir === undefined) fail('usage: node dist/e2e.js /path/to/reference-host')
  const hdr = await headers()
  const db = await mysql()
  const port = await new Promise<number>((r) => {
    const s = createServer().listen(0, '127.0.0.1', () => {
      const p = (s.address() as AddressInfo).port
      s.close(() => r(p))
    })
  })
  const hport = (hdr.address() as AddressInfo).port
  const staged = stage(dir)
  let host: Host | undefined
  try {
    host = await startHost(dir, staged.file, db.port, hport, port)
    const mounted = host.out.join('').split('\n').find((l) => l.includes('blogs module mounted'))
    if (mounted === undefined || !mounted.includes('with @lightwebinc/bcommon')) fail(`the bundle's mount line is missing:\n${host.out.join('')}`)
    console.log('ok  the bundle mounts from beside the host\'s node_modules')
    const id = (n: string): string => byName(v, n).txid
    const q = (k: string, n: number) => ({ stream: v.stream, producer: v.identityKey, [k]: n })
    expect('funding tree admits its five funding outputs', await submit(host, 'funding-tree'), [0, 1, 2, 3, 4])
    expect('carrier seq 3 before seq 1 and 2', await submit(host, 'carrier-seq-3'), [0])
    expect('carrier seq 1', await submit(host, 'carrier-seq-1'), [0])
    expect('interval 2 before interval 1 is not admitted', await submit(host, 'anchor-interval-2'), [])
    expect('carrier seq 2', await submit(host, 'carrier-seq-2'), [0])
    expect('interval 1', await submit(host, 'anchor-interval-1'), [0])
    expect('interval 2 offered again is admitted', await submit(host, 'anchor-interval-2'), [0])
    const answers = async (when: string, swept: boolean): Promise<void> => {
      expect(`${when}: seq 2`, await ask(host!, q('seq', 2)), swept ? [] : [id('carrier-seq-2')])
      expect(`${when}: seq 3`, await ask(host!, q('seq', 3)), [id('carrier-seq-3')])
      expect(`${when}: interval 1 (spent by interval 2)`, await ask(host!, q('interval', 1)), [id('anchor-interval-1')])
      expect(`${when}: interval 2`, await ask(host!, q('interval', 2)), [id('anchor-interval-2')])
      // Every funding output of the tree is spent by now; the host still
      // answers the tree, with its proof.
      const tree = await askTree(host!, id('funding-tree'))
      expect(`${when}: the fully spent funding tree, by txid`, tree.map(([t]) => t), [id('funding-tree')])
      if (!v.headers.some((h) => h.height === tree[0]![1])) fail(`${when}: the tree's BEEF carries no proof at a vector height`)
      expect(`${when}: a carrier is not a tree`, await askTree(host!, id('carrier-seq-3')), [])
    }
    await answers('live', false)
    expect('the sweep admits its tombstone', await submit(host, 'sweep'), [0])
    expect('live: retracted seq 1 answers nothing', await ask(host, q('seq', 1)), [])
    const metrics = await (await fetch(`${host.url}/metrics`)).text()
    const line = metrics.split('\n').find((l) => l.startsWith('blogs_refused_total') && l.includes('reason="anchor-input"') && l.includes('recorded="no"'))
    expect('the deferred anchor is counted', line?.trim().split(' ').pop(), '1')
    // The engine counts a raised refusal as a failed topic for that submission.
    const failed = metrics.split('\n').find((l) => l.startsWith('overlay_host_topic_failures_total') && l.includes(v.topic))
    expect('the host counts it as one failed topic admission', failed?.trim().split(' ').pop(), '1')
    await stopHost(host)
    host = await startHost(dir, staged.file, db.port, hport, port)
    if (!host.out.join('').includes('ls_log')) fail(`no restore line:\n${host.out.join('')}`)
    await answers('restored', true)
    expect('restored: retracted seq 1 answers nothing', await ask(host, q('seq', 1)), [])
    console.log('e2e: PASS')
  } finally {
    if (host !== undefined) await stopHost(host)
    docker('rm', '-f', db.id)
    hdr.close()
    rmSync(staged.root, { recursive: true, force: true })
  }
}

main().catch((e: unknown) => {
  console.error(e instanceof Error ? e.message : e)
  process.exit(1)
})
