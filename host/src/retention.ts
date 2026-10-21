/**
 * The host's retention policy for `tm_log_` topics (docs/host.md,
 * "Retention"): how long a proven batch carrier is kept, and the state file
 * that makes a dropped position stay dropped across restarts.
 *
 * Off unless BLOGS_KEEP_DAYS is set. Then a carrier is dropped only when
 * every one of these holds: its seq lies in an interval of the canonical
 * anchor chain that is complete (every seq held, the root found), every
 * earlier interval of the stream has been dropped the same way, and the
 * interval's anchor timestamp is at least BLOGS_KEEP_DAYS old. Anchor
 * tokens, funding outputs with their transactions, and sweeps are never
 * dropped (spec section 8.1), nor is any unproven carrier.
 */
import { closeSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, writeSync } from 'node:fs'
import { isAbsolute, join } from 'node:path'

/** The least BLOGS_KEEP_DAYS a host may set. */
export const MinKeepDays = 1
/** How often a retention pass runs after the one that follows restore. */
export const RetentionEvery = 60 * 60 * 1000
/** The state file's name in BLOGS_STATE_DIR. */
export const StateFile = 'blogs-retention.json'

export interface RetentionConfig {
  keepMs: number
  stateDir: string
}

/** Reads BLOGS_KEEP_DAYS and BLOGS_STATE_DIR; undefined when retention is off. Throws on anything it cannot take exactly. */
export function parseRetention(env: Record<string, string | undefined>): RetentionConfig | undefined {
  const raw = env['BLOGS_KEEP_DAYS']?.trim() ?? ''
  if (raw === '') {
    if ((env['BLOGS_STATE_DIR'] ?? '') !== '') throw new Error('BLOGS_STATE_DIR is set but BLOGS_KEEP_DAYS is not: set both to keep carriers for a bounded time, or neither to keep everything')
    return undefined
  }
  if (!/^[0-9]+(\.[0-9]+)?$/.test(raw)) throw new Error(`BLOGS_KEEP_DAYS: "${raw}" is not a number of days`)
  const days = Number(raw)
  if (!(days >= MinKeepDays)) throw new Error(`BLOGS_KEEP_DAYS: ${raw} is below the floor of ${MinKeepDays} day`)
  const dir = env['BLOGS_STATE_DIR'] ?? ''
  if (dir === '' || !isAbsolute(dir)) throw new Error('BLOGS_KEEP_DAYS needs BLOGS_STATE_DIR, an absolute directory for the retention state')
  return { keepMs: days * 24 * 60 * 60 * 1000, stateDir: dir }
}

/** One stream's dropped prefix: intervals 1 to interval, seqs 1 to seq, and the commitment kept at seq. */
export interface Mark {
  producer: string
  stream: string
  interval: number
  seq: number
  /** C of the batch at seq, hash byte order, hex: what seq + 1's prev must equal to be linked. */
  c: string
}

export interface RetentionState {
  marks: Mark[]
  /** topic | tree txid -> the lowest funding output of that tree whose carrier was dropped. */
  trees: Array<[string, number]>
}

/** Where the state lives: a file written whole, flushed, then renamed over the last. */
export interface StateStore {
  load(): RetentionState
  save(s: RetentionState): void
}

const isCount = (n: unknown): n is number => typeof n === 'number' && Number.isSafeInteger(n) && n >= 0

/** The state file in dir. load() of a missing file is the empty state; a file that does not parse stops the host. */
export function fileStore(dir: string): StateStore {
  const path = join(dir, StateFile)
  return {
    load(): RetentionState {
      let raw: string
      try {
        raw = readFileSync(path, 'utf8')
      } catch (e) {
        if ((e as NodeJS.ErrnoException).code === 'ENOENT') return { marks: [], trees: [] }
        throw e
      }
      const j = JSON.parse(raw) as { v?: unknown; marks?: unknown; trees?: unknown }
      const ok =
        j.v === 1 &&
        Array.isArray(j.marks) &&
        Array.isArray(j.trees) &&
        j.marks.every(
          (m: Partial<Mark>) =>
            typeof m.producer === 'string' && typeof m.stream === 'string' && isCount(m.interval) && isCount(m.seq) && typeof m.c === 'string' && /^[0-9a-f]{64}$/.test(m.c),
        ) &&
        j.trees.every((t: unknown) => Array.isArray(t) && t.length === 2 && typeof t[0] === 'string' && isCount(t[1]))
      if (!ok) throw new Error(`${path} is not a blogs retention state; a position read as held again would be counted a hole`)
      return { marks: j.marks as Mark[], trees: j.trees as Array<[string, number]> }
    },
    save(s: RetentionState): void {
      mkdirSync(dir, { recursive: true })
      const tmp = `${path}.tmp`
      const fd = openSync(tmp, 'w', 0o600)
      try {
        writeSync(fd, JSON.stringify({ v: 1, marks: s.marks, trees: s.trees }))
        fsyncSync(fd)
      } finally {
        closeSync(fd)
      }
      renameSync(tmp, path)
    },
  }
}

/** A store in memory, for tests. */
export function memoryStore(): StateStore & { saved: number; state: RetentionState } {
  const m = {
    saved: 0,
    state: { marks: [], trees: [] } as RetentionState,
    load: (): RetentionState => structuredClone(m.state),
    save: (s: RetentionState): void => {
      m.state = structuredClone(s)
      m.saved++
    },
  }
  return m
}

export interface Retention {
  keepMs: number
  store: StateStore
  /** The host's clock, Unix milliseconds. */
  now?: () => number
}
