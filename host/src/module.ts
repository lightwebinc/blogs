/**
 * The blogs host module: `tm_log_<name>_<suffix>` for each configured stream
 * and `ls_log` for all of them, loaded into a reference overlay host by path
 * (`OVERLAY_MODULES=/abs/path/to/dist/module.js`). The host carries no
 * blogs code; this file's default export is the factory it calls.
 *
 * Configuration is one environment variable, read when the host calls the
 * factory:
 *
 *   BLOGS_STREAMS=example_app_qzxkvbmwtr=open,audit_mkvbqzxtrw=02ab...+03cd...
 *
 * a comma list of `<stream>=open` or `<stream>=<key>[+<key>...]` (66
 * lowercase hex characters each, compressed keys). Openness is stated,
 * never inferred (spec section 8.1). Every `tm_log_` topic in OVERLAY_TOPICS
 * must be configured here, because a blogs topic left to the host's default
 * manager would admit anything; the host itself refuses a configured stream
 * whose topic OVERLAY_TOPICS does not name.
 *
 * Retention (retention.ts) is off unless BLOGS_KEEP_DAYS is set, with
 * BLOGS_STATE_DIR naming the directory for its state.
 */
import type { Module, ModuleHost } from '@lightwebinc/bcommon'
import { fromHex, validatePolicy, type Policy } from './admit.js'
import { LookupService, TopicPrefix, checkProducer, checkStream, topic } from './logrec.js'
import { LogLookupService } from './ls_log.js'
import { RetentionEvery, fileStore, parseRetention, type Retention } from './retention.js'
import { AdmitKinds, LogTopicManager, Reasons } from './tm_log.js'

/**
 * The bundle's banner, which the bundle step defines (scripts/bundle.js):
 * the package and library versions the host is running. The tsc tree
 * leaves it undefined.
 */
declare const BLOGS_BUILD: string | undefined

export interface StreamConfig {
  stream: string
  policy: Policy
}

/** Parses BLOGS_STREAMS. Throws on anything it cannot take exactly. */
export function parseStreams(raw: string | undefined): StreamConfig[] {
  if (raw === undefined || raw.trim() === '') throw new Error('BLOGS_STREAMS is required: name each stream and its producer policy')
  const out: StreamConfig[] = []
  for (const entry of raw.split(',')) {
    const e = entry.trim()
    const eq = e.indexOf('=')
    if (eq < 0) throw new Error(`BLOGS_STREAMS: "${e}" is not <stream>=open or <stream>=<key>+<key>`)
    const stream = e.slice(0, eq)
    const value = e.slice(eq + 1)
    try {
      checkStream(stream)
    } catch {
      throw new Error(`BLOGS_STREAMS: "${stream}" is not a stream identifier <name>_<suffix>`)
    }
    if (out.some((s) => s.stream === stream)) throw new Error(`BLOGS_STREAMS names ${stream} twice`)
    let policy: Policy
    if (value === 'open') {
      policy = { open: true }
    } else {
      const producers = value.split('+').map((k) => {
        if (!/^[0-9a-f]{66}$/.test(k)) throw new Error(`BLOGS_STREAMS: ${stream}: "${k}" is not 66 lowercase hex characters`)
        const b = fromHex(k)
        try {
          checkProducer(b)
        } catch {
          throw new Error(`BLOGS_STREAMS: ${stream}: ${k} is not a compressed secp256k1 key`)
        }
        return b
      })
      policy = { producers }
    }
    validatePolicy(policy)
    out.push({ stream, policy })
  }
  return out
}

/**
 * The module for these streams. `overlayTopics` is OVERLAY_TOPICS, when
 * known; `retention`, when given, drops proven carriers older than its
 * policy (a pass after restore and every hour).
 */
export function blogsModule(host: ModuleHost, streams: StreamConfig[], overlayTopics?: readonly string[], retention?: Retention): Module {
  if (streams.length === 0) throw new Error('blogs: no streams configured')
  const names = new Set(streams.map((s) => topic(s.stream)))
  for (const t of overlayTopics ?? []) {
    if (t.startsWith(TopicPrefix) && !names.has(t)) {
      throw new Error(`OVERLAY_TOPICS names ${t} but BLOGS_STREAMS does not configure it; it would be admitted by the default manager`)
    }
  }
  for (const kind of AdmitKinds) host.metrics.preset('blogs_admitted_total', { kind })
  for (const reason of Reasons) {
    host.metrics.preset('blogs_refused_total', { reason, recorded: 'yes' })
    host.metrics.preset('blogs_refused_total', { reason, recorded: 'no' })
  }
  for (const c of ['blogs_retractions_total', 'blogs_candidates_dropped_total', 'blogs_tokens_dropped_total', 'blogs_retired_total']) host.metrics.preset(c)
  const ls = new LogLookupService(names, host, retention)
  host.metrics.gauge('blogs_streams', () => ls.streamCount)
  host.metrics.gauge('blogs_batches', () => ls.batchCount)
  host.metrics.gauge('blogs_anchor_tokens', () => ls.tokenCount)
  host.metrics.gauge('blogs_funding_trees', () => ls.treeCount)
  host.metrics.gauge('blogs_retracted_batches', () => ls.retractedCount)
  host.metrics.gauge('blogs_missing_positions', () => ls.missingCount)
  host.metrics.gauge('blogs_fork_positions', () => ls.forkSeqCount)
  host.metrics.gauge('blogs_retired_positions', () => ls.retiredCount)
  if (retention !== undefined) {
    setInterval(() => {
      ls.retain().catch((e: unknown) => host.log('ls_log retention pass failed', { err: String(e) }))
    }, RetentionEvery).unref()
  }
  const topics: Record<string, LogTopicManager> = {}
  for (const s of streams) topics[topic(s.stream)] = new LogTopicManager(topic(s.stream), s.policy, ls.coin, host)
  const build = typeof BLOGS_BUILD === 'string' ? BLOGS_BUILD : 'unbundled'
  host.log('blogs module mounted', {
    topics: [...names].join(','),
    lookup: LookupService,
    build,
    retention: retention === undefined ? 'off' : `${retention.keepMs / 86400000} days`,
  })
  return { topics, lookups: { [LookupService]: ls } }
}

/** The factory the host calls: streams from BLOGS_STREAMS, checked against OVERLAY_TOPICS. */
export default function create(host: ModuleHost): Module {
  const overlay = process.env['OVERLAY_TOPICS']
  const topics = overlay === undefined ? undefined : overlay.split(',').map((t) => t.trim()).filter((t) => t !== '')
  const streams = parseStreams(process.env['BLOGS_STREAMS'])
  const r = parseRetention(process.env)
  return blogsModule(host, streams, topics, r === undefined ? undefined : { keepMs: r.keepMs, store: fileStore(r.stateDir) })
}

export { LogTopicManager, Unrecorded } from './tm_log.js'
export { LogLookupService, parseQuery } from './ls_log.js'
export { parseRetention } from './retention.js'
