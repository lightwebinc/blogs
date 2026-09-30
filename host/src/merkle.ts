/**
 * The interval root and inclusion paths of spec section 5.2: RFC 6962 over
 * the covered batches' commitments in ascending seq order, the same tree as
 * bcommon's Go commit package. The leaf datum is the commitment; a leaf is
 * SHA-256(0x00 || C), a node SHA-256(0x01 || left || right), split at the
 * largest power of two strictly below n, and the last leaf is never
 * duplicated.
 */
import { Hash } from '@bsv/sdk'
import { anchorCovers, type Anchor } from './logrec.js'

function sha256(parts: Uint8Array[]): Uint8Array {
  const all: number[] = []
  for (const p of parts) all.push(...p)
  return Uint8Array.from(Hash.sha256(all))
}

export function leafHash(c: Uint8Array): Uint8Array {
  return sha256([Uint8Array.of(0x00), c])
}

export function nodeHash(l: Uint8Array, r: Uint8Array): Uint8Array {
  return sha256([Uint8Array.of(0x01), l, r])
}

function split(n: number): number {
  let k = 1
  while (k * 2 < n) k *= 2
  return k
}

function subtree(leaves: Uint8Array[]): Uint8Array {
  if (leaves.length === 1) return leafHash(leaves[0]!)
  const k = split(leaves.length)
  return nodeHash(subtree(leaves.slice(0, k)), subtree(leaves.slice(k)))
}

/** The root over commitments; the empty tree is SHA-256 of nothing. */
export function intervalRoot(commitments: Uint8Array[]): Uint8Array {
  return commitments.length === 0 ? sha256([]) : subtree(commitments)
}

/** One step of a path, leaf to root; left means the sibling is on the left. */
export interface Step {
  hash: Uint8Array
  left: boolean
}

/** The inclusion path of the leaf at index. */
export function prove(commitments: Uint8Array[], index: number): Step[] {
  if (!Number.isInteger(index) || index < 0 || index >= commitments.length) throw new RangeError('merkle: index out of range')
  if (commitments.length === 1) return []
  const k = split(commitments.length)
  if (index < k) return [...prove(commitments.slice(0, k), index), { hash: subtree(commitments.slice(k)), left: false }]
  return [...prove(commitments.slice(k), index - k), { hash: subtree(commitments.slice(0, k)), left: true }]
}

/** Whether path takes c's leaf to root. It proves no index by itself. */
export function verifyPath(c: Uint8Array, path: Step[], root: Uint8Array): boolean {
  let h = leafHash(c)
  for (const s of path) h = s.left ? nodeHash(s.hash, h) : nodeHash(h, s.hash)
  return h.length === root.length && h.every((x, i) => x === root[i])
}

/**
 * The leaf index a path proves in a tree of n leaves, or undefined for a
 * path whose length or sides no leaf of that tree has.
 */
export function pathIndex(path: Step[], n: number): number | undefined {
  let lo = 0
  let size = n
  for (let i = path.length - 1; i >= 0; i--) {
    if (size < 2) return undefined
    const k = split(size)
    if (path[i]!.left) {
      lo += k
      size -= k
    } else {
      size = k
    }
  }
  return size === 1 ? lo : undefined
}

/**
 * Whether the batch with commitment c sits at seq under the anchor: the
 * path's shape must prove index seq - first in a tree of count leaves, or a
 * path for another position would be taken for this one.
 */
export function verifyBatch(a: Anchor, c: Uint8Array, seq: number, path: Step[]): boolean {
  if (!anchorCovers(a, seq)) return false
  return pathIndex(path, a.count) === seq - a.first && verifyPath(c, path, a.root)
}
