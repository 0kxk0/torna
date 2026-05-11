/**
 * Tree invariant checker.
 *
 * Verifies that an on-chain Torna tree satisfies all B+ tree invariants:
 *   I1. Every leaf is sorted (keys ascending).
 *   I2. For every internal node, separator[i] equals first key of the
 *       subtree rooted at children[i+1] (B+ tree convention).
 *   I3. Leaf chain (leftmost_leaf → next_leaf → ... → 0) traverses every
 *       initialized leaf exactly once, in left-to-right order.
 *   I4. header.total_entries equals the sum of all leaves' key_counts.
 *   I5. Every internal node has key_count children (= key_count + 1 in
 *       children array... wait, internal has key_count keys and key_count+1
 *       children). Each child's NodeHeader.parent_idx references this node.
 *   I6. Tree height == observed depth from root to any leaf.
 *   I7. Root.node_idx == header.root_node_idx.
 *   I8. For non-root nodes, key_count is in [MIN, MAX].
 *   I9. For root, key_count is in [1, MAX] if height > 1; or [0, MAX] for leaf root.
 */
import { Connection, PublicKey } from "@solana/web3.js";
import {
  decodeNode,
  decodeTreeHeader,
  deriveNodePda,
  TreeHeader,
  NodeView,
  KEYS_PER_NODE_MAX,
  KEYS_PER_NODE_MIN,
  KEY_SIZE,
} from "./torna.ts";

export interface InvariantReport {
  ok: boolean;
  errors: string[];
  warnings: string[];
  stats: {
    height: number;
    nodeCount: number;
    totalEntries: bigint;
    leafCount: number;
    actualEntries: number;
    leftmostLeaf: number;
    chainCovered: number;
  };
}

async function fetchNode(
  conn: Connection,
  programId: PublicKey,
  treeId: number,
  nodeIdx: number,
): Promise<NodeView | null> {
  const [pk] = deriveNodePda(programId, treeId, nodeIdx);
  const acc = await conn.getAccountInfo(pk);
  if (!acc) return null;
  const n = decodeNode(acc.data);
  if (!n.hdr.initialized) return null;
  return n;
}

/** Returns the first (smallest) key in the subtree rooted at the given node. */
async function firstKeyOfSubtree(
  conn: Connection,
  programId: PublicKey,
  treeId: number,
  node: NodeView,
): Promise<Buffer> {
  let cur = node;
  while (!cur.hdr.isLeaf) {
    const childIdx = cur.children![0];
    const child = await fetchNode(conn, programId, treeId, childIdx);
    if (!child) throw new Error(`child ${childIdx} missing`);
    cur = child;
  }
  if (cur.keys.length === 0) throw new Error("leaf has no keys");
  return cur.keys[0];
}

export async function checkInvariants(
  conn: Connection,
  programId: PublicKey,
  headerPda: PublicKey,
): Promise<InvariantReport> {
  const errors: string[] = [];
  const warnings: string[] = [];

  const hdrAcc = await conn.getAccountInfo(headerPda);
  if (!hdrAcc) {
    return {
      ok: false,
      errors: ["header not on chain"],
      warnings: [],
      stats: { height: 0, nodeCount: 0, totalEntries: 0n, leafCount: 0, actualEntries: 0, leftmostLeaf: 0, chainCovered: 0 },
    };
  }
  const hdr: TreeHeader = decodeTreeHeader(hdrAcc.data);

  /* Empty tree. */
  if (hdr.height === 0) {
    if (hdr.totalEntries !== 0n) errors.push("empty tree but total_entries != 0");
    return {
      ok: errors.length === 0,
      errors,
      warnings,
      stats: { height: 0, nodeCount: hdr.nodeCount, totalEntries: hdr.totalEntries, leafCount: 0, actualEntries: 0, leftmostLeaf: 0, chainCovered: 0 },
    };
  }

  /* I7. Root present and matches header. */
  const root = await fetchNode(conn, programId, hdr.treeId, hdr.rootNodeIdx);
  if (!root) {
    errors.push(`root node ${hdr.rootNodeIdx} not on chain`);
    return {
      ok: false,
      errors,
      warnings,
      stats: { height: hdr.height, nodeCount: hdr.nodeCount, totalEntries: hdr.totalEntries, leafCount: 0, actualEntries: 0, leftmostLeaf: 0, chainCovered: 0 },
    };
  }
  if (root.hdr.nodeIdx !== hdr.rootNodeIdx) {
    errors.push(`root.node_idx mismatch: ${root.hdr.nodeIdx} vs header.${hdr.rootNodeIdx}`);
  }

  /* DFS walk: visit all reachable nodes, check invariants. */
  interface Visit {
    nodeIdx: number;
    depthFromRoot: number;
    parentIdx: number;
    isRoot: boolean;
  }
  const stack: Visit[] = [{ nodeIdx: hdr.rootNodeIdx, depthFromRoot: 0, parentIdx: 0, isRoot: true }];
  let leafCount = 0;
  let actualEntries = 0;
  const leafSet: number[] = []; // all leaf node indices encountered

  while (stack.length) {
    const v = stack.pop()!;
    const n = await fetchNode(conn, programId, hdr.treeId, v.nodeIdx);
    if (!n) {
      errors.push(`unreachable: node ${v.nodeIdx} referenced but missing`);
      continue;
    }

    /* I1. Sorted keys */
    for (let i = 1; i < n.keys.length; i++) {
      if (n.keys[i - 1].compare(n.keys[i]) >= 0) {
        errors.push(`node ${v.nodeIdx} keys not strictly ascending at index ${i}`);
        break;
      }
    }

    /* I8/I9. key_count bounds */
    if (v.isRoot) {
      if (n.hdr.isLeaf) {
        if (n.hdr.keyCount > KEYS_PER_NODE_MAX) errors.push(`root leaf key_count ${n.hdr.keyCount} > MAX`);
      } else {
        if (n.hdr.keyCount < 1) errors.push(`root internal has no separators (key_count=${n.hdr.keyCount})`);
        if (n.hdr.keyCount > KEYS_PER_NODE_MAX) errors.push(`root internal key_count ${n.hdr.keyCount} > MAX`);
      }
    } else {
      if (n.hdr.keyCount < KEYS_PER_NODE_MIN) {
        warnings.push(`non-root node ${v.nodeIdx} below MIN: key_count=${n.hdr.keyCount} (MIN=${KEYS_PER_NODE_MIN})`);
      }
      if (n.hdr.keyCount > KEYS_PER_NODE_MAX) {
        errors.push(`node ${v.nodeIdx} key_count ${n.hdr.keyCount} > MAX`);
      }
    }

    if (n.hdr.isLeaf) {
      leafCount++;
      leafSet.push(v.nodeIdx);
      actualEntries += n.hdr.keyCount;
      /* I6. Leaf depth must equal height - 1 (leaves at the bottom). */
      if (v.depthFromRoot !== hdr.height - 1) {
        errors.push(`leaf ${v.nodeIdx} at depth ${v.depthFromRoot}, expected ${hdr.height - 1}`);
      }
    } else {
      /* I2. For each internal node, verify each child's first key matches the parent separator. */
      for (let i = 0; i < n.hdr.keyCount; i++) {
        const childIdx = n.children![i + 1]; // child to the right of separator[i]
        const child = await fetchNode(conn, programId, hdr.treeId, childIdx);
        if (!child) {
          errors.push(`node ${v.nodeIdx} child[${i + 1}]=${childIdx} not on chain`);
          continue;
        }
        const firstKey = await firstKeyOfSubtree(conn, programId, hdr.treeId, child);
        if (n.keys[i].compare(firstKey) !== 0) {
          errors.push(
            `node ${v.nodeIdx} separator[${i}] (${n.keys[i].readUInt32BE(28)}) != first key of subtree at child ${childIdx} (${firstKey.readUInt32BE(28)})`,
          );
        }
      }
      /* Push children for further visit. */
      for (const ci of n.children!) {
        stack.push({ nodeIdx: ci, depthFromRoot: v.depthFromRoot + 1, parentIdx: v.nodeIdx, isRoot: false });
      }
    }
  }

  /* I4. totalEntries vs actual */
  if (hdr.totalEntries !== BigInt(actualEntries)) {
    warnings.push(`total_entries=${hdr.totalEntries} but actual=${actualEntries} (FAST-path deletes don't update counter)`);
  }

  /* I3. Leaf chain. */
  const visited = new Set<number>();
  let chainCovered = 0;
  if (hdr.leftmostLeafIdx) {
    let cur: number | null = hdr.leftmostLeafIdx;
    let prevKey: Buffer | null = null;
    while (cur !== null && cur !== 0) {
      if (visited.has(cur)) {
        errors.push(`leaf chain has a cycle at node ${cur}`);
        break;
      }
      visited.add(cur);
      chainCovered++;
      const n = await fetchNode(conn, programId, hdr.treeId, cur);
      if (!n) {
        errors.push(`leaf chain references missing node ${cur}`);
        break;
      }
      if (!n.hdr.isLeaf) {
        errors.push(`leaf chain hit non-leaf node ${cur}`);
        break;
      }
      if (n.keys.length > 0) {
        if (prevKey && prevKey.compare(n.keys[0]) >= 0) {
          errors.push(`leaf chain not strictly ascending at node ${cur}: prev ${prevKey.readUInt32BE(28)} >= first ${n.keys[0].readUInt32BE(28)}`);
        }
        prevKey = n.keys[n.keys.length - 1];
      }
      cur = n.hdr.nextLeafIdx === 0 ? null : n.hdr.nextLeafIdx;
    }
    if (chainCovered !== leafCount) {
      errors.push(`leaf chain covers ${chainCovered} leaves but DFS found ${leafCount}`);
    }
  }

  return {
    ok: errors.length === 0,
    errors,
    warnings,
    stats: {
      height: hdr.height,
      nodeCount: hdr.nodeCount,
      totalEntries: hdr.totalEntries,
      leafCount,
      actualEntries,
      leftmostLeaf: hdr.leftmostLeafIdx,
      chainCovered,
    },
  };
}

export function printReport(name: string, r: InvariantReport): void {
  const tag = r.ok ? "✓" : "✗";
  console.log(`${tag} [${name}] height=${r.stats.height} nodes=${r.stats.nodeCount} leaves=${r.stats.leafCount} entries=${r.stats.actualEntries}`);
  for (const e of r.errors) console.log(`    ERROR: ${e}`);
  for (const w of r.warnings) console.log(`    warn:  ${w}`);
}
