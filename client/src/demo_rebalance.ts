/**
 * Rebalance demo: trigger IX_DELETE's borrow/merge path on a multi-leaf tree.
 *
 * Requires the bench_parallel state (a tree with multiple leaves). Run:
 *   SETUP_N=200 npx tsx src/bench_parallel.ts   # builds the tree
 *   npx tsx src/demo_rebalance.ts               # this script
 *
 * Strategy:
 *   1. Inspect tree to find a leaf with the most entries and a non-empty
 *      adjacent sibling.
 *   2. Delete keys via IX_DELETE_FAST until the leaf is at MIN + 1 entries.
 *   3. Issue IX_DELETE with the sibling provided — should trigger borrow
 *      (if sibling has > MIN) or merge.
 *   4. Re-inspect; report what happened.
 */
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
  Connection,
  Keypair,
  PublicKey,
  Transaction,
  sendAndConfirmTransaction,
} from "@solana/web3.js";

import {
  decodeNode,
  decodeTreeHeader,
  deriveNodePda,
  deriveHeaderPda,
  ixDeleteFast,
  ixDelete,
  ixFind,
  keyFromU32,
  readReturnData,
  KEYS_PER_NODE_MIN,
  VAL_SIZE,
  traverseToLeaf,
} from "./torna.ts";

import { TornaStateStore } from "./state.ts";

const REPO_ROOT = path.resolve(import.meta.dirname, "..", "..");
const PROGRAM_KP_PATH = path.join(REPO_ROOT, "out", "torna_btree-keypair.json");
const STATE_FILE = path.join(REPO_ROOT, "client", "state", "bench.json");
const RPC_URL = process.env.RPC ?? "https://api.devnet.solana.com";
const PAYER_PATH = process.env.WALLET ?? path.join(os.homedir(), ".config", "solana", "id.json");

function loadKeypair(p: string): Keypair {
  return Keypair.fromSecretKey(Uint8Array.from(JSON.parse(fs.readFileSync(p, "utf8"))));
}

async function sendTx(conn: Connection, tx: Transaction, signers: Keypair[], label: string) {
  const sig = await sendAndConfirmTransaction(conn, tx, signers, {
    commitment: "confirmed",
    skipPreflight: false,
  });
  const parsed = await conn.getTransaction(sig, {
    commitment: "confirmed",
    maxSupportedTransactionVersion: 0,
  });
  const logs = parsed?.meta?.logMessages ?? [];
  let cu = 0;
  for (const l of logs) {
    const m = l.match(/consumed (\d+) of \d+ compute units/);
    if (m) cu = Math.max(cu, Number(m[1]));
  }
  console.log(`  [${label}] sig=${sig.slice(0, 12)}… cu=${cu}`);
  return { sig, logs, cuUsed: cu };
}

async function main() {
  const conn = new Connection(RPC_URL, "confirmed");
  const payer = loadKeypair(PAYER_PATH);
  const programId = loadKeypair(PROGRAM_KP_PATH).publicKey;
  const store = new TornaStateStore(STATE_FILE);
  if (!store.exists()) {
    console.error("No bench state. Run `SETUP_N=200 tsx src/bench_parallel.ts` first.");
    process.exit(1);
  }
  store.load();
  const [headerPda] = deriveHeaderPda(programId, store.treeId);

  /* ---- Discover tree shape ---- */
  const hdrAcc = await conn.getAccountInfo(headerPda);
  const hdr = decodeTreeHeader(hdrAcc!.data);
  console.log(`tree: height=${hdr.height} nodes=${hdr.nodeCount} entries=${hdr.totalEntries} (MIN=${KEYS_PER_NODE_MIN})`);
  if (hdr.height < 2) {
    console.error("Tree height < 2 — no internal node to test rebalance against.");
    process.exit(1);
  }

  const [rootPk] = deriveNodePda(programId, hdr.treeId, hdr.rootNodeIdx);
  const rootAcc = await conn.getAccountInfo(rootPk);
  const root = decodeNode(rootAcc!.data);
  if (!root.children) {
    console.error("Root is leaf, can't test.");
    process.exit(1);
  }

  /* Print each leaf's status. */
  console.log("\nleaf states:");
  const leafInfo: { idx: number; pk: PublicKey; keyCount: number; first: number; last: number }[] = [];
  for (let i = 0; i < root.children.length; i++) {
    const ci = root.children[i];
    const [pk] = deriveNodePda(programId, hdr.treeId, ci);
    const acc = await conn.getAccountInfo(pk);
    if (!acc) continue;
    const n = decodeNode(acc.data);
    const first = n.keys.length ? n.keys[0].readUInt32BE(28) : -1;
    const last = n.keys.length ? n.keys[n.keys.length - 1].readUInt32BE(28) : -1;
    leafInfo.push({ idx: ci, pk, keyCount: n.hdr.keyCount, first, last });
    console.log(`  leaf #${ci}  count=${n.hdr.keyCount}  range=[${first}, ${last}]`);
  }

  /* Pick a leaf with the MOST entries (target for deletion-down-to-underflow). */
  leafInfo.sort((a, b) => b.keyCount - a.keyCount);
  const target = leafInfo[0];
  if (!target || target.keyCount <= KEYS_PER_NODE_MIN) {
    console.log("\nNo leaf has > MIN entries; can't demonstrate underflow trigger cleanly.");
    process.exit(0);
  }
  console.log(`\ntarget leaf: #${target.idx} (count=${target.keyCount}, range [${target.first}, ${target.last}])`);

  /* Determine sibling: prefer the leaf immediately to the right in the root's children array. */
  const idxInRoot = root.children.indexOf(target.idx);
  let siblingPos: 1 | 2;
  let siblingInfo: typeof leafInfo[number] | undefined;
  if (idxInRoot < root.children.length - 1) {
    const rightIdx = root.children[idxInRoot + 1];
    siblingInfo = leafInfo.find((l) => l.idx === rightIdx);
    siblingPos = 1;
  } else {
    const leftIdx = root.children[idxInRoot - 1];
    siblingInfo = leafInfo.find((l) => l.idx === leftIdx);
    siblingPos = 2;
  }
  if (!siblingInfo) {
    console.error("Couldn't resolve sibling.");
    process.exit(1);
  }
  console.log(`sibling:     #${siblingInfo.idx} (side=${siblingPos === 1 ? "right" : "left"}, count=${siblingInfo.keyCount})`);

  /* ---- Phase 1: shrink target leaf down to MIN via DELETE_FAST ---- */
  console.log("\n--- Phase 1: shrink target via DELETE_FAST ---");
  const targetAcc = await conn.getAccountInfo(target.pk);
  const targetNode = decodeNode(targetAcc!.data);
  let toDelete = target.keyCount - KEYS_PER_NODE_MIN;
  // Pick the LAST `toDelete` keys (so first key of leaf stays stable for parent separator).
  const victimKeys = targetNode.keys
    .slice(-toDelete)
    .map((k) => k.readUInt32BE(28));
  console.log(`  will delete ${toDelete} keys: ${victimKeys.slice(0, 8).join(", ")}${victimKeys.length > 8 ? "…" : ""}`);

  for (const v of victimKeys) {
    const key = keyFromU32(v);
    const { path: p } = await traverseToLeaf(conn, programId, headerPda, key);
    const pathPks = p.map((idx) => deriveNodePda(programId, hdr.treeId, idx)[0]);
    const tx = new Transaction().add(
      ixDeleteFast({ programId, treeHeader: headerPda, authority: payer.publicKey, key, pathAccounts: pathPks }),
    );
    await sendTx(conn, tx, [payer], `del_fast ${v}`);
  }

  /* Verify state */
  {
    const acc = await conn.getAccountInfo(target.pk);
    const n = decodeNode(acc!.data);
    console.log(`  target leaf now has ${n.hdr.keyCount} entries (MIN = ${KEYS_PER_NODE_MIN})`);
    console.log(`  next delete will push below MIN → rebalance must fire`);
  }

  /* ---- Phase 2: trigger underflow via IX_DELETE with sibling ---- */
  console.log("\n--- Phase 2: trigger underflow via IX_DELETE ---");
  const remainingKeys = (await conn.getAccountInfo(target.pk).then((a) => decodeNode(a!.data).keys)).map(
    (k) => k.readUInt32BE(28),
  );
  const trigger = remainingKeys[Math.floor(remainingKeys.length / 2)];
  console.log(`  underflow trigger key: ${trigger}`);

  const triggerKey = keyFromU32(trigger);
  const { path: p } = await traverseToLeaf(conn, programId, headerPda, triggerKey);
  const pathPks = p.map((idx) => deriveNodePda(programId, hdr.treeId, idx)[0]);

  const tx = new Transaction().add(
    ixDelete({
      programId,
      treeHeader: headerPda,
      payer: payer.publicKey,
      key: triggerKey,
      pathAccounts: pathPks,
      siblings: [
        {
          level: pathPks.length - 1, // leaf level
          side: siblingPos === 1 ? "right" : "left",
          pubkey: siblingInfo.pk,
        },
      ],
    }),
  );
  await sendTx(conn, tx, [payer], `delete+rebalance`);

  /* ---- Re-inspect ---- */
  console.log("\n--- post-rebalance ---");
  const newHdr = decodeTreeHeader((await conn.getAccountInfo(headerPda))!.data);
  console.log(`  tree: height=${newHdr.height} nodes=${newHdr.nodeCount} entries=${newHdr.totalEntries}`);

  const newRootAcc = await conn.getAccountInfo(rootPk);
  const newRoot = decodeNode(newRootAcc!.data);
  console.log(`  root key_count=${newRoot.hdr.keyCount}  children=${newRoot.children!.join(", ")}`);

  for (const li of leafInfo) {
    const acc = await conn.getAccountInfo(li.pk);
    if (!acc || acc.lamports === 0) {
      console.log(`  leaf #${li.idx}  MERGED/FREED (account closed)`);
      continue;
    }
    const n = decodeNode(acc.data);
    if (!n.hdr.initialized) {
      console.log(`  leaf #${li.idx}  uninitialized (merged → freed)`);
      continue;
    }
    const first = n.keys.length ? n.keys[0].readUInt32BE(28) : -1;
    const last = n.keys.length ? n.keys[n.keys.length - 1].readUInt32BE(28) : -1;
    const delta = n.hdr.keyCount - li.keyCount;
    console.log(`  leaf #${li.idx}  count=${n.hdr.keyCount} (${delta >= 0 ? "+" : ""}${delta})  range=[${first}, ${last}]`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
