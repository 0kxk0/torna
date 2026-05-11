/**
 * Torna end-to-end demo (PDA self-allocation version).
 *
 *   1. Initialize a fresh tree on chain.
 *   2. Insert N entries — program does CPI to system_program to create only
 *      the nodes that splits actually consume. Unused spare PDAs cost nothing.
 *   3. Verify with Find.
 *   4. Range scan a contiguous slice.
 *   5. Print stats + CU benchmark.
 */
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
  Connection,
  Keypair,
  PublicKey,
  SystemProgram,
  Transaction,
  ComputeBudgetProgram,
  sendAndConfirmTransaction,
} from "@solana/web3.js";

import {
  decodeNode,
  decodeTreeHeader,
  deriveNodePda,
  deriveHeaderPda,
  ixInitTree,
  ixInsert,
  ixFind,
  ixRangeScan,
  keyFromU32,
  valueFromU64,
  readReturnData,
  NODE_ACCOUNT_DATA_SIZE,
  TREE_HEADER_SIZE,
  KEYS_PER_NODE_MAX,
  KEY_SIZE,
  VAL_SIZE,
  traverseToLeaf,
} from "./torna.ts";

import { TornaStateStore } from "./state.ts";

/* ---------------- Config ---------------- */

const REPO_ROOT = path.resolve(import.meta.dirname, "..", "..");
const PROGRAM_KP_PATH = path.join(REPO_ROOT, "out", "torna_btree-keypair.json");
const STATE_FILE = path.join(REPO_ROOT, "client", "state", "demo.json");
const RPC_URL = process.env.RPC ?? "https://api.devnet.solana.com";
const ENTRIES = Number(process.env.ENTRIES ?? "30");
const PAYER_PATH = process.env.WALLET ?? path.join(os.homedir(), ".config", "solana", "id.json");

/* ---------------- Helpers ---------------- */

function loadKeypair(p: string): Keypair {
  const data = JSON.parse(fs.readFileSync(p, "utf8"));
  return Keypair.fromSecretKey(Uint8Array.from(data));
}

interface SentTx {
  sig: string;
  logs: string[];
  cuUsed: number;
}

async function sendTx(
  conn: Connection,
  tx: Transaction,
  signers: Keypair[],
  label: string,
): Promise<SentTx> {
  const sig = await sendAndConfirmTransaction(conn, tx, signers, {
    commitment: "confirmed",
    skipPreflight: false,
  });
  const parsed = await conn.getTransaction(sig, {
    commitment: "confirmed",
    maxSupportedTransactionVersion: 0,
  });
  const logs = parsed?.meta?.logMessages ?? [];
  let cuUsed = 0;
  for (const log of logs) {
    const m = log.match(/consumed (\d+) of \d+ compute units/);
    if (m) cuUsed = Math.max(cuUsed, Number(m[1]));
  }
  console.log(`  [${label}] sig=${sig.slice(0, 16)}… cu=${cuUsed}`);
  return { sig, logs, cuUsed };
}

/* ---------------- Demo ---------------- */

async function main() {
  const conn = new Connection(RPC_URL, "confirmed");
  const payer = loadKeypair(PAYER_PATH);
  console.log(`payer:    ${payer.publicKey.toBase58()}`);

  const programKp = loadKeypair(PROGRAM_KP_PATH);
  const programId = programKp.publicKey;
  console.log(`program:  ${programId.toBase58()}`);
  console.log(`rpc:      ${RPC_URL}`);
  console.log(`entries:  ${ENTRIES}`);
  console.log("");

  const progAcc = await conn.getAccountInfo(programId);
  if (!progAcc || !progAcc.executable) {
    console.error("Program not deployed (or not executable). Run:");
    console.error(`  solana program deploy ${PROGRAM_KP_PATH.replace("-keypair.json", ".so")}`);
    process.exit(1);
  }

  /* ---- State ---- */
  const store = new TornaStateStore(STATE_FILE);
  const isFresh = !store.exists();
  if (isFresh) {
    store.programId = programId;
    store.treeId = Math.floor(Math.random() * 0xffffffff);
  } else {
    store.load();
    if (!store.programId.equals(programId)) {
      console.log("state's programId mismatches the built .so — starting fresh");
      fs.unlinkSync(STATE_FILE);
      store.programId = programId;
      store.treeId = Math.floor(Math.random() * 0xffffffff);
    }
  }

  /* ---- Derive header PDA ---- */
  const [headerPda, headerBump] = deriveHeaderPda(programId, store.treeId);
  console.log(`header PDA: ${headerPda.toBase58()} (bump ${headerBump})`);

  /* ---- InitTree ---- */
  const rentHeader = await conn.getMinimumBalanceForRentExemption(TREE_HEADER_SIZE);
  const rentPerNode = await conn.getMinimumBalanceForRentExemption(NODE_ACCOUNT_DATA_SIZE);
  console.log(`rent/header = ${rentHeader} lamports`);
  console.log(`rent/node   = ${rentPerNode} lamports`);

  if (isFresh || !(await conn.getAccountInfo(headerPda))) {
    console.log(`init tree (treeId=${store.treeId})…`);
    const tx = new Transaction().add(
      ixInitTree({
        programId,
        payer: payer.publicKey,
        headerPda,
        treeId: store.treeId,
        headerBump,
        rentLamports: BigInt(rentHeader),
      }),
    );
    await sendTx(conn, tx, [payer], "init_tree");
    store.save();
  }

  /* ---- Insert N entries ---- */
  console.log(`\ninserting ${ENTRIES} entries (KEYS_PER_NODE_MAX=${KEYS_PER_NODE_MAX})…`);
  const cuSamples: number[] = [];
  const splitSamples: number[] = []; // CUs of inserts that triggered any split

  // Insert in interleaved order to exercise both halves of the tree.
  const order: number[] = [];
  for (let i = 0; i < ENTRIES; i++) order.push(i);
  order.sort((a, b) => (a % 2) - (b % 2) || a - b);

  for (const i of order) {
    const key = keyFromU32(i);
    const value = valueFromU64(BigInt(i * 1000));

    const hdrAccount = await conn.getAccountInfo(headerPda);
    const hdr = decodeTreeHeader(hdrAccount!.data);

    /* Path to leaf. */
    let pathIndices: number[] = [];
    if (hdr.height > 0) {
      const { path: p } = await traverseToLeaf(
        conn, programId, headerPda, key,
      );
      pathIndices = p;
    }
    const pathPubkeys = pathIndices.map(
      (idx) => deriveNodePda(programId, hdr.treeId, idx)[0],
    );

    /* Worst-case spares: height + 1 (split propagates all the way + new root).
     * Unused spare PDAs cost nothing — only consumed ones get allocated via CPI. */
    const spareCount = hdr.height === 0 ? 1 : hdr.height + 1;
    const spares: { pubkey: PublicKey; bump: number }[] = [];
    for (let s = 0; s < spareCount; s++) {
      const idx = hdr.nodeCount + 1 + s;
      const [pk, bump] = deriveNodePda(programId, hdr.treeId, idx);
      spares.push({ pubkey: pk, bump });
    }

    const tx = new Transaction()
      .add(ComputeBudgetProgram.setComputeUnitLimit({ units: 400_000 }))
      .add(
        ixInsert({
          programId,
          treeHeader: headerPda,
          payer: payer.publicKey,
          key,
          value,
          rentLamports: BigInt(rentPerNode),
          pathAccounts: pathPubkeys,
          spareAccounts: spares.map((s) => s.pubkey),
          spareBumps: spares.map((s) => s.bump),
        }),
      );

    const r = await sendTx(conn, tx, [payer], `insert ${i}`);
    cuSamples.push(r.cuUsed);

    const hdrAfter = decodeTreeHeader(
      (await conn.getAccountInfo(headerPda))!.data,
    );
    if (hdrAfter.nodeCount > hdr.nodeCount) {
      splitSamples.push(r.cuUsed);
      console.log(`    ↳ height ${hdrAfter.height}  nodes ${hdrAfter.nodeCount}  (+${hdrAfter.nodeCount - hdr.nodeCount} allocated)`);
    }
  }

  /* ---- Stats ---- */
  const finalHdr = decodeTreeHeader(
    (await conn.getAccountInfo(headerPda))!.data,
  );
  console.log("\n=== tree stats ===");
  console.log(`  tree_id           : ${finalHdr.treeId}`);
  console.log(`  height            : ${finalHdr.height}`);
  console.log(`  node_count        : ${finalHdr.nodeCount}`);
  console.log(`  total_entries     : ${finalHdr.totalEntries}`);
  console.log(`  root_node_idx     : ${finalHdr.rootNodeIdx}`);

  const avg = cuSamples.reduce((s, x) => s + x, 0) / cuSamples.length;
  const noSplit = cuSamples.filter((_, i) => !splitSamples.includes(cuSamples[i]));
  console.log("\n=== CU benchmark ===");
  console.log(`  samples           : ${cuSamples.length}`);
  console.log(`  min/avg/max       : ${Math.min(...cuSamples)} / ${avg.toFixed(0)} / ${Math.max(...cuSamples)} CU`);
  console.log(`  splits triggered  : ${splitSamples.length}`);
  if (splitSamples.length)
    console.log(`  split avg/max CU  : ${(splitSamples.reduce((s, x) => s + x, 0) / splitSamples.length).toFixed(0)} / ${Math.max(...splitSamples)}`);

  /* ---- Find ---- */
  console.log("\n=== find spot checks ===");
  for (const i of [0, 1, Math.floor(ENTRIES / 2), ENTRIES - 1]) {
    if (i >= ENTRIES) continue;
    const key = keyFromU32(i);
    const { path: p, treeId } = await traverseToLeaf(
      conn, programId, headerPda, key,
    );
    const pathPks = p.map((idx) => deriveNodePda(programId, treeId, idx)[0]);
    const tx = new Transaction().add(
      ixFind({
        programId,
        treeHeader: headerPda,
        key,
        pathAccounts: pathPks,
      }),
    );
    const r = await sendTx(conn, tx, [payer], `find ${i}`);
    const ret = readReturnData(r.logs);
    if (ret && ret.length >= 1 + VAL_SIZE && ret[0] === 1) {
      const v = Buffer.from(ret.subarray(1, 1 + VAL_SIZE)).readBigUInt64LE(0);
      console.log(`  key=${i.toString().padStart(4)} found, value=${v}`);
    } else {
      console.log(`  key=${i.toString().padStart(4)} NOT FOUND`);
    }
  }

  /* ---- Range scan ---- */
  if (ENTRIES >= 10) {
    const startN = 5;
    const endN = Math.min(ENTRIES - 1, 20);
    console.log(`\n=== range scan: keys [${startN}, ${endN}] ===`);
    const startKey = keyFromU32(startN);
    const endKey = keyFromU32(endN);
    const { path: p, treeId } = await traverseToLeaf(
      conn, programId, headerPda, startKey,
    );
    const pathPks = p.map((idx) => deriveNodePda(programId, treeId, idx)[0]);

    /* Pre-load a few extra leaves following the start leaf via next_leaf chain. */
    const extraChain: PublicKey[] = [];
    {
      const startLeafIdx = p[p.length - 1];
      const startLeafAcc = await conn.getAccountInfo(
        deriveNodePda(programId, treeId, startLeafIdx)[0],
      );
      let next = decodeNode(startLeafAcc!.data).hdr.nextLeafIdx;
      for (let k = 0; k < 6 && next !== 0; k++) {
        const [pk] = deriveNodePda(programId, treeId, next);
        extraChain.push(pk);
        const acc = await conn.getAccountInfo(pk);
        if (!acc) break;
        next = decodeNode(acc.data).hdr.nextLeafIdx;
      }
    }

    const tx = new Transaction().add(
      ixRangeScan({
        programId,
        treeHeader: headerPda,
        startKey,
        endKey,
        pathAccounts: pathPks,
        chainAccounts: extraChain,
        maxResults: 32,
      }),
    );
    const r = await sendTx(conn, tx, [payer], "range_scan");
    const ret = readReturnData(r.logs);
    if (ret && ret.length >= 2) {
      const count = ret[0] | (ret[1] << 8);
      console.log(`  ${count} results:`);
      for (let i = 0; i < count; i++) {
        const off = 2 + i * (KEY_SIZE + VAL_SIZE);
        const k = ret.readUInt32BE(off + KEY_SIZE - 4);
        const v = ret.readBigUInt64LE(off + KEY_SIZE);
        console.log(`    key=${k} value=${v}`);
      }
    }
  }

  console.log(`\nexplorer: https://explorer.solana.com/address/${headerPda.toBase58()}?cluster=devnet`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
