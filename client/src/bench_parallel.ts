/**
 * Parallelism benchmark for Torna.
 *
 *   1. Build a tree by inserting N keys with stride 1000 (0, 1000, 2000, …).
 *      This produces a multi-leaf tree where each leaf covers a wide u32 range
 *      with gaps between existing keys — so we can insert NEW keys later that
 *      land in specific leaves without colliding.
 *   2. Discover the leaves' separator boundaries by reading the tree's
 *      internal root.
 *   3. Generate K test keys, one per leaf (pick a u32 inside each leaf's range
 *      that's not an existing key).
 *   4. Send all K inserts in parallel via IX_INSERT_FAST and time it.
 *   5. Send same K inserts sequentially as a control. Compare.
 *
 *   Expected: parallel ≈ ~1 slot of wall time (because write sets are
 *   disjoint — header read-only, each insert writes a different leaf).
 *   Sequential ≈ K slots.
 *
 *   Usage:  npx tsx src/bench_parallel.ts
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
  TransactionConfirmationStrategy,
} from "@solana/web3.js";

import {
  decodeNode,
  decodeTreeHeader,
  deriveNodePda,
  deriveHeaderPda,
  ixInitTree,
  ixInsert,
  ixInsertFast,
  keyFromU32,
  valueFromU64,
  NODE_ACCOUNT_DATA_SIZE,
  TREE_HEADER_SIZE,
  KEYS_PER_NODE_MAX,
  traverseToLeaf,
} from "./torna.ts";

import { TornaStateStore } from "./state.ts";

/* ---------------- Config ---------------- */

const REPO_ROOT = path.resolve(import.meta.dirname, "..", "..");
const PROGRAM_KP_PATH = path.join(REPO_ROOT, "out", "torna_btree-keypair.json");
const STATE_FILE = path.join(REPO_ROOT, "client", "state", "bench.json");
const RPC_URL = process.env.RPC ?? "https://api.devnet.solana.com";
const SETUP_N = Number(process.env.SETUP_N ?? "200");
const STRIDE = 1000;
const PAYER_PATH = process.env.WALLET ?? path.join(os.homedir(), ".config", "solana", "id.json");

function loadKeypair(p: string): Keypair {
  return Keypair.fromSecretKey(Uint8Array.from(JSON.parse(fs.readFileSync(p, "utf8"))));
}

interface SentTx {
  sig: string;
  slot: number | null;
  cuUsed: number;
}

async function sendAndTrack(
  conn: Connection,
  tx: Transaction,
  signers: Keypair[],
  label: string,
  noisy = true,
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
  if (noisy) console.log(`    [${label}] sig=${sig.slice(0, 12)}… slot=${parsed?.slot} cu=${cuUsed}`);
  return { sig, slot: parsed?.slot ?? null, cuUsed };
}

/* ---------------- Setup: build a sparse-key tree ---------------- */

async function setupTree(
  conn: Connection,
  payer: Keypair,
  programId: PublicKey,
  store: TornaStateStore,
  headerPda: PublicKey,
  headerBump: number,
): Promise<void> {
  const rentHeader = await conn.getMinimumBalanceForRentExemption(TREE_HEADER_SIZE);
  const rentPerNode = await conn.getMinimumBalanceForRentExemption(NODE_ACCOUNT_DATA_SIZE);

  if (!(await conn.getAccountInfo(headerPda))) {
    console.log(`  init_tree @ ${headerPda.toBase58()} (treeId=${store.treeId})`);
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
    await sendAndTrack(conn, tx, [payer], "init_tree", false);
    store.save();
  }

  const hdrAccount = await conn.getAccountInfo(headerPda);
  const hdr0 = decodeTreeHeader(hdrAccount!.data);
  console.log(`  starting from total_entries=${hdr0.totalEntries}`);

  if (hdr0.totalEntries >= BigInt(SETUP_N)) {
    console.log("  tree already at target size; skipping setup inserts");
    return;
  }

  console.log(`  inserting ${SETUP_N} sparse keys (stride=${STRIDE})…`);
  for (let i = Number(hdr0.totalEntries); i < SETUP_N; i++) {
    const key = keyFromU32(i * STRIDE);
    const value = valueFromU64(BigInt(i));

    const hdrAccount = await conn.getAccountInfo(headerPda);
    const hdr = decodeTreeHeader(hdrAccount!.data);

    let pathIndices: number[] = [];
    if (hdr.height > 0) {
      const { path: p } = await traverseToLeaf(conn, programId, headerPda, key);
      pathIndices = p;
    }
    const pathPubkeys = pathIndices.map((idx) => deriveNodePda(programId, hdr.treeId, idx)[0]);

    const spareCount = hdr.height === 0 ? 1 : hdr.height + 1;
    const spares: { pubkey: PublicKey; bump: number }[] = [];
    for (let s = 0; s < spareCount; s++) {
      const [pk, bump] = deriveNodePda(programId, hdr.treeId, hdr.nodeCount + 1 + s);
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
    await sendAndTrack(conn, tx, [payer], `setup ${i}`, false);
    if (i % 25 === 0) console.log(`    progress: ${i}/${SETUP_N}`);
  }
  console.log("  setup complete");
}

/* ---------------- Discover leaves + pick bench keys ---------------- */

async function pickBenchKeys(
  conn: Connection,
  programId: PublicKey,
  treeHeader: PublicKey,
): Promise<{ probes: { keyVal: number; leafIdx: number }[]; rangeHints: string }> {
  const hdrAcc = await conn.getAccountInfo(treeHeader);
  const hdr = decodeTreeHeader(hdrAcc!.data);

  if (hdr.height < 2) {
    throw new Error(`tree height ${hdr.height} too shallow for parallelism demo`);
  }

  // Read root, list its children = leaves (assuming height==2)
  const [rootPk] = deriveNodePda(programId, hdr.treeId, hdr.rootNodeIdx);
  const rootAcc = await conn.getAccountInfo(rootPk);
  const root = decodeNode(rootAcc!.data);

  if (!root.children) throw new Error("root is not internal");

  // Read each leaf to find a gap inside its range
  // Leaf 0 covers (-inf, root.keys[0]) → use root.keys[0] - STRIDE/2
  // Leaf i covers [root.keys[i-1], root.keys[i]) → use root.keys[i-1] + STRIDE/2
  // Last leaf covers [root.keys[last], +inf) → use root.keys[last] + STRIDE/2
  const probes: { keyVal: number; leafIdx: number }[] = [];
  const rangeStrs: string[] = [];

  // Decode separator keys as u32
  const sepVals = root.keys.map((k) => k.readUInt32BE(28));

  for (let i = 0; i <= sepVals.length; i++) {
    let probeVal: number;
    if (i === 0) {
      probeVal = Math.max(1, sepVals[0] - STRIDE / 2);
      rangeStrs.push(`leaf #${root.children[i]} (range < ${sepVals[0]})`);
    } else if (i === sepVals.length) {
      probeVal = sepVals[sepVals.length - 1] + STRIDE / 2;
      rangeStrs.push(`leaf #${root.children[i]} (range ≥ ${sepVals[sepVals.length - 1]})`);
    } else {
      probeVal = sepVals[i - 1] + STRIDE / 2;
      rangeStrs.push(`leaf #${root.children[i]} (range [${sepVals[i - 1]}, ${sepVals[i]}))`);
    }
    probes.push({ keyVal: probeVal, leafIdx: root.children[i] });
  }

  return { probes, rangeHints: rangeStrs.join(", ") };
}

/* ---------------- Benchmark ---------------- */

async function buildFastInsertTx(
  conn: Connection,
  programId: PublicKey,
  treeHeader: PublicKey,
  authority: PublicKey,
  keyVal: number,
  serial: number,
): Promise<Transaction> {
  const key = keyFromU32(keyVal);
  const value = valueFromU64(BigInt(serial));

  const { path: p, treeId } = await traverseToLeaf(conn, programId, treeHeader, key);
  const pathPubkeys = p.map((idx) => deriveNodePda(programId, treeId, idx)[0]);

  return new Transaction()
    .add(ComputeBudgetProgram.setComputeUnitLimit({ units: 100_000 }))
    .add(
      ixInsertFast({
        programId,
        treeHeader,
        authority,
        key,
        value,
        pathAccounts: pathPubkeys,
      }),
    );
}

async function runParallel(
  conn: Connection,
  programId: PublicKey,
  payer: Keypair,
  treeHeader: PublicKey,
  keys: number[],
): Promise<{ wallMs: number; results: SentTx[] }> {
  // Pre-build all txs so the timing measures only network/scheduler latency.
  const txs = await Promise.all(
    keys.map((k, i) => buildFastInsertTx(conn, programId, treeHeader, payer.publicKey, k, 90000 + i)),
  );
  // Fetch a single recent blockhash and use it for all txs
  const { blockhash, lastValidBlockHeight } = await conn.getLatestBlockhash("confirmed");
  for (const tx of txs) {
    tx.recentBlockhash = blockhash;
    tx.lastValidBlockHeight = lastValidBlockHeight;
    tx.feePayer = payer.publicKey;
    tx.sign(payer);
  }

  const start = Date.now();
  const results = await Promise.all(
    txs.map(async (tx, i) => {
      const raw = tx.serialize();
      const sig = await conn.sendRawTransaction(raw, { skipPreflight: false });
      await conn.confirmTransaction(
        { signature: sig, blockhash, lastValidBlockHeight } as TransactionConfirmationStrategy,
        "confirmed",
      );
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
      return { sig, slot: parsed?.slot ?? null, cuUsed };
    }),
  );
  const wallMs = Date.now() - start;
  return { wallMs, results };
}

async function runSerial(
  conn: Connection,
  programId: PublicKey,
  payer: Keypair,
  treeHeader: PublicKey,
  keys: number[],
): Promise<{ wallMs: number; results: SentTx[] }> {
  const results: SentTx[] = [];
  const start = Date.now();
  for (let i = 0; i < keys.length; i++) {
    const tx = await buildFastInsertTx(conn, programId, treeHeader, payer.publicKey, keys[i], 80000 + i);
    const r = await sendAndTrack(conn, tx, [payer], `serial ${i}`, false);
    results.push(r);
  }
  return { wallMs: Date.now() - start, results };
}

/* ---------------- Main ---------------- */

async function main() {
  const conn = new Connection(RPC_URL, "confirmed");
  const payer = loadKeypair(PAYER_PATH);
  const programId = loadKeypair(PROGRAM_KP_PATH).publicKey;
  console.log(`payer:   ${payer.publicKey.toBase58()}`);
  console.log(`program: ${programId.toBase58()}`);
  console.log(`rpc:     ${RPC_URL}`);

  const store = new TornaStateStore(STATE_FILE);
  if (!store.exists()) {
    store.programId = programId;
    store.treeId = Math.floor(Math.random() * 0xffffffff);
  } else {
    store.load();
    if (!store.programId.equals(programId)) {
      fs.unlinkSync(STATE_FILE);
      store.programId = programId;
      store.treeId = Math.floor(Math.random() * 0xffffffff);
    }
  }

  const [headerPda, headerBump] = deriveHeaderPda(programId, store.treeId);
  console.log(`header PDA: ${headerPda.toBase58()} (bump ${headerBump})`);

  /* Setup */
  console.log("\n--- setup ---");
  await setupTree(conn, payer, programId, store, headerPda, headerBump);

  /* Pick bench keys */
  console.log("\n--- discovery ---");
  const { probes, rangeHints } = await pickBenchKeys(conn, programId, headerPda);
  console.log(`  leaves: ${rangeHints}`);
  console.log(`  probe keys (u32 values): ${probes.map((p) => p.keyVal).join(", ")}`);
  const benchKeys = probes.map((p) => p.keyVal);

  /* Warm-up */
  console.log("\n--- warm-up ---");
  await buildFastInsertTx(conn, programId, headerPda, payer.publicKey, benchKeys[0], 70000);

  /* SERIAL */
  console.log("\n--- SERIAL (k=" + benchKeys.length + ") ---");
  const serialKeys = benchKeys.map((k) => k + 11);
  const serial = await runSerial(conn, programId, payer, headerPda, serialKeys);
  console.log(`  total wall: ${serial.wallMs}ms`);
  const sSlots = new Set(serial.results.map((r) => r.slot));
  console.log(`  distinct slots: ${sSlots.size}  slots: ${[...sSlots].join(", ")}`);

  /* PARALLEL */
  console.log("\n--- PARALLEL (k=" + benchKeys.length + ") ---");
  const parKeys = benchKeys.map((k) => k + 23);
  const parallel = await runParallel(conn, programId, payer, headerPda, parKeys);
  console.log(`  total wall: ${parallel.wallMs}ms`);
  const pSlots = new Set(parallel.results.map((r) => r.slot));
  console.log(`  distinct slots: ${pSlots.size}  slots: ${[...pSlots].join(", ")}`);
  for (const r of parallel.results) {
    console.log(`    sig=${r.sig.slice(0, 12)}… slot=${r.slot} cu=${r.cuUsed}`);
  }

  /* Verdict */
  console.log("\n=== summary ===");
  console.log(`  serial   wall: ${serial.wallMs}ms  slots: ${[...sSlots].sort().join(", ")}`);
  console.log(`  parallel wall: ${parallel.wallMs}ms  slots: ${[...pSlots].sort().join(", ")}`);
  const speedup = serial.wallMs / parallel.wallMs;
  console.log(`  parallel speedup: ${speedup.toFixed(2)}×`);
  console.log(`  parallel slot density: ${(benchKeys.length / pSlots.size).toFixed(2)} tx/slot`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
