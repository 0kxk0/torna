/**
 * Delete demo: prove the delete + rebalance path.
 *
 *   1. Reuse an existing tree (from demo.ts state).
 *   2. Delete some keys via IX_DELETE_FAST (no rebalance).
 *   3. Verify not found via IX_FIND.
 *   4. Re-insert one of them and re-verify.
 *   5. (Optional) trigger a rebalance via IX_DELETE with sibling provided.
 */
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
  Connection,
  Keypair,
  PublicKey,
  Transaction,
  ComputeBudgetProgram,
  SystemProgram,
  sendAndConfirmTransaction,
} from "@solana/web3.js";

import {
  decodeNode,
  decodeTreeHeader,
  deriveNodePda,
  deriveHeaderPda,
  ixInsert,
  ixFind,
  ixDeleteFast,
  ixDelete,
  keyFromU32,
  valueFromU64,
  readReturnData,
  NODE_ACCOUNT_DATA_SIZE,
  KEY_SIZE,
  VAL_SIZE,
  traverseToLeaf,
} from "./torna.ts";

import { TornaStateStore } from "./state.ts";

const REPO_ROOT = path.resolve(import.meta.dirname, "..", "..");
const PROGRAM_KP_PATH = path.join(REPO_ROOT, "out", "torna_btree-keypair.json");
const STATE_FILE = path.join(REPO_ROOT, "client", "state", "demo.json");
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
    console.error("No demo state found. Run `tsx src/demo.ts` first to populate.");
    process.exit(1);
  }
  store.load();
  if (!store.programId.equals(programId)) {
    console.error("Demo state programId mismatch — re-run demo.ts to rebuild.");
    process.exit(1);
  }

  const [headerPda] = deriveHeaderPda(programId, store.treeId);
  const hdr0Acc = await conn.getAccountInfo(headerPda);
  if (!hdr0Acc) {
    console.error("Header not found on chain.");
    process.exit(1);
  }
  const hdr0 = decodeTreeHeader(hdr0Acc.data);
  console.log(`tree: height=${hdr0.height} nodes=${hdr0.nodeCount} entries=${hdr0.totalEntries}`);

  /* ---- DELETE FAST ---- */
  console.log("\n=== DELETE_FAST (no rebalance, header RO) ===");
  const victims = [0, 5, 17];
  for (const v of victims) {
    if (v >= Number(hdr0.totalEntries)) continue;
    const key = keyFromU32(v);
    const { path: p } = await traverseToLeaf(conn, programId, headerPda, key);
    const pathPks = p.map((idx) => deriveNodePda(programId, hdr0.treeId, idx)[0]);
    const tx = new Transaction().add(
      ixDeleteFast({
        programId,
        treeHeader: headerPda,
        authority: payer.publicKey,
        key,
        pathAccounts: pathPks,
      }),
    );
    const r = await sendTx(conn, tx, [payer], `delete_fast ${v}`);
    const ret = readReturnData(r.logs);
    if (ret && ret[0] === 1) {
      const val = Buffer.from(ret.subarray(1, 1 + VAL_SIZE)).readBigUInt64LE(0);
      console.log(`    deleted key=${v} value=${val}`);
    } else {
      console.log(`    key=${v} not found`);
    }
  }

  /* ---- Verify deleted keys are gone ---- */
  console.log("\n=== verify deletions ===");
  for (const v of victims) {
    const key = keyFromU32(v);
    const { path: p } = await traverseToLeaf(conn, programId, headerPda, key);
    const pathPks = p.map((idx) => deriveNodePda(programId, hdr0.treeId, idx)[0]);
    const tx = new Transaction().add(
      ixFind({
        programId,
        treeHeader: headerPda,
        key,
        pathAccounts: pathPks,
      }),
    );
    const r = await sendTx(conn, tx, [payer], `find ${v}`);
    const ret = readReturnData(r.logs);
    if (ret && ret[0] === 1) {
      console.log(`    ✗ key=${v} STILL EXISTS — delete failed`);
    } else {
      console.log(`    ✓ key=${v} not found (correctly deleted)`);
    }
  }

  /* ---- Re-insert one and verify ---- */
  console.log("\n=== re-insert + verify ===");
  {
    const v = victims[0];
    const key = keyFromU32(v);
    const value = valueFromU64(BigInt(v * 1000 + 777)); // marker value to confirm new insert
    const hdrAccount = await conn.getAccountInfo(headerPda);
    const hdr = decodeTreeHeader(hdrAccount!.data);
    const { path: p } = await traverseToLeaf(conn, programId, headerPda, key);
    const pathPks = p.map((idx) => deriveNodePda(programId, hdr.treeId, idx)[0]);
    const spareCount = hdr.height === 0 ? 1 : hdr.height + 1;
    const spares: { pubkey: PublicKey; bump: number }[] = [];
    for (let s = 0; s < spareCount; s++) {
      const [pk, bump] = deriveNodePda(programId, hdr.treeId, hdr.nodeCount + 1 + s);
      spares.push({ pubkey: pk, bump });
    }
    const rentPerNode = await conn.getMinimumBalanceForRentExemption(NODE_ACCOUNT_DATA_SIZE);
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
          pathAccounts: pathPks,
          spareAccounts: spares.map((s) => s.pubkey),
          spareBumps: spares.map((s) => s.bump),
        }),
      );
    await sendTx(conn, tx, [payer], `re-insert ${v}`);

    const tx2 = new Transaction().add(
      ixFind({ programId, treeHeader: headerPda, key, pathAccounts: pathPks }),
    );
    const r2 = await sendTx(conn, tx2, [payer], `verify-find ${v}`);
    const ret = readReturnData(r2.logs);
    if (ret && ret[0] === 1) {
      const val = Buffer.from(ret.subarray(1, 1 + VAL_SIZE)).readBigUInt64LE(0);
      console.log(`    ✓ key=${v} re-inserted with value=${val} (expected ${v * 1000 + 777})`);
    } else {
      console.log(`    ✗ re-insert failed`);
    }
  }

  /* ---- Final stats ---- */
  const hdrFinal = decodeTreeHeader((await conn.getAccountInfo(headerPda))!.data);
  console.log("\n=== final stats ===");
  console.log(`  height: ${hdrFinal.height}  nodes: ${hdrFinal.nodeCount}  entries: ${hdrFinal.totalEntries}`);
  console.log(`  explorer: https://explorer.solana.com/address/${headerPda.toBase58()}?cluster=devnet`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
