/**
 * Demo: multi-authority delegates (v3.2).
 *
 *   1. Initialize a fresh tree with the local wallet as primary authority.
 *   2. Generate a delegate keypair (separate from primary).
 *   3. Call IX_ADD_DELEGATE — primary signs.
 *   4. Insert a key signed by the DELEGATE (not primary), with the delegate
 *      account included in the tx so the program can validate the relationship.
 *   5. Call IX_REMOVE_DELEGATE — primary signs.
 *   6. Try the same insert again with the (now-removed) delegate → expect failure.
 *
 *   Requires devnet SOL on the primary + a small airdrop to the delegate
 *   (for tx fees only — the primary still pays for any node rent).
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
  decodeDelegateAccount,
  decodeTreeHeader,
  deriveDelegatePda,
  deriveHeaderPda,
  deriveNodePda,
  ixInitTree,
  ixInsertFast,
  ixAddDelegate,
  ixRemoveDelegate,
  withDelegate,
  keyFromU32,
  valueFromU64,
  NODE_ACCOUNT_DATA_SIZE,
  TREE_HEADER_SIZE,
  DELEGATE_ACCT_SIZE,
  traverseToLeaf,
} from "./torna.ts";

const REPO_ROOT = path.resolve(import.meta.dirname, "..", "..");
const PROGRAM_KP_PATH = path.join(REPO_ROOT, "out", "torna_btree-keypair.json");
const RPC_URL = process.env.RPC ?? "https://api.devnet.solana.com";
const PAYER_PATH = process.env.WALLET ?? path.join(os.homedir(), ".config", "solana", "id.json");

function loadKeypair(p: string): Keypair {
  return Keypair.fromSecretKey(Uint8Array.from(JSON.parse(fs.readFileSync(p, "utf8"))));
}

async function send(
  conn: Connection,
  tx: Transaction,
  signers: Keypair[],
  label: string,
  expectFail = false,
): Promise<{ ok: boolean; cu: number; sig?: string }> {
  try {
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
    console.log(`  [${label}] sig=${sig.slice(0, 12)}… cu=${cu}${expectFail ? " (UNEXPECTED SUCCESS!)" : ""}`);
    return { ok: !expectFail, cu, sig };
  } catch (e) {
    const err = e as Error & { logs?: string[]; transactionLogs?: string[] };
    const msg = err.message.split("\n")[0];
    console.log(`  [${label}] ${expectFail ? "✓ failed as expected" : "ERR"}: ${msg.slice(0, 200)}`);
    const logs = err.logs ?? err.transactionLogs;
    if (!expectFail && logs) {
      for (const l of logs) console.log(`      | ${l}`);
    }
    return { ok: expectFail, cu: 0 };
  }
}

async function main() {
  const conn = new Connection(RPC_URL, "confirmed");
  const primary = loadKeypair(PAYER_PATH);
  const programId = loadKeypair(PROGRAM_KP_PATH).publicKey;
  const delegate = Keypair.generate();

  console.log(`primary:  ${primary.publicKey.toBase58()}`);
  console.log(`delegate: ${delegate.publicKey.toBase58()}`);

  /* Fund the delegate enough to pay tx fees. */
  {
    const tx = new Transaction().add(
      SystemProgram.transfer({
        fromPubkey: primary.publicKey,
        toPubkey: delegate.publicKey,
        lamports: 50_000_000, // 0.05 SOL — plenty for a few txs
      }),
    );
    await sendAndConfirmTransaction(conn, tx, [primary]);
    console.log(`  funded delegate with 0.05 SOL`);
  }

  /* Create a fresh tree with a random tree_id. */
  const treeId = Math.floor(Math.random() * 0xffffffff);
  const [headerPda, headerBump] = deriveHeaderPda(programId, treeId);
  const [delegatePda, delegateBump] = deriveDelegatePda(programId, treeId);
  console.log(`treeId:   ${treeId}`);
  console.log(`header:   ${headerPda.toBase58()}`);
  console.log(`delegate account: ${delegatePda.toBase58()}`);

  const rentHeader = await conn.getMinimumBalanceForRentExemption(TREE_HEADER_SIZE);
  const rentDelegate = await conn.getMinimumBalanceForRentExemption(DELEGATE_ACCT_SIZE);
  const rentNode = await conn.getMinimumBalanceForRentExemption(NODE_ACCOUNT_DATA_SIZE);

  /* ---- 1. InitTree ---- */
  console.log("\n=== init_tree ===");
  await send(
    conn,
    new Transaction().add(
      ixInitTree({
        programId,
        payer: primary.publicKey,
        headerPda,
        treeId,
        headerBump,
        rentLamports: BigInt(rentHeader),
      }),
    ),
    [primary],
    "init_tree",
  );

  /* ---- 2. Add the delegate ---- */
  console.log("\n=== add_delegate ===");
  await send(
    conn,
    new Transaction().add(
      ixAddDelegate({
        programId,
        treeHeader: headerPda,
        primaryAuthority: primary.publicKey,
        delegatePda,
        delegate: delegate.publicKey,
        delegateBump,
        rentLamports: BigInt(rentDelegate),
      }),
    ),
    [primary],
    "add_delegate",
  );

  /* Verify by reading the delegate account. */
  const delegateAcc = await conn.getAccountInfo(delegatePda);
  if (!delegateAcc) {
    console.log("  ✗ delegate account not on chain");
    process.exit(1);
  }
  const decoded = decodeDelegateAccount(delegateAcc.data);
  console.log(`  delegate account: count=${decoded.count} bump=${decoded.bump}`);
  console.log(`  registered delegates: ${decoded.delegates.map((p) => p.toBase58()).join(", ")}`);

  /* ---- 3. Insert as delegate (NOT primary). First insert is special — we need
   *       a spare for the leaf via the full Insert ix. That requires payer
   *       + sysprog. The primary is still the payer for system-program calls
   *       (they're funding rent), but we ALSO want the delegate to be the
   *       authorizing signer. Easiest path: use the FAST insert after the
   *       first leaf is in place. So: do one primary insert to seed the tree,
   *       THEN test the delegate insert via FAST. */
  console.log("\n=== seed: first insert with primary (creates the leaf root) ===");
  {
    // We need to do a full Insert because the tree is empty.
    const { ixInsert } = await import("./torna.ts");
    const [spareNode0, spareBump0] = deriveNodePda(programId, treeId, 1);
    const tx = new Transaction()
      .add(ComputeBudgetProgram.setComputeUnitLimit({ units: 400_000 }))
      .add(
        ixInsert({
          programId,
          treeHeader: headerPda,
          payer: primary.publicKey,
          key: keyFromU32(100),
          value: valueFromU64(100n),
          rentLamports: BigInt(rentNode),
          pathAccounts: [],
          spareAccounts: [spareNode0],
          spareBumps: [spareBump0],
        }),
      );
    await send(conn, tx, [primary], "seed_insert");
  }

  /* ---- 4. Insert as delegate via FAST path, with delegate account attached ---- */
  console.log("\n=== insert via DELEGATE (FAST path, delegate account attached) ===");
  {
    const hdr = decodeTreeHeader((await conn.getAccountInfo(headerPda))!.data);
    const { path: p } = await traverseToLeaf(conn, programId, headerPda, keyFromU32(200));
    const pathPks = p.map((idx) => deriveNodePda(programId, hdr.treeId, idx)[0]);

    let ix = ixInsertFast({
      programId,
      treeHeader: headerPda,
      authority: delegate.publicKey, // delegate signs — NOT primary
      key: keyFromU32(200),
      value: valueFromU64(200n),
      pathAccounts: pathPks,
    });
    ix = withDelegate(ix, delegatePda);

    const tx = new Transaction()
      .add(ComputeBudgetProgram.setComputeUnitLimit({ units: 100_000 }))
      .add(ix);
    await send(conn, tx, [delegate], "delegate_insert");
  }

  /* ---- 5. Remove the delegate ---- */
  console.log("\n=== remove_delegate ===");
  await send(
    conn,
    new Transaction().add(
      ixRemoveDelegate({
        programId,
        treeHeader: headerPda,
        primaryAuthority: primary.publicKey,
        delegatePda,
        delegate: delegate.publicKey,
      }),
    ),
    [primary],
    "remove_delegate",
  );

  const after = decodeDelegateAccount((await conn.getAccountInfo(delegatePda))!.data);
  console.log(`  delegate count after remove: ${after.count}`);

  /* ---- 6. Try delegate insert again — should fail ---- */
  console.log("\n=== delegate insert AGAIN (should fail) ===");
  {
    const hdr = decodeTreeHeader((await conn.getAccountInfo(headerPda))!.data);
    const { path: p } = await traverseToLeaf(conn, programId, headerPda, keyFromU32(201));
    const pathPks = p.map((idx) => deriveNodePda(programId, hdr.treeId, idx)[0]);

    let ix = ixInsertFast({
      programId,
      treeHeader: headerPda,
      authority: delegate.publicKey,
      key: keyFromU32(201),
      value: valueFromU64(201n),
      pathAccounts: pathPks,
    });
    ix = withDelegate(ix, delegatePda);

    const tx = new Transaction()
      .add(ComputeBudgetProgram.setComputeUnitLimit({ units: 100_000 }))
      .add(ix);
    await send(conn, tx, [delegate], "delegate_insert_revoked", /*expectFail=*/ true);
  }

  console.log("\ndone. Tree:");
  console.log(`  https://explorer.solana.com/address/${headerPda.toBase58()}?cluster=devnet`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
