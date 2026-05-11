/**
 * Event demo (v3.4) — exercises every event type and decodes them off the
 * tx logs the way an indexer would.
 *
 *   1. InitTree + 3 inserts          → 3 InsertEvent
 *   2. delete 1 key                   → 1 DeleteEvent
 *   3. AddDelegate                    → 1 DelegateAddedEvent
 *   4. RemoveDelegate                 → 1 DelegateRemovedEvent
 *   5. TransferAuthority back-and-forth → 2 AuthorityChangeEvent
 *
 *   Run after the program is deployed:
 *     npx tsx src/demo_events.ts
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
  decodeTreeHeader,
  deriveDelegatePda,
  deriveHeaderPda,
  deriveNodePda,
  ixAddDelegate,
  ixDeleteFast,
  ixInitTree,
  ixInsert,
  ixRemoveDelegate,
  ixTransferAuthority,
  keyFromU32,
  valueFromU64,
  NODE_ACCOUNT_DATA_SIZE,
  TREE_HEADER_SIZE,
  DELEGATE_ACCT_SIZE,
  traverseToLeaf,
} from "./torna.ts";

import { eventsFromLogs, formatEvent } from "./events.ts";

const REPO_ROOT = path.resolve(import.meta.dirname, "..", "..");
const PROGRAM_KP_PATH = path.join(REPO_ROOT, "out", "torna_btree-keypair.json");
const RPC_URL = process.env.RPC ?? "https://api.devnet.solana.com";
const PAYER_PATH = process.env.WALLET ?? path.join(os.homedir(), ".config", "solana", "id.json");

function loadKeypair(p: string): Keypair {
  return Keypair.fromSecretKey(Uint8Array.from(JSON.parse(fs.readFileSync(p, "utf8"))));
}

async function runAndParse(
  conn: Connection,
  tx: Transaction,
  signers: Keypair[],
  label: string,
  valueSize: number = 32,
): Promise<void> {
  const sig = await sendAndConfirmTransaction(conn, tx, signers, {
    commitment: "confirmed",
    skipPreflight: false,
  });
  const parsed = await conn.getTransaction(sig, {
    commitment: "confirmed",
    maxSupportedTransactionVersion: 0,
  });
  const logs = parsed?.meta?.logMessages ?? [];
  const events = eventsFromLogs(logs, valueSize);
  console.log(`[${label}] sig=${sig.slice(0, 12)}…  events: ${events.length}`);
  for (const e of events) {
    console.log(`    ${formatEvent(e)}`);
  }
}

async function main() {
  const conn = new Connection(RPC_URL, "confirmed");
  const primary = loadKeypair(PAYER_PATH);
  const programId = loadKeypair(PROGRAM_KP_PATH).publicKey;
  const newPrimary = Keypair.generate();
  const delegate = Keypair.generate();

  console.log(`primary:   ${primary.publicKey.toBase58()}`);
  console.log(`programId: ${programId.toBase58()}`);

  /* Fund the rotated authority so it can pay tx fees for the back-transfer. */
  {
    const tx = new Transaction().add(
      SystemProgram.transfer({
        fromPubkey: primary.publicKey,
        toPubkey: newPrimary.publicKey,
        lamports: 10_000_000,
      }),
    );
    await sendAndConfirmTransaction(conn, tx, [primary]);
  }

  /* Fresh tree. */
  const treeId = Math.floor(Math.random() * 0xffffffff);
  const [headerPda, headerBump] = deriveHeaderPda(programId, treeId);
  const [delegatePda, delegateBump] = deriveDelegatePda(programId, treeId);

  const rentHeader = await conn.getMinimumBalanceForRentExemption(TREE_HEADER_SIZE);
  const rentNode = await conn.getMinimumBalanceForRentExemption(NODE_ACCOUNT_DATA_SIZE);
  const rentDelegate = await conn.getMinimumBalanceForRentExemption(DELEGATE_ACCT_SIZE);

  /* ── init ── */
  await runAndParse(
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

  /* ── three inserts (first allocates the leaf root) ── */
  for (let i = 0; i < 3; i++) {
    const hdr = decodeTreeHeader((await conn.getAccountInfo(headerPda))!.data);
    const key = keyFromU32(100 + i);

    let pathPubkeys: PublicKey[] = [];
    if (hdr.height > 0) {
      const { path: p } = await traverseToLeaf(conn, programId, headerPda, key);
      pathPubkeys = p.map((idx) => deriveNodePda(programId, hdr.treeId, idx)[0]);
    }
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
          payer: primary.publicKey,
          key,
          value: valueFromU64(BigInt(1000 + i)),
          rentLamports: BigInt(rentNode),
          pathAccounts: pathPubkeys,
          spareAccounts: spares.map((s) => s.pubkey),
          spareBumps: spares.map((s) => s.bump),
        }),
      );
    await runAndParse(conn, tx, [primary], `insert ${100 + i}`);
  }

  /* ── delete one ── */
  {
    const hdr = decodeTreeHeader((await conn.getAccountInfo(headerPda))!.data);
    const key = keyFromU32(101);
    const { path: p } = await traverseToLeaf(conn, programId, headerPda, key);
    const pathPks = p.map((idx) => deriveNodePda(programId, hdr.treeId, idx)[0]);
    await runAndParse(
      conn,
      new Transaction().add(
        ixDeleteFast({
          programId,
          treeHeader: headerPda,
          authority: primary.publicKey,
          key,
          pathAccounts: pathPks,
        }),
      ),
      [primary],
      "delete 101",
    );
  }

  /* ── add delegate ── */
  await runAndParse(
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

  /* ── remove delegate ── */
  await runAndParse(
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

  /* ── transfer authority away and back ── */
  await runAndParse(
    conn,
    new Transaction().add(
      ixTransferAuthority({
        programId,
        treeHeader: headerPda,
        currentAuthority: primary.publicKey,
        newAuthority: newPrimary.publicKey,
      }),
    ),
    [primary],
    "transfer → newPrimary",
  );

  await runAndParse(
    conn,
    new Transaction().add(
      ixTransferAuthority({
        programId,
        treeHeader: headerPda,
        currentAuthority: newPrimary.publicKey,
        newAuthority: primary.publicKey,
      }),
    ),
    [newPrimary],
    "transfer ← back",
  );

  console.log(`\nTree: https://explorer.solana.com/address/${headerPda.toBase58()}?cluster=devnet`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
