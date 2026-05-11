/**
 * Run invariant checks against an existing tree.
 *
 *   STATE=demo.json   npx tsx src/test_invariants.ts
 *   STATE=bench.json  npx tsx src/test_invariants.ts
 *
 * Exits 0 if all invariants pass, 1 otherwise.
 */
import * as fs from "fs";
import * as path from "path";
import { Connection, Keypair, PublicKey } from "@solana/web3.js";
import { deriveHeaderPda } from "./torna.ts";
import { TornaStateStore } from "./state.ts";
import { checkInvariants, printReport } from "./invariants.ts";

const REPO_ROOT = path.resolve(import.meta.dirname, "..", "..");
const PROGRAM_KP_PATH = path.join(REPO_ROOT, "out", "torna_btree-keypair.json");
const STATE_FILE = path.join(REPO_ROOT, "client", "state", process.env.STATE ?? "demo.json");
const RPC_URL = process.env.RPC ?? "https://api.devnet.solana.com";

function loadKeypair(p: string): Keypair {
  return Keypair.fromSecretKey(Uint8Array.from(JSON.parse(fs.readFileSync(p, "utf8"))));
}

async function main() {
  console.log(`state: ${STATE_FILE}`);
  console.log(`rpc:   ${RPC_URL}`);

  const conn = new Connection(RPC_URL, "confirmed");
  const programId = loadKeypair(PROGRAM_KP_PATH).publicKey;
  const store = new TornaStateStore(STATE_FILE);
  if (!store.exists()) {
    console.error("State file not found.");
    process.exit(1);
  }
  store.load();

  const [headerPda] = deriveHeaderPda(programId, store.treeId);
  console.log(`header: ${headerPda.toBase58()}\n`);

  const report = await checkInvariants(conn, programId, headerPda);
  printReport(STATE_FILE.split("/").pop()!, report);

  process.exit(report.ok ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
