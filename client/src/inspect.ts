/**
 * Inspect on-chain tree state. Walks node indices 1..nodeCount and
 * resolves them through PDA derivation.
 */
import { Connection, PublicKey } from "@solana/web3.js";
import { decodeNode, decodeTreeHeader, deriveNodePda, deriveHeaderPda } from "./torna.ts";
import { TornaStateStore } from "./state.ts";
import * as path from "path";

const STATE_FILE = path.resolve(import.meta.dirname, "..", "state", "demo.json");
const RPC_URL = process.env.RPC ?? "https://api.devnet.solana.com";

async function main() {
  const conn = new Connection(RPC_URL, "confirmed");
  const store = new TornaStateStore(STATE_FILE);
  store.load();

  const [headerPda] = deriveHeaderPda(store.programId, store.treeId);
  const h = await conn.getAccountInfo(headerPda);
  console.log("=== HEADER ===", headerPda.toBase58());
  const hdr = decodeTreeHeader(h!.data);
  console.log(hdr);

  for (let idx = 1; idx <= hdr.nodeCount; idx++) {
    const [pk] = deriveNodePda(store.programId, hdr.treeId, idx);
    const a = await conn.getAccountInfo(pk);
    if (!a) {
      console.log(`=== node ${idx} (${pk.toBase58()}) === NOT ALLOCATED (unused spare?)`);
      continue;
    }
    const n = decodeNode(a.data);
    console.log(`=== node ${idx} (${pk.toBase58()}) size=${a.data.length} ===`);
    console.log("  hdr:", n.hdr);
    const ks = n.keys.slice(0, 16).map((k) => k.readUInt32BE(28));
    console.log(`  keys[0..${Math.min(n.keys.length, 16)}]:`, ks, n.keys.length > 16 ? `… +${n.keys.length - 16}` : "");
    if (n.children) console.log("  children:", n.children.slice(0, 16), n.children.length > 16 ? `… +${n.children.length - 16}` : "");
    if (n.values) {
      const vs = n.values.slice(0, 16).map((v) => Number(v.readBigUInt64LE(0)));
      console.log(`  values[0..${Math.min(n.values.length, 16)}]:`, vs);
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
