/**
 * Torna client library — talks to torna_btree.so on Solana.
 * Mirrors the on-chain layout exactly. Keep in sync with src/torna_btree/torna_btree.c.
 */
import {
  Connection,
  Keypair,
  PublicKey,
  SystemProgram,
  Transaction,
  TransactionInstruction,
  sendAndConfirmTransaction,
  ComputeBudgetProgram,
  ConfirmOptions,
} from "@solana/web3.js";

export const KEY_SIZE = 32;
export const VAL_SIZE = 32;                      /* v2: 8 → 32 bytes (fits a Pubkey or composite struct) */
export const KEYS_PER_NODE_MAX = 64;
export const KEYS_ARRAY_SIZE = KEYS_PER_NODE_MAX + 1;
export const CHILDREN_ARRAY_SIZE = KEYS_ARRAY_SIZE + 1;

export const NODE_HEADER_SIZE = 16;
export const NODE_ACCOUNT_DATA_SIZE = 8192;      /* v2: 4096 → 8192 to fit 32-byte values at fanout 64 */
export const TREE_HEADER_SIZE = 80;              /* +32 bytes for authority pubkey */

export enum Ix {
  InitTree = 0,
  Insert = 2,
  Find = 3,
  RangeScan = 4,
  Stats = 5,
  InsertFast = 6,
  DeleteFast = 7,
  Delete = 8,
  BulkInsertFast = 9,
  BulkDeleteFast = 10,
  TransferAuthority = 11,
}

/** Build IX_TRANSFER_AUTHORITY — current authority signs to move auth to a new pubkey.
 *  ix_data: [disc=11][newAuthority[32]]
 *  accounts: [treeHeader(w), currentAuthority(s)]
 */
export function ixTransferAuthority(args: {
  programId: PublicKey;
  treeHeader: PublicKey;
  currentAuthority: PublicKey;
  newAuthority: PublicKey;
}): TransactionInstruction {
  const data = Buffer.alloc(1 + 32);
  data.writeUInt8(Ix.TransferAuthority, 0);
  args.newAuthority.toBuffer().copy(data, 1);
  return new TransactionInstruction({
    programId: args.programId,
    keys: [
      { pubkey: args.treeHeader, isSigner: false, isWritable: true },
      { pubkey: args.currentAuthority, isSigner: true, isWritable: false },
    ],
    data,
  });
}

export const KEYS_PER_NODE_MIN = KEYS_PER_NODE_MAX / 2;

export interface TreeHeader {
  magic: number;
  treeId: number;
  rootNodeIdx: number;
  height: number;
  nodeCount: number;
  leftmostLeafIdx: number;
  keySize: number;
  valueSize: number;
  totalEntries: bigint;
  authority: PublicKey;
}

export interface NodeHeader {
  isLeaf: boolean;
  initialized: boolean;
  keyCount: number;
  nodeIdx: number;
  parentIdx: number;
  nextLeafIdx: number;
}

export interface NodeView {
  hdr: NodeHeader;
  keys: Buffer[];
  values?: Buffer[]; // leaf
  children?: number[]; // internal
}

// -----------------------------------------------------------------------------
// Encoding / decoding
// -----------------------------------------------------------------------------

export function decodeTreeHeader(data: Buffer): TreeHeader {
  return {
    magic: data.readUInt32LE(0),
    treeId: data.readUInt32LE(4),
    rootNodeIdx: data.readUInt32LE(8),
    height: data.readUInt32LE(12),
    nodeCount: data.readUInt32LE(16),
    leftmostLeafIdx: data.readUInt32LE(20),
    keySize: data.readUInt16LE(24),
    valueSize: data.readUInt16LE(26),
    totalEntries: data.readBigUInt64LE(28),
    authority: new PublicKey(Buffer.from(data.subarray(36, 36 + 32))),
  };
}

export function decodeNode(data: Buffer): NodeView {
  const hdr: NodeHeader = {
    isLeaf: data.readUInt8(0) !== 0,
    initialized: data.readUInt8(1) !== 0,
    keyCount: data.readUInt16LE(2),
    nodeIdx: data.readUInt32LE(4),
    parentIdx: data.readUInt32LE(8),
    nextLeafIdx: data.readUInt32LE(12),
  };
  const keys: Buffer[] = [];
  const keysOffset = NODE_HEADER_SIZE;
  for (let i = 0; i < hdr.keyCount; i++) {
    keys.push(
      Buffer.from(data.subarray(keysOffset + i * KEY_SIZE, keysOffset + (i + 1) * KEY_SIZE)),
    );
  }
  if (hdr.isLeaf) {
    const valOffset = keysOffset + KEYS_ARRAY_SIZE * KEY_SIZE;
    const values: Buffer[] = [];
    for (let i = 0; i < hdr.keyCount; i++) {
      values.push(
        Buffer.from(data.subarray(valOffset + i * VAL_SIZE, valOffset + (i + 1) * VAL_SIZE)),
      );
    }
    return { hdr, keys, values };
  } else {
    const childOffset = keysOffset + KEYS_ARRAY_SIZE * KEY_SIZE;
    const children: number[] = [];
    for (let i = 0; i <= hdr.keyCount; i++) {
      children.push(data.readUInt32LE(childOffset + i * 4));
    }
    return { hdr, keys, children };
  }
}

// -----------------------------------------------------------------------------
// Instruction builders
// -----------------------------------------------------------------------------

/** Build IX_DELETE_FAST — shift-delete in leaf only, no rebalance. Header RO.
 *
 *  ix_data: [disc=7][key[32]][path_len]
 *  accounts: [header(ro), path...(ro), leaf(w)]
 *  return_data: [found u8][value u8[8]] (if found=1)
 */
export function ixDeleteFast(args: {
  programId: PublicKey;
  treeHeader: PublicKey;
  authority: PublicKey;
  key: Buffer;
  pathAccounts: PublicKey[];
}): TransactionInstruction {
  const data = Buffer.alloc(1 + KEY_SIZE + 1);
  data.writeUInt8(Ix.DeleteFast, 0);
  args.key.copy(data, 1);
  data.writeUInt8(args.pathAccounts.length, 1 + KEY_SIZE);

  const leafIdx = args.pathAccounts.length - 1;
  const keys = [
    { pubkey: args.treeHeader, isSigner: false, isWritable: false },
    { pubkey: args.authority, isSigner: true, isWritable: false },
    ...args.pathAccounts.map((p, i) => ({
      pubkey: p,
      isSigner: false,
      isWritable: i === leafIdx,
    })),
  ];
  return new TransactionInstruction({ programId: args.programId, keys, data });
}

/** Build IX_DELETE — full delete with cascading rebalance.
 *
 *  ix_data: [disc=8][key[32]][path_len][sibling_sides[path_len]]
 *    sibling_sides[i]: 0 = no sibling at level i, 1 = right sibling, 2 = left sibling
 *  accounts: [header(w), payer(s,w), path...(w), siblings... in level order]
 *
 *  `siblings` lists entries for each level that has a sibling. The order of
 *  entries in the tx accounts array follows level order (level 0 first if
 *  it has one — note level 0 is root, can never have a sibling).
 */
export function ixDelete(args: {
  programId: PublicKey;
  treeHeader: PublicKey;
  payer: PublicKey;
  key: Buffer;
  pathAccounts: PublicKey[]; // root → leaf, length == tree height
  siblings: { level: number; side: "right" | "left"; pubkey: PublicKey }[];
}): TransactionInstruction {
  const pathLen = args.pathAccounts.length;
  // Build sibling_sides byte array.
  const sides = Buffer.alloc(pathLen); // all 0 by default
  // Sort siblings by level to ensure correct ordering in account list.
  const sorted = [...args.siblings].sort((a, b) => a.level - b.level);
  for (const s of sorted) {
    if (s.level < 0 || s.level >= pathLen) throw new Error(`sibling level out of range: ${s.level}`);
    if (sides[s.level] !== 0) throw new Error(`duplicate sibling for level ${s.level}`);
    sides[s.level] = s.side === "right" ? 1 : 2;
  }

  const data = Buffer.alloc(1 + KEY_SIZE + 1 + pathLen);
  data.writeUInt8(Ix.Delete, 0);
  args.key.copy(data, 1);
  data.writeUInt8(pathLen, 1 + KEY_SIZE);
  sides.copy(data, 1 + KEY_SIZE + 1);

  const keys = [
    { pubkey: args.treeHeader, isSigner: false, isWritable: true },
    { pubkey: args.payer, isSigner: true, isWritable: true },
    ...args.pathAccounts.map((p) => ({ pubkey: p, isSigner: false, isWritable: true })),
    ...sorted.map((s) => ({ pubkey: s.pubkey, isSigner: false, isWritable: true })),
  ];
  return new TransactionInstruction({ programId: args.programId, keys, data });
}

/** Build IX_BULK_INSERT_FAST — insert N keys (ascending order) into one leaf.
 *
 *  ix_data: [disc=9][path_len][count][(key[32] + value[32]) * count]
 *  accounts: [header(ro), path(ro), leaf(w)]
 *  Caller must pre-sort entries ascending. Refuses on overflow.
 */
export function ixBulkInsertFast(args: {
  programId: PublicKey;
  treeHeader: PublicKey;
  authority: PublicKey;
  pathAccounts: PublicKey[];
  entries: { key: Buffer; value: Buffer }[];
}): TransactionInstruction {
  if (args.entries.length === 0) throw new Error("entries cannot be empty");
  if (args.entries.length > 255) throw new Error("entries too many (>255)");
  for (const e of args.entries) {
    if (e.key.length !== KEY_SIZE) throw new Error("key must be 32 bytes");
    if (e.value.length !== VAL_SIZE) throw new Error("value must be 32 bytes");
  }

  const count = args.entries.length;
  const entryBytes = KEY_SIZE + VAL_SIZE;
  const data = Buffer.alloc(1 + 1 + 1 + count * entryBytes);
  data.writeUInt8(Ix.BulkInsertFast, 0);
  data.writeUInt8(args.pathAccounts.length, 1);
  data.writeUInt8(count, 2);
  for (let i = 0; i < count; i++) {
    args.entries[i].key.copy(data, 3 + i * entryBytes);
    args.entries[i].value.copy(data, 3 + i * entryBytes + KEY_SIZE);
  }

  const leafIdx = args.pathAccounts.length - 1;
  const keys = [
    { pubkey: args.treeHeader, isSigner: false, isWritable: false },
    { pubkey: args.authority, isSigner: true, isWritable: false },
    ...args.pathAccounts.map((p, i) => ({
      pubkey: p,
      isSigner: false,
      isWritable: i === leafIdx,
    })),
  ];
  return new TransactionInstruction({ programId: args.programId, keys, data });
}

/** Build IX_BULK_DELETE_FAST — delete N keys from one leaf.
 *
 *  ix_data: [disc=10][path_len][count][key[32] * count]
 *  accounts: [header(ro), path(ro), leaf(w)]
 *  return_data: [u16 deleted_count]
 */
export function ixBulkDeleteFast(args: {
  programId: PublicKey;
  treeHeader: PublicKey;
  authority: PublicKey;
  pathAccounts: PublicKey[];
  keys: Buffer[];
}): TransactionInstruction {
  if (args.keys.length === 0) throw new Error("keys cannot be empty");
  if (args.keys.length > 255) throw new Error("keys too many (>255)");
  for (const k of args.keys) {
    if (k.length !== KEY_SIZE) throw new Error("key must be 32 bytes");
  }

  const count = args.keys.length;
  const data = Buffer.alloc(1 + 1 + 1 + count * KEY_SIZE);
  data.writeUInt8(Ix.BulkDeleteFast, 0);
  data.writeUInt8(args.pathAccounts.length, 1);
  data.writeUInt8(count, 2);
  for (let i = 0; i < count; i++) {
    args.keys[i].copy(data, 3 + i * KEY_SIZE);
  }

  const leafIdx = args.pathAccounts.length - 1;
  const metas = [
    { pubkey: args.treeHeader, isSigner: false, isWritable: false },
    { pubkey: args.authority, isSigner: true, isWritable: false },
    ...args.pathAccounts.map((p, i) => ({
      pubkey: p,
      isSigner: false,
      isWritable: i === leafIdx,
    })),
  ];
  return new TransactionInstruction({ programId: args.programId, keys: metas, data });
}

/** Derive the tree header's PDA from (programId, treeId).
 *  Seeds = ("torna_hdr", treeId_u32_LE). */
export function deriveHeaderPda(
  programId: PublicKey,
  treeId: number,
): [PublicKey, number] {
  const buf = Buffer.alloc(4);
  buf.writeUInt32LE(treeId, 0);
  return PublicKey.findProgramAddressSync(
    [Buffer.from("torna_hdr"), buf],
    programId,
  );
}

/** Build IX_INIT_TREE — program allocates the header PDA via CPI.
 *
 *  ix_data: [disc=0][treeId u32 LE][bump u8][rent_lamports u64 LE]
 *  accounts: [payer(s,w), header_pda(w), system_program]
 */
export function ixInitTree(args: {
  programId: PublicKey;
  payer: PublicKey;
  headerPda: PublicKey;
  treeId: number;
  headerBump: number;
  rentLamports: bigint;
}): TransactionInstruction {
  const data = Buffer.alloc(1 + 4 + 1 + 8);
  data.writeUInt8(Ix.InitTree, 0);
  data.writeUInt32LE(args.treeId, 1);
  data.writeUInt8(args.headerBump, 5);
  data.writeBigUInt64LE(args.rentLamports, 6);
  return new TransactionInstruction({
    programId: args.programId,
    keys: [
      { pubkey: args.payer, isSigner: true, isWritable: true },
      { pubkey: args.headerPda, isSigner: false, isWritable: true },
      { pubkey: SystemProgram.programId, isSigner: false, isWritable: false },
    ],
    data,
  });
}

/**
 * Build IX_INSERT with PDA self-allocation.
 *
 *   ix_data:
 *     [0]                disc = 2
 *     [1..32]            key
 *     [33..40]           value
 *     [41..48]           rent_lamports (LE u64)
 *     [49]               path_len
 *     [50]               spare_count
 *     [51..]             bump bytes (one per spare)
 *
 *   accounts: [header(w), payer(s,w), system_program, path...(w), spares...(w)]
 *
 *   Spares are PDAs at seeds = ("torna", tree_id_LE, node_idx_LE). Pass bumps
 *   alongside in the same order. Unused spare PDAs cost nothing (only consumed
 *   ones are allocated via CPI to system_program).
 */
export function ixInsert(args: {
  programId: PublicKey;
  treeHeader: PublicKey;
  payer: PublicKey;
  key: Buffer;
  value: Buffer;
  rentLamports: bigint;
  pathAccounts: PublicKey[]; // root → leaf
  spareAccounts: PublicKey[];
  spareBumps: number[];
}): TransactionInstruction {
  if (args.key.length !== KEY_SIZE) throw new Error("key must be 32 bytes");
  if (args.value.length !== VAL_SIZE) throw new Error("value must be 8 bytes");
  if (args.pathAccounts.length > 255) throw new Error("path too long");
  if (args.spareAccounts.length > 255) throw new Error("too many spares");
  if (args.spareAccounts.length !== args.spareBumps.length)
    throw new Error("spareAccounts and spareBumps must align");

  const headerLen = 1 + KEY_SIZE + VAL_SIZE + 8 + 1 + 1;
  const data = Buffer.alloc(headerLen + args.spareBumps.length);
  data.writeUInt8(Ix.Insert, 0);
  args.key.copy(data, 1);
  args.value.copy(data, 1 + KEY_SIZE);
  data.writeBigUInt64LE(args.rentLamports, 1 + KEY_SIZE + VAL_SIZE);
  data.writeUInt8(args.pathAccounts.length, 1 + KEY_SIZE + VAL_SIZE + 8);
  data.writeUInt8(args.spareAccounts.length, 1 + KEY_SIZE + VAL_SIZE + 8 + 1);
  for (let i = 0; i < args.spareBumps.length; i++) {
    data.writeUInt8(args.spareBumps[i], headerLen + i);
  }

  const keys = [
    { pubkey: args.treeHeader, isSigner: false, isWritable: true },
    { pubkey: args.payer, isSigner: true, isWritable: true },
    { pubkey: SystemProgram.programId, isSigner: false, isWritable: false },
    ...args.pathAccounts.map((p) => ({ pubkey: p, isSigner: false, isWritable: true })),
    ...args.spareAccounts.map((p) => ({ pubkey: p, isSigner: false, isWritable: true })),
  ];
  return new TransactionInstruction({ programId: args.programId, keys, data });
}

/**
 * Build IX_INSERT_FAST — write-only-to-leaf no-split path.
 *
 *   ix_data: [u8 disc=6][u8 key[32]][u8 value[8]][u8 path_len]
 *   accounts:
 *     [0]                tree header (READ-ONLY)
 *     [1..path_len-1]    internal nodes (read-only)
 *     [path_len]         leaf (writable)
 *
 *   Header read-only allows two FAST inserts to different leaves to execute
 *   concurrently — their write sets ({leaf_a} vs {leaf_b}) are disjoint.
 */
export function ixInsertFast(args: {
  programId: PublicKey;
  treeHeader: PublicKey;
  authority: PublicKey; // must match tree.authority; signs the tx
  key: Buffer;
  value: Buffer;
  pathAccounts: PublicKey[]; // root → leaf; LAST element is the writable leaf
}): TransactionInstruction {
  if (args.key.length !== KEY_SIZE) throw new Error("key must be 32 bytes");
  if (args.value.length !== VAL_SIZE) throw new Error("value must be 32 bytes");
  if (args.pathAccounts.length === 0) throw new Error("path must contain at least the leaf");

  const data = Buffer.alloc(1 + KEY_SIZE + VAL_SIZE + 1);
  data.writeUInt8(Ix.InsertFast, 0);
  args.key.copy(data, 1);
  args.value.copy(data, 1 + KEY_SIZE);
  data.writeUInt8(args.pathAccounts.length, 1 + KEY_SIZE + VAL_SIZE);

  const leafIdx = args.pathAccounts.length - 1;
  const keys = [
    { pubkey: args.treeHeader, isSigner: false, isWritable: false },
    { pubkey: args.authority, isSigner: true, isWritable: false },
    ...args.pathAccounts.map((p, i) => ({
      pubkey: p,
      isSigner: false,
      isWritable: i === leafIdx,
    })),
  ];
  return new TransactionInstruction({ programId: args.programId, keys, data });
}

/** Derive the PDA for a B+ tree node at (treeId, nodeIdx).
 *  Seeds = ("torna", treeId_u32_LE, nodeIdx_u32_LE). */
export function deriveNodePda(
  programId: PublicKey,
  treeId: number,
  nodeIdx: number,
): [PublicKey, number] {
  const treeIdBuf = Buffer.alloc(4);
  treeIdBuf.writeUInt32LE(treeId, 0);
  const idxBuf = Buffer.alloc(4);
  idxBuf.writeUInt32LE(nodeIdx, 0);
  return PublicKey.findProgramAddressSync(
    [Buffer.from("torna"), treeIdBuf, idxBuf],
    programId,
  );
}

export function ixFind(args: {
  programId: PublicKey;
  treeHeader: PublicKey;
  key: Buffer;
  pathAccounts: PublicKey[]; // root → leaf
}): TransactionInstruction {
  const data = Buffer.alloc(1 + KEY_SIZE + 1);
  data.writeUInt8(Ix.Find, 0);
  args.key.copy(data, 1);
  data.writeUInt8(args.pathAccounts.length, 1 + KEY_SIZE);

  const keys = [
    { pubkey: args.treeHeader, isSigner: false, isWritable: false },
    ...args.pathAccounts.map((p) => ({ pubkey: p, isSigner: false, isWritable: false })),
  ];
  return new TransactionInstruction({ programId: args.programId, keys, data });
}

export function ixRangeScan(args: {
  programId: PublicKey;
  treeHeader: PublicKey;
  startKey: Buffer;
  endKey: Buffer;
  pathAccounts: PublicKey[]; // path to leaf containing startKey
  chainAccounts: PublicKey[]; // additional leaves
  maxResults: number;
}): TransactionInstruction {
  const data = Buffer.alloc(1 + 2 * KEY_SIZE + 2);
  data.writeUInt8(Ix.RangeScan, 0);
  args.startKey.copy(data, 1);
  args.endKey.copy(data, 1 + KEY_SIZE);
  data.writeUInt8(args.pathAccounts.length, 1 + 2 * KEY_SIZE);
  data.writeUInt8(args.maxResults, 1 + 2 * KEY_SIZE + 1);

  const keys = [
    { pubkey: args.treeHeader, isSigner: false, isWritable: false },
    ...args.pathAccounts.map((p) => ({ pubkey: p, isSigner: false, isWritable: false })),
    ...args.chainAccounts.map((p) => ({ pubkey: p, isSigner: false, isWritable: false })),
  ];
  return new TransactionInstruction({ programId: args.programId, keys, data });
}

// -----------------------------------------------------------------------------
// Tree traversal (off-chain)
// -----------------------------------------------------------------------------

/** Walk the tree off-chain to find which leaf contains the given key.
 *  Returns the sequence of node indices (root → leaf). Node pubkeys are
 *  derived via PDA from (programId, treeId, nodeIdx) — no local index needed. */
export async function traverseToLeaf(
  conn: Connection,
  programId: PublicKey,
  treeHeaderPubkey: PublicKey,
  key: Buffer,
): Promise<{ path: number[]; height: number; treeId: number }> {
  const hdrAcc = await conn.getAccountInfo(treeHeaderPubkey);
  if (!hdrAcc) throw new Error("tree header not found");
  const hdr = decodeTreeHeader(hdrAcc.data);
  if (hdr.height === 0) return { path: [], height: 0, treeId: hdr.treeId };

  let curIdx = hdr.rootNodeIdx;
  const path: number[] = [curIdx];
  for (let level = 0; level < hdr.height - 1; level++) {
    const [curPk] = deriveNodePda(programId, hdr.treeId, curIdx);
    const acc = await conn.getAccountInfo(curPk);
    if (!acc) throw new Error(`node ${curIdx} not found on chain`);
    const node = decodeNode(acc.data);
    if (node.hdr.isLeaf) throw new Error("internal expected, got leaf");
    let pos = node.keys.findIndex((k) => k.compare(key) >= 0);
    if (pos === -1) pos = node.keys.length;
    let descIdx: number;
    if (pos < node.keys.length && node.keys[pos].compare(key) === 0) {
      descIdx = node.children![pos + 1];
    } else {
      descIdx = node.children![pos];
    }
    curIdx = descIdx;
    path.push(curIdx);
  }
  return { path, height: hdr.height, treeId: hdr.treeId };
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

export function keyFromString(s: string): Buffer {
  const b = Buffer.alloc(KEY_SIZE);
  Buffer.from(s, "utf8").copy(b);
  return b;
}

export function keyFromU32(n: number): Buffer {
  // Big-endian → lexicographic order matches numeric order
  const b = Buffer.alloc(KEY_SIZE);
  b.writeUInt32BE(n, KEY_SIZE - 4);
  return b;
}

/** Encode a u64 into the value slot (low 8 bytes, rest zero-padded). */
export function valueFromU64(n: bigint): Buffer {
  const b = Buffer.alloc(VAL_SIZE);
  b.writeBigUInt64LE(n, 0);
  return b;
}

/** Encode an arbitrary buffer as a value (padded/truncated to VAL_SIZE). */
export function valueFromBytes(src: Buffer): Buffer {
  const b = Buffer.alloc(VAL_SIZE);
  src.copy(b, 0, 0, Math.min(src.length, VAL_SIZE));
  return b;
}

export function readReturnData(logs: string[]): Buffer | null {
  // Solana logs: "Program return: <pubkey> <base64>"
  for (const log of logs) {
    const m = log.match(/Program return:\s+\S+\s+(.+)/);
    if (m) return Buffer.from(m[1], "base64");
  }
  return null;
}
