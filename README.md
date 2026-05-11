# Torna

A B+ tree on Solana. Written in C, spread across many accounts, parallel-safe.

Other Solana programs CPI into this. Closest analog is SQLite — a single ordered
table, but on-chain, and built for the kind of concurrency the account model
allows when you stop pretending everything has to live in one 10 MB blob.

The thing Solana doesn't give you out of the box: ordered key ranges, sorted
iteration, and parallel writes that don't fight over the same account. Torna
gives you all three. Phoenix and OpenBook v2 ship custom slab allocators inside
a single 10 MB account — tight and fast, but every write serializes through one
master. spl-account-compression scales to millions of leaves but has no order
and reads need off-chain Merkle proofs. Torna is the third option.

It's also written in C, not Rust + Anchor. That's deliberate. Multi-account
pointer-walking is borderline impossible with the borrow checker; trivial in C
once you accept the unsafe. The whole project comes down to one observation:
SBF loads all your accounts into one address space, so why are we acting like
they're islands?

## Numbers

Run on devnet, fresh trees, payer pre-funded.

| workload | result |
|---|---|
| 20 inserts, fresh tree | height 1, 1 node, ~0.029 SOL |
| 200 inserts | height 2, 5 nodes, **0.18 SOL total**, avg **5,823 CU** per insert |
| 6 parallel inserts to different leaves | **all 6 landed in slot 461684841**, 731 ms wall |
| Same 6 inserts done serially | 4,727 ms wall, 6 different slots |
| Range scan (16 results across leaves) | 20,291 CU |
| Delete with borrow rebalance | 15,556 CU |

That's **6.47× speedup** from the parallelism, in one of the most contested
parts of a real DEX workload (place order, cancel order). Phoenix can't do this
without redesigning around something Phoenix's slab is fundamentally not.

Devnet program: `C4qNsrX92Nn5z8HwSZCcioMgRoLG6BzCCK6X7kRjEFoD`

## Quick start

```bash
# build
make

# deploy (one-time ~6 SOL on devnet, ~0 SOL on local validator)
solana program deploy out/torna_btree.so --program-id out/torna_btree-keypair.json

# install client deps
cd client && npm install

# demo: insert 20 entries, then find a few, then range scan
ENTRIES=20 npx tsx src/demo.ts

# the headline benchmark — 6 parallel inserts in one slot
SETUP_N=200 npx tsx src/bench_parallel.ts

# delete demo + cascading rebalance
npx tsx src/demo_delete.ts
npx tsx src/demo_rebalance.ts

# verify on-chain tree against B+ invariants
STATE=demo.json npx tsx src/test_invariants.ts
```

Want to skip devnet costs entirely? Run `./scripts/ci.sh`. Spins up
`solana-test-validator`, deploys the program, runs every demo and invariant
check in sequence, tears down at the end. No rent burned.

## What's in the repo

```
torna/
├── src/torna_btree/torna_btree.c     C/SBF program, ~1,830 lines
├── Makefile                          uses platform-tools-sdk/sbf/c/sbf.mk
├── client/                           TypeScript client + demos + benchmarks
│   └── src/
│       ├── torna.ts                  12 ix builders, PDA helpers, decoders
│       ├── state.ts                  minimal local state (programId + treeId)
│       ├── demo.ts                   insert + find + range_scan
│       ├── demo_delete.ts            DELETE_FAST demo
│       ├── demo_rebalance.ts         full delete with borrow
│       ├── bench_parallel.ts         the 6-tx-one-slot benchmark
│       ├── stress_test.ts            seeded random workload + invariant checks
│       ├── invariants.ts             tree validation logic (9 invariants)
│       ├── test_invariants.ts        invariant runner
│       ├── inspect.ts                on-chain tree dump
│       └── concurrency.ts            retry helpers, lock-model notes
├── sdk-rust/                         Rust SDK crate (torna-sdk)
├── examples-rust/mini_orderbook/     example consumer program using CPI
├── idl/torna_btree.json              Anchor-compatible IDL
├── docs/positioning.md               vs spl-account-compression / Phoenix
└── scripts/ci.sh                     local validator CI
```

## How it actually works

Every B+ tree node is its own Solana account.

Internal nodes hold separator keys plus child indices. Leaves hold (key, value)
pairs and a `next_leaf_idx` for ordered traversal across accounts. The tree
header sits at PDA `("torna_hdr", tree_id)` and tracks the root index, height,
node count, and authority. Every node sits at PDA `("torna", tree_id,
node_idx)`. That's it for the addressing scheme. Anyone holding the program ID
and the tree ID can derive every pubkey in the tree without local state.

For parallelism, the rule is: declare only the accounts you actually write.
`InsertFast` and `DeleteFast` mark the tree header as read-only and the target
leaf as writable. Two FAST inserts to different leaves carry disjoint write
sets, so Solana's scheduler runs them in the same slot. If the header were
writable on every insert, all txs would queue behind it. They don't, because
the FAST path doesn't touch the header.

When a split is needed, the full `Insert` path is used. That one writes the
header (incrementing `node_count`, possibly updating `root_node_idx` and
`height`), and it allocates spare node PDAs via CPI into the system program.
Only consumed spares get rent — unused pre-passed PDAs cost nothing.

Deletes work the same way but in reverse: leaf-level shift removes the entry,
and if the leaf falls below MIN keys the program borrows from a sibling or
merges. Merges free the emptied account and refund rent to the payer. Cascade
walks up the path; if the root ends up with one child, the tree height shrinks
and the root account is closed.

## Instructions

| disc | name | accounts (writable in **bold**) | what |
|------|------|-------------------------------|------|
| 0 | InitTree | payer(s), **header(PDA)**, sysprog | create tree header at PDA via CPI |
| 2 | Insert | **header**, payer(s), sysprog, **path**, **spares** | full insert with split + spare alloc |
| 3 | Find | header, path | descend, return value |
| 4 | RangeScan | header, path, chain | walk leaf chain in `[start, end]` |
| 5 | Stats | header | return TreeHeader |
| 6 | InsertFast | header, authority(s), path…, **leaf** | parallel-safe insert (no split) |
| 7 | DeleteFast | header, authority(s), path…, **leaf** | parallel-safe shift-delete |
| 8 | Delete | **header**, payer(s), **path**, **siblings** | full delete with cascading rebalance |
| 9 | BulkInsertFast | header, authority(s), path…, **leaf** | up to ~18 entries into one leaf |
| 10 | BulkDeleteFast | header, authority(s), path…, **leaf** | batch delete from one leaf |
| 11 | TransferAuthority | **header**, current_authority(s) | move write-authority |

## Architecture in one paragraph

KEYS_PER_NODE_MAX = 64. Each node is 8 KB. Header is 80 bytes including a
32-byte authority pubkey. Keys are fixed at 32 bytes (composite keys encode
anything you need — price + side + timestamp). Values are fixed at 32 bytes
today (Pubkey-sized; fits order IDs, account refs, packed small structs).
Runtime variable sizes are on the roadmap. Tree height grows as splits
propagate up; root collapses on merge cascade. The leaf chain
(`next_leaf_idx`) lets RangeScan stream entries in sorted order across as many
accounts as needed in one tx — only bounded by the per-tx account limit.

## Roadmap

**v3.1 — runtime variable value sizes — shipped 2026-05-12.**
Each tree now picks its own `value_size` at `InitTree` time and the on-chain
stride math respects it across every helper, ix-data parser, and return-data
writer. Stack arrays remain sized at the compile-time bound `VAL_SIZE_MAX = 64`.
Values can be any width in `[1, 64]` bytes — an NFT marketplace can use 64-byte
listing structs in the same deployed program where a DAO uses 40-byte vote
records and an oracle index uses 8-byte u64 prices.
Keys are still fixed at 32 bytes; runtime key_size is on the longer-term list
but hasn't been a constraint in practice (composite keys pack well into 32).

**v3.2 — multi-authority delegates — shipped 2026-05-12.**
An optional side account at PDA `("torna_dlg", tree_id)` holds up to eight
additional signers per tree. `IX_ADD_DELEGATE` and `IX_REMOVE_DELEGATE`
are restricted to the primary authority. Every write instruction now goes
through a unified `tx_has_authorized_signer` check that accepts either the
primary or any registered delegate. Authority transfer (`IX 11`) stays
primary-only by design — delegates can't rotate the root key.
Same change also patched a latent FAST-path index regression that had been
quietly breaking parallel writes since the v3 authority model landed.

**v3.3 — Anchor codegen integration test.**
The IDL exists. v3.3 actually runs it through Anchor's TypeScript codegen and
verifies the generated client can call every instruction end-to-end. Cheap
work, just hasn't been done.

**v3.4 — on-chain events — shipped 2026-05-12.**
Five structured event types emitted via `sol_log_data` on every state
change: `Insert`, `Delete`, `AuthorityChange`, `DelegateAdded`,
`DelegateRemoved`. Each is a single log line with a 1-byte discriminator
and a packed payload. The TS client ships a decoder (`events.ts`) that
turns tx logs into a typed `TornaEvent` union. Indexers and Geyser
plugins can rebuild tree state externally without polling on-chain
accounts.

**v3.5 — atomic multi-leaf bulk insert.**
`BulkInsertFast` targets one leaf today. v3.5 supports inserting into multiple
leaves in one tx — useful for batch order placement that hits several price
levels at once.

**v4 — variable fanout.**
Today fanout is 64 at compile time. Different workloads want different
fanouts. v4 makes this a tree-level parameter set at InitTree, with the same
8 KB node size budget.

**v4.x — formal verification.**
After v3 stabilizes, run the C through a verifier (cbmc, frama-c) to prove the
B+ invariants hold under all reachable states. Probably also a fuzzer that
exercises insert / delete sequences and asserts invariants every step.

## License

Source-available for hackathon judging, personal evaluation, and education.
No production deployment, no commercial use, no derivative works without
explicit written permission. See [LICENSE](./LICENSE) for the full terms.

For commercial licensing or partnership inquiries, contact the author.
