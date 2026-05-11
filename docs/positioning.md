# Torna vs the alternatives

A clear-eyed positioning of Torna against the closest things on Solana today.

## TL;DR

| | **Torna** | spl-account-compression | Phoenix slab | OpenBook v2 |
|---|---|---|---|---|
| Primitive type | Multi-account B+ tree | Concurrent Merkle tree | Single-account slab allocator | Single-account slab allocator |
| Ordered key range scan | ✓ | ✗ (only membership proof) | ✓ (within one account) | ✓ (within one account) |
| Sorted iteration | ✓ | ✗ | ✓ | ✓ |
| Insert / delete | ✓ | ✓ | ✓ | ✓ |
| Find by key | ✓ | ✓ via off-chain proof | ✓ | ✓ |
| **Parallel writes to disjoint regions** | **✓** | ✗ (root contention) | ✗ (single account) | ✗ |
| Scales past 10 MB total state | ✓ (multi-account span) | ✓ (compressed) | ✗ (10 MB account limit) | ✗ |
| Implementation language | C / SBF | Rust / Anchor | Rust | Rust |
| CU per insert (uncontended) | ~3,800 – 5,800 | ~25,000+ | ~10,000+ | ~15,000+ |

## What each one is actually for

### Torna (this project)

- A **sorted key-value store** on Solana, spread across many accounts.
- Closest analog: SQLite, or a single ordered table in an OLTP DB.
- **Strengths:** range queries, sorted iteration, parallel writes across leaves, zero local state (PDA-derived).
- **Weaknesses:** consumer programs need to traverse off-chain to compute paths, full delete with rebalance serializes on the header.
- **Sweet spot:** orderbook bids/asks, NFT marketplace listings by price, DAO timeline indexes, prediction market outcomes, on-chain leaderboards.

### `spl-account-compression`

- A **concurrent Merkle tree** for compressed state. Each leaf is just a hash; the underlying data lives off-chain.
- Strengths: massive scale (millions of leaves in tens of KB of on-chain state), suitable for compressed NFTs.
- Weaknesses: **no order, no range scan**. You can prove membership, not "give me everything between X and Y." Reads require off-chain Merkle proofs.
- Sweet spot: cNFTs, large registries where you only need "is X a member?" queries.
- **Not a substitute for Torna.** Different primitive entirely.

### Phoenix slab (Phoenix V1, Ellipsis Labs)

- A custom slab allocator inside a single 10 MB account.
- Strengths: extremely CU-efficient for matching, well-tuned for Phoenix's use case (centralized limit orderbook).
- Weaknesses: **single account = no parallelism**. All trades serialize on the orderbook account. Hits the 10 MB account ceiling.
- Sweet spot: a single high-throughput orderbook where the team controls the implementation.
- **Limit:** another team can't easily reuse it. The slab format is internal.

### OpenBook v2

- Similar to Phoenix in spirit: a custom in-program structure for orderbook state, packed into a single account per market.
- Same tradeoffs as Phoenix slab — fast and tight, but no parallelism across markets sharing a book, no generic reusability.

## Where Torna actually wins

The two things only Torna offers, on Solana today, as a reusable primitive:

1. **Sorted, range-scannable, multi-account state.**
   No other on-chain primitive lets you do "give me all entries in `[X, Y]` in ascending order, walking across multiple accounts." spl-account-compression is unordered. Phoenix/OpenBook are sorted but trapped inside one account.

2. **Parallel writes to disjoint regions.**
   Phoenix serializes all order placements through its single account. Torna parallelizes them across leaves. Devnet benchmark: 6 inserts in 1 slot vs 6 inserts in 6 slots — **6.47× speedup** for the same workload (run `client/src/bench_parallel.ts`).

This second point matters more as a Solana program scales. Single-account designs hit a ceiling at ~1 tx/slot per book. Torna hits the network's actual ceiling: many tx/slot, bounded only by leaf count.

## Where the alternatives win

- **For pure compressed registry / cNFT**: spl-account-compression is purpose-built and cheaper.
- **For centralised matching with extreme tuning**: Phoenix slab beats Torna's CU numbers when the use case fits a single 10 MB account, because Phoenix doesn't pay for path validation.
- **For "fully Anchor-ergonomic"**: nothing currently — Torna ships an IDL but the canonical client is TypeScript/Rust direct, not Anchor codegen.

## When you should use Torna

You should reach for Torna when ALL three are true:

1. You need **ordered** keys (range scan or sorted iteration).
2. You expect state to grow past what fits in one 10 MB account, OR you want write parallelism.
3. The data is hot enough that off-chain Merkle proofs (the cNFT pattern) would be awkward — you need on-chain reads.

## When you should NOT use Torna

- If your reads are point queries only (just "does X exist?"), spl-account-compression is cheaper.
- If your whole orderbook fits in 10 MB and one writer serializes fine, Phoenix or a custom slab is faster.
- If you don't have CPI bandwidth or your consumer program needs Anchor IDL ergonomics today, you'll be paying integration overhead Torna doesn't yet abstract.

## A concrete head-to-head

If your protocol places ~1k orders/minute across 1000 different price points, and you want each order placement to take one slot regardless of others:

| Approach | Concurrency | Latency per place | Saturation |
|---|---|---|---|
| Phoenix slab | All serial through master | ~400 ms × N | hits 1 tx/slot |
| OpenBook v2 | Same | Same | Same |
| **Torna** | One leaf per price band | **~400 ms regardless** | **hits network rate** |

That's the moat.
