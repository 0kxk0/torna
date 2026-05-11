/**
 * Multi-client concurrency model + retry helpers for Torna.
 *
 * ──── Solana account-lock layer (free correctness) ────────────────────────────
 *
 * Solana enforces write/read locks on accounts declared in each tx. This gives
 * us correctness for free in most cases:
 *
 * • IX_INSERT_FAST / IX_DELETE_FAST / IX_BULK_*_FAST — header declared
 *   READ-ONLY, only the target leaf is writable. Multiple FAST ops targeting
 *   DIFFERENT leaves carry disjoint write sets and execute in parallel.
 *   FAST ops targeting the SAME leaf serialize by leaf account lock — also
 *   correct.
 *
 * • IX_INSERT (with split) / IX_DELETE (with rebalance) — header declared
 *   WRITABLE. Multiple full-path ops serialize on the header lock. This is
 *   automatic: the Solana scheduler will not parallelize two txs writing to
 *   the same account. So `node_count` and `total_entries` cannot race —
 *   txs are linearized by the runtime.
 *
 * ──── Where retries actually matter ───────────────────────────────────────────
 *
 * 1. PDA collision on spare allocation.
 *    A client predicts `next_node_idx = header.node_count + i` before sending
 *    the tx. If between READ and SEND another tx ran and consumed that idx,
 *    the system_program::create_account CPI inside Insert will fail with
 *    "AccountAlreadyInUse" / SystemProgramError::AccountAlreadyInUse (0x0).
 *    Resolution: re-read header, recompute spare PDAs, resend.
 *
 * 2. Header lock contention.
 *    Heavy concurrent splitting inserts queue on the header lock. Not a
 *    correctness issue — just latency. Throughput hits the slot rate.
 *    Mitigation: prefer FAST path when caller knows no split is needed.
 *
 * 3. Stale path.
 *    Client reads tree state, traverses, then submits. If the tree mutates
 *    between read and submit (e.g., a split moves a key to a new leaf), the
 *    in-tx path validation fails with ERR_BAD_PATH. Resolution: re-traverse
 *    and resubmit.
 *
 * The `withRetry` helper below wraps a tx-sending operation with these three
 * recovery strategies.
 */
import { Connection, SendTransactionError } from "@solana/web3.js";

export interface RetryOptions {
  maxAttempts?: number;
  baseDelayMs?: number;
  onRetry?: (attempt: number, err: unknown) => void;
}

/** Returns true if the error is one of the known recoverable categories. */
export function isRetriable(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const msg = err.message;
  if (msg.includes("AccountAlreadyInUse") || msg.includes("custom program error: 0x0")) return true;
  if (msg.includes("custom program error: 0x69")) return true; // ERR_BAD_PATH = 105 = 0x69
  if (msg.includes("blockhash") && msg.includes("not found")) return true;
  if (msg.includes("block height exceeded")) return true;
  return false;
}

/** Run `op` with at most maxAttempts attempts. Backs off exponentially. */
export async function withRetry<T>(
  op: () => Promise<T>,
  opts: RetryOptions = {},
): Promise<T> {
  const max = opts.maxAttempts ?? 4;
  const base = opts.baseDelayMs ?? 200;
  let lastErr: unknown;
  for (let attempt = 0; attempt < max; attempt++) {
    try {
      return await op();
    } catch (err) {
      lastErr = err;
      if (!isRetriable(err)) throw err;
      if (opts.onRetry) opts.onRetry(attempt, err);
      const delay = base * Math.pow(2, attempt);
      await new Promise((r) => setTimeout(r, delay));
    }
  }
  throw lastErr;
}
