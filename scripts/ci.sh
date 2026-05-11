#!/usr/bin/env bash
# Local validator CI for Torna.
#
# Spins up solana-test-validator on a free port, points the Solana CLI at it,
# deploys the program, runs all demos + invariant checks, then tears down.
#
# Usage:
#   ./scripts/ci.sh                # full run
#   ./scripts/ci.sh keep-running   # leave validator up after tests
#
# Exits non-zero on any test failure.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

VALIDATOR_LEDGER="$(mktemp -d -t torna-ledger-XXXXXX)"
VALIDATOR_LOG="$(mktemp -t torna-validator-XXXXXX.log)"
PROGRAM_SO="$REPO_ROOT/out/torna_btree.so"
PROGRAM_KP="$REPO_ROOT/out/torna_btree-keypair.json"
PREV_RPC="$(solana config get json_rpc_url | awk '{print $NF}')"
PREV_WS="$(solana config get websocket_url | awk '{print $NF}')"

LOCAL_RPC="http://127.0.0.1:8899"
LOCAL_WS="ws://127.0.0.1:8900"

VALIDATOR_PID=""

cleanup() {
  echo "--- cleanup ---"
  if [[ "${1:-}" != "keep" && -n "$VALIDATOR_PID" ]]; then
    kill "$VALIDATOR_PID" 2>/dev/null || true
    wait "$VALIDATOR_PID" 2>/dev/null || true
  fi
  echo "  restoring RPC config to $PREV_RPC"
  solana config set --url "$PREV_RPC" --ws "$PREV_WS" >/dev/null
  if [[ "${1:-}" != "keep" ]]; then
    rm -rf "$VALIDATOR_LEDGER"
  fi
}

trap 'cleanup' EXIT

echo "--- starting solana-test-validator ---"
solana-test-validator \
  --ledger "$VALIDATOR_LEDGER" \
  --reset \
  --quiet \
  > "$VALIDATOR_LOG" 2>&1 &
VALIDATOR_PID=$!
echo "  PID $VALIDATOR_PID  log $VALIDATOR_LOG"

echo -n "  waiting for RPC..."
for i in $(seq 1 60); do
  if curl -fs "$LOCAL_RPC" -X POST -H "Content-Type: application/json" \
      --data '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' >/dev/null 2>&1; then
    echo " ready"
    break
  fi
  echo -n "."
  sleep 1
done

solana config set --url "$LOCAL_RPC" --ws "$LOCAL_WS" >/dev/null

PAYER_PUBKEY="$(solana address)"
echo "  payer: $PAYER_PUBKEY"
solana airdrop 100 "$PAYER_PUBKEY" >/dev/null
echo "  balance: $(solana balance)"

echo "--- building program ---"
make >/dev/null

echo "--- deploying program ---"
solana program deploy "$PROGRAM_SO" --program-id "$PROGRAM_KP" >/dev/null
PROGRAM_ID="$(solana address -k "$PROGRAM_KP")"
echo "  program: $PROGRAM_ID"

rm -f "$REPO_ROOT/client/state/"*.json

cd "$REPO_ROOT/client"

run() {
  local name="$1"
  shift
  echo ""
  echo "▶ $name"
  if "$@"; then
    echo "  ✓ $name"
  else
    echo "  ✗ $name FAILED"
    exit 1
  fi
}

run "demo (insert + find + range_scan)" \
  env RPC="$LOCAL_RPC" ENTRIES=20 npx tsx src/demo.ts

run "test_invariants on demo.json" \
  env RPC="$LOCAL_RPC" STATE=demo.json npx tsx src/test_invariants.ts

run "demo_delete (DELETE_FAST)" \
  env RPC="$LOCAL_RPC" npx tsx src/demo_delete.ts

run "test_invariants after deletes" \
  env RPC="$LOCAL_RPC" STATE=demo.json npx tsx src/test_invariants.ts

run "bench_parallel (200 setup + 6 parallel)" \
  env RPC="$LOCAL_RPC" SETUP_N=200 npx tsx src/bench_parallel.ts

run "test_invariants on bench.json" \
  env RPC="$LOCAL_RPC" STATE=bench.json npx tsx src/test_invariants.ts

run "demo_rebalance (full delete with borrow)" \
  env RPC="$LOCAL_RPC" npx tsx src/demo_rebalance.ts

run "test_invariants post-rebalance" \
  env RPC="$LOCAL_RPC" STATE=bench.json npx tsx src/test_invariants.ts

echo ""
echo "=================================="
echo "All CI checks passed."
echo "=================================="
