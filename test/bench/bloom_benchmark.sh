#!/usr/bin/env bash
# Runs bloom filter before/after benchmark.
# Usage: ./test/bench/bloom_benchmark.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DATA_DIR=$(mktemp -d)
trap "rm -rf $DATA_DIR" EXIT

BINARY="$REPO_ROOT/bin/node"
if [ ! -f "$BINARY" ]; then
  echo "Building node binary..."
  go build -o "$BINARY" "$REPO_ROOT/cmd/node/"
fi

run_bench() {
  local bloom=$1
  local label=$2
  local port=19$(( RANDOM % 900 + 100 ))

  local dir="$DATA_DIR/$label"
  mkdir -p "$dir"

  echo ""
  echo "=== $label (BLOOM_ENABLED=$bloom) ==="

  BLOOM_ENABLED="$bloom" DATA_DIR="$dir" GRPC_ADDR=":$port" "$BINARY" &
  NODE_PID=$!
  sleep 1

  echo "Ingesting 5000 entries..."
  go run "$REPO_ROOT/test/load/main.go" \
    --node-addr="localhost:$port" \
    --addr="localhost:$port" \
    --mode=ingest \
    --workers=10 \
    --duration=10s

  echo "Running 100 queries (keyword=error)..."
  go run "$REPO_ROOT/test/load/main.go" \
    --node-addr="localhost:$port" \
    --addr="localhost:$port" \
    --mode=query \
    --workers=5 \
    --duration=5s

  kill "$NODE_PID" 2>/dev/null || true
  wait "$NODE_PID" 2>/dev/null || true
}

run_bench "false" "bloom-disabled"
run_bench "true"  "bloom-enabled"
