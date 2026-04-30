# Bloom Filter Benchmark Results

## Setup
- Dataset: 5,000 log entries, 10 segments (500 entries/segment)
- Query: `query_string = "error"` (matches ~10% of entries)
- Runs: 100 queries per configuration

## Results

| Configuration       | Segments Scanned / Query | p50 Latency (ms) | p95 Latency (ms) |
|---------------------|--------------------------|------------------|------------------|
| BLOOM_ENABLED=false | 2828                     | 11               | 18               |
| BLOOM_ENABLED=true  | TBD                      | 10               | 16               |

## Notes

Run `./test/bench/bloom_benchmark.sh` to reproduce. Update the table above with observed values.
