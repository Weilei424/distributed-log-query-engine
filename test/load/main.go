package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
)

var (
	addr     = flag.String("addr", "localhost:9001", "coordinator gRPC address (used for query)")
	nodeAddr = flag.String("node-addr", "localhost:50051", "node gRPC address (used for ingest)")
	workers  = flag.Int("workers", 10, "concurrent goroutines per mode")
	duration = flag.Duration("duration", 30*time.Second, "test duration")
	mode     = flag.String("mode", "both", `"ingest", "query", or "both"`)
)

func main() {
	flag.Parse()

	// Ingest goes to a storage node; query goes to the coordinator fan-out.
	nodeConn, err := grpc.NewClient(*nodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("dial node %s: %v", *nodeAddr, err))
	}
	defer nodeConn.Close()

	coordConn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("dial coordinator %s: %v", *addr, err))
	}
	defer coordConn.Close()

	ingestClient := logengine.NewIngestServiceClient(nodeConn)
	queryClient := logengine.NewQueryServiceClient(coordConn)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var (
		ingestTotal  atomic.Int64
		ingestErrors atomic.Int64
		queryTotal   atomic.Int64
		queryErrors  atomic.Int64
		queryPartial atomic.Int64
	)
	var latencies []int64
	var latMu sync.Mutex
	var wg sync.WaitGroup

	services := []string{"auth", "billing", "api", "worker", "scheduler"}

	if *mode == "ingest" || *mode == "both" {
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						svc := services[rand.Intn(len(services))]
						_, err := ingestClient.IngestBatch(ctx, &logengine.IngestBatchRequest{
							Entries: []*logengine.LogEntry{{
								Service:   svc,
								Level:     "INFO",
								Message:   fmt.Sprintf("load test message %d", rand.Int63()),
								Timestamp: time.Now().UnixNano(),
							}},
						})
						if err != nil {
							ingestErrors.Add(1)
						} else {
							ingestTotal.Add(1)
						}
					}
				}
			}()
		}
	}

	if *mode == "query" || *mode == "both" {
		keywords := []string{"load", "test", "message", "auth", "billing"}
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						kw := keywords[rand.Intn(len(keywords))]
						start := time.Now()
						resp, err := queryClient.Query(ctx, &logengine.QueryRequest{
							QueryString: kw,
							Limit:   20,
						})
						ms := time.Since(start).Milliseconds()
						if err != nil {
							queryErrors.Add(1)
						} else {
							queryTotal.Add(1)
							latMu.Lock()
							latencies = append(latencies, ms)
							latMu.Unlock()
							if resp.Partial {
								queryPartial.Add(1)
							}
						}
					}
				}
			}()
		}
	}

	wg.Wait()
	secs := duration.Seconds()

	if *mode == "ingest" || *mode == "both" {
		total := ingestTotal.Load()
		fmt.Printf("--- Ingest ---\n")
		fmt.Printf("  total:    %d entries\n", total)
		fmt.Printf("  rate:     %.0f/s\n", float64(total)/secs)
		fmt.Printf("  errors:   %d\n\n", ingestErrors.Load())
	}

	if *mode == "query" || *mode == "both" {
		total := queryTotal.Load()
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		p50, p95 := int64(0), int64(0)
		if n := len(latencies); n > 0 {
			p50 = latencies[n*50/100]
			p95 = latencies[n*95/100]
		}
		var partialPct float64
		if total > 0 {
			partialPct = float64(queryPartial.Load()) / float64(total) * 100
		}
		fmt.Printf("--- Query ---\n")
		fmt.Printf("  total:    %d queries\n", total)
		fmt.Printf("  p50:      %dms\n", p50)
		fmt.Printf("  p95:      %dms\n", p95)
		fmt.Printf("  partial:  %.1f%%\n", partialPct)
		fmt.Printf("  errors:   %d\n", queryErrors.Load())
	}
}
