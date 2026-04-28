package observability

import "github.com/prometheus/client_golang/prometheus"

var (
	IngestRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logengine_ingest_requests_total",
		Help: "Total log entries processed by the ingest server.",
	}, []string{"node_id", "status"})

	AppendDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "logengine_append_duration_seconds",
		Help:    "Latency of segment append operations.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0},
	}, []string{"node_id"})

	ActiveSegmentBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logengine_active_segment_bytes",
		Help: "Current size of the active segment file in bytes.",
	}, []string{"node_id"})

	MountedSegmentsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logengine_mounted_segments_total",
		Help: "Number of segment files currently open.",
	}, []string{"node_id"})

	IndexTokenCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logengine_index_token_count",
		Help: "Number of unique tokens in the in-memory inverted index.",
	}, []string{"node_id"})

	QueryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "logengine_query_duration_seconds",
		Help:    "Latency of query execution.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0},
	}, []string{"node_id", "type"})

	FanOutTimeoutsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logengine_fanout_timeouts_total",
		Help: "Number of per-node fan-out timeouts.",
	})

	FanOutPartialTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logengine_fanout_partial_total",
		Help: "Number of fan-out responses returned with partial=true.",
	})

	NodeHealthStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logengine_node_health_status",
		Help: "Node health: 1 = healthy, 0 = unhealthy.",
	}, []string{"node_id"})

	ReplicationLagEntries = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logengine_replication_lag_entries",
		Help: "Number of entries pending replication per target address.",
	}, []string{"node_id"})
)

// Register wires all metrics into reg. Call once at process startup.
func Register(reg prometheus.Registerer) {
	reg.MustRegister(
		IngestRequestsTotal,
		AppendDuration,
		ActiveSegmentBytes,
		MountedSegmentsTotal,
		IndexTokenCount,
		QueryDuration,
		FanOutTimeoutsTotal,
		FanOutPartialTotal,
		NodeHealthStatus,
		ReplicationLagEntries,
	)
}
