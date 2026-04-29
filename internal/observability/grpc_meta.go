package observability

import (
	"context"

	"google.golang.org/grpc/metadata"
)

const (
	metaKeyRequestID = "x-request-id"
	metaKeyForwarded = "x-forwarded-ingest"
)

// OutgoingContextWithRequestID adds the request ID to outgoing gRPC metadata.
func OutgoingContextWithRequestID(ctx context.Context, id string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metaKeyRequestID, id)
}

// RequestIDFromIncomingContext extracts the request ID from incoming gRPC metadata.
// Returns "" if not present.
func RequestIDFromIncomingContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get(metaKeyRequestID)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

// OutgoingContextWithForwarded marks an outgoing gRPC call as a forwarded ingest hop.
func OutgoingContextWithForwarded(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metaKeyForwarded, "1")
}

// IsForwardedFromIncomingContext returns true if the incoming gRPC call was forwarded
// from another node's ingest server.
func IsForwardedFromIncomingContext(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	return len(md.Get(metaKeyForwarded)) > 0
}
