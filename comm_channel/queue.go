package comm_channel

import "context"

// QueueDepthChecker lets the producer query how many unprocessed messages
// are sitting in the broker queue, used to enforce max backlog.

// Deprecated: now using maxbacklog == state count for received + state count for unprocessed
type QueueDepthChecker interface {
	// QueueDepth returns the current number of ready (unacked) messages in the queue.
	QueueDepth(ctx context.Context) (int64, error)
}