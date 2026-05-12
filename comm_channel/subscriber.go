package comm_channel

import (
	"context"

	"github.com/ekefan/marbl/domain"
)

// TaskHandler is the function the consumer registers to process each task.
type TaskHandler func(ctx context.Context, task *domain.Task) error

// TaskSubscriber is the consumer-side transport contract.
type TaskSubscriber interface {
	// Serve starts consuming from the queue and dispatches each message to handler.
	// Blocks until ctx is cancelled or a fatal broker error occurs.
	// Each message is acked on handler success, nacked on handler error.
	Serve(ctx context.Context, handler TaskHandler) error

	// Close stops consuming and closes the channel cleanly.
	Close() error
}
