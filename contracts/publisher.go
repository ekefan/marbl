// contracts: interfaces storage and transport must implement to handle tasks
package contracts

import (
	"context"

	"github.com/ekefan/marbl/tasks"
)

// TaskPublisher is the producer-side transport contract.
type TaskPublisher interface {
	// Publish sends a task to the broker and waits for broker confirmation.
	// Returns an error if the broker rejects or is unreachable.
	Publish(ctx context.Context, task *tasks.Task) error

	// Close tears down the channel and connection cleanly.
	Close() error
}
