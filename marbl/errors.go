package marbl

import "errors"

// ErrMaxBacklogReached is returned by Producer.Run when the broker queue
// depth hits MaxBacklog. It signals a clean intentional stop — not a crash.
var ErrMaxBacklogReached = errors.New("max backlog reached")