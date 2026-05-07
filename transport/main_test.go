package transport_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ekefan/marbl/tasks"
	"github.com/ekefan/marbl/transport"
)

const testQueue = "test_tasks"

// brokerDSN is set once in TestMain and shared across all test files.
var brokerDSN string

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "rabbitmq:3.13-alpine",
			ExposedPorts: []string{"5672/tcp"},
			WaitingFor: wait.ForLog("Server startup complete").
				WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		log.Fatalf("start rabbitmq container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		log.Fatalf("get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5672")
	if err != nil {
		log.Fatalf("get container port: %v", err)
	}

	brokerDSN = fmt.Sprintf("amqp://guest:guest@%s:%s/", host, port.Port())

	code := m.Run()

	if err := container.Terminate(ctx); err != nil {
		log.Printf("terminate rabbitmq container: %v", err)
	}

	os.Exit(code)
}

// --- shared test helpers ---

func newPublisher(t *testing.T) *transport.Publisher {
	t.Helper()
	pub, err := transport.NewPublisher(transport.PublisherConfig{
		DSN:       brokerDSN,
		QueueName: testQueue,
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(func() { pub.Close() })
	return pub
}

func newSubscriber(t *testing.T) *transport.Subscriber {
	t.Helper()
	sub, err := transport.NewSubscriber(transport.SubscriberConfig{
		DSN:       brokerDSN,
		QueueName: testQueue,
		Prefetch:  10,
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	t.Cleanup(func() { sub.Close() })
	return sub
}

func newTask(t *testing.T, id int64, typ int, val int) *tasks.Task {
	t.Helper()
	task, err := tasks.New(id, tasks.TaskType(typ), tasks.TaskValue(val))
	if err != nil {
		t.Fatalf("new task: %v", err)
	}
	return task
}

// purgeQueue drains all messages from the queue between tests
// so each test starts with a clean slate.
func purgeQueue(t *testing.T) {
	t.Helper()
	pub := newPublisher(t)
	if err := pub.PurgeQueue(context.Background()); err != nil {
		t.Fatalf("purge queue: %v", err)
	}
}