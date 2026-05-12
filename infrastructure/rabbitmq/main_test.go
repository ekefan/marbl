package rabbitmq_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ekefan/marbl/domain"
	"github.com/ekefan/marbl/infrastructure/rabbitmq"
)

const testQueue = "test_tasks"

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

func newPublisher(t *testing.T, name string) *rabbitmq.Publisher {
	t.Helper()
	pub, err := rabbitmq.NewPublisher(rabbitmq.PublisherConfig{
		DSN:       brokerDSN,
		QueueName: testQueue+ fmt.Sprintf("%s", name),
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(func() { pub.Close() })
	return pub
}

func newSubscriber(t *testing.T, name string) *rabbitmq.Subscriber {
	t.Helper()
	sub, err := rabbitmq.NewSubscriber(rabbitmq.SubscriberConfig{
		DSN:       brokerDSN,
		QueueName: testQueue+ fmt.Sprintf("%s", name),
		Prefetch:  10,
	})
	if err != nil {
		t.Fatalf("new subscriber: %v", err)
	}
	t.Cleanup(func() { sub.Close() })
	return sub
}

func newTask(t *testing.T, id int64, typ int, val int) *domain.Task {
	t.Helper()
	task, err := domain.NewTask(id, domain.TaskType(typ), domain.TaskValue(val))
	if err != nil {
		t.Fatalf("new task: %v", err)
	}
	return task
}

func purgeQueue(t *testing.T, pub *rabbitmq.Publisher) {
	t.Helper()
	if err := pub.PurgeQueue(context.Background()); err != nil {
		t.Fatalf("purge queue: %v", err)
	}
}