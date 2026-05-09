package metrics_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ekefan/marbl/metrics"
)

// newRegistry returns a fresh isolated registry for each test.
// Never use prometheus.DefaultRegisterer in tests — it's global and leaks between tests.
func newRegistry() *prometheus.Registry {
	return prometheus.NewRegistry()
}

// --- ProducerMetrics ---

func TestProducerMetrics_Registers(t *testing.T) {
	reg := newRegistry()
	m, err := metrics.NewProducerMetrics(reg)
	if err != nil {
		t.Fatalf("NewProducerMetrics() error: %v", err)
	}
	if m.TasksProduced == nil {
		t.Error("TasksProduced is nil")
	}
}

func TestProducerMetrics_CounterIncrements(t *testing.T) {
	reg := newRegistry()
	m, _ := metrics.NewProducerMetrics(reg)

	m.TasksProduced.Add(3)

	gathered, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error: %v", err)
	}

	for _, mf := range gathered {
		if mf.GetName() == "marbl_producer_tasks_produced_total" {
			got := mf.GetMetric()[0].GetCounter().GetValue()
			if got != 3 {
				t.Errorf("counter value: got %.0f, want 3", got)
			}
			return
		}
	}
	t.Error("metric marbl_producer_tasks_produced_total not found")
}

func TestProducerMetrics_DoubleRegisterFails(t *testing.T) {
	reg := newRegistry()
	_, err1 := metrics.NewProducerMetrics(reg)
	_, err2 := metrics.NewProducerMetrics(reg)

	if err1 != nil {
		t.Fatalf("first register failed: %v", err1)
	}
	if err2 == nil {
		t.Error("expected error on double register, got nil")
	}
}

// --- ConsumerMetrics ---

func TestConsumerMetrics_Registers(t *testing.T) {
	reg := newRegistry()
	m, err := metrics.NewConsumerMetrics(reg)
	if err != nil {
		t.Fatalf("NewConsumerMetrics() error: %v", err)
	}

	if m.TasksProcessing == nil {
		t.Error("TasksProcessing is nil")
	}
	if m.TasksDone == nil {
		t.Error("TasksDone is nil")
	}
	if m.TasksProcessedByType == nil {
		t.Error("TasksProcessedByType is nil")
	}
	if m.ValueSumByType == nil {
		t.Error("ValueSumByType is nil")
	}
}

func TestConsumerMetrics_GaugeTracksProcessing(t *testing.T) {
	reg := newRegistry()
	m, _ := metrics.NewConsumerMetrics(reg)

	m.TasksProcessing.Inc()
	m.TasksProcessing.Inc()
	m.TasksProcessing.Dec()

	gathered, _ := reg.Gather()
	for _, mf := range gathered {
		if mf.GetName() == "marbl_consumer_tasks_processing" {
			got := mf.GetMetric()[0].GetGauge().GetValue()
			if got != 1 {
				t.Errorf("gauge value: got %.0f, want 1", got)
			}
			return
		}
	}
	t.Error("metric marbl_consumer_tasks_processing not found")
}

func TestConsumerMetrics_CounterVecByType(t *testing.T) {
	reg := newRegistry()
	m, _ := metrics.NewConsumerMetrics(reg)

	m.TasksProcessedByType.WithLabelValues("3").Add(5)
	m.TasksProcessedByType.WithLabelValues("7").Add(2)

	gathered, _ := reg.Gather()
	for _, mf := range gathered {
		if mf.GetName() != "marbl_consumer_tasks_processed_by_type_total" {
			continue
		}
		found := map[string]float64{}
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "task_type" {
					found[lp.GetValue()] = metric.GetCounter().GetValue()
				}
			}
		}
		if found["3"] != 5 {
			t.Errorf("type 3 count: got %.0f, want 5", found["3"])
		}
		if found["7"] != 2 {
			t.Errorf("type 7 count: got %.0f, want 2", found["7"])
		}
		return
	}
	t.Error("metric marbl_consumer_tasks_processed_by_type_total not found")
}

func TestConsumerMetrics_ValueSumGaugeVec(t *testing.T) {
	reg := newRegistry()
	m, _ := metrics.NewConsumerMetrics(reg)

	m.ValueSumByType.WithLabelValues("2").Add(30)
	m.ValueSumByType.WithLabelValues("2").Add(15)

	gathered, _ := reg.Gather()
	for _, mf := range gathered {
		if mf.GetName() != "marbl_consumer_value_sum_by_type" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "task_type" && lp.GetValue() == "2" {
					got := metric.GetGauge().GetValue()
					if got != 45 {
						t.Errorf("value sum type 2: got %.0f, want 45", got)
					}
					return
				}
			}
		}
	}
	t.Error("metric marbl_consumer_value_sum_by_type not found")
}

// --- Metrics HTTP server ---

func TestMetricsServer_ExposesMetricsEndpoint(t *testing.T) {
	reg := newRegistry()
	m, _ := metrics.NewProducerMetrics(reg)
	m.TasksProduced.Add(7)

	srv := metrics.NewServer(":0", reg, nil)

	// use a known free port for testing
	srv2 := metrics.NewServer("127.0.0.1:0", reg, nil)
	_ = srv2

	// start on a fixed test port
	testSrv := metrics.NewServer("127.0.0.1:19091", reg, nil)
	testSrv.Start()

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = testSrv.Shutdown(ctx)
	}()

	// give server a moment to start
	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:19091/metrics")
	if err != nil {
		t.Fatalf("GET /metrics error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "marbl_producer_tasks_produced_total") {
		t.Error("response does not contain expected metric name")
	}

	_ = srv
}

func TestMetricsServer_Shutdown(t *testing.T) {
	reg := newRegistry()
	srv := metrics.NewServer("127.0.0.1:19092", reg, nil)
	srv.Start()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown() error: %v", err)
	}

	// server should no longer accept connections
	time.Sleep(50 * time.Millisecond)
	_, err := http.Get("http://127.0.0.1:19092/metrics")
	if err == nil {
		t.Error("expected connection refused after shutdown, got nil")
	}
}