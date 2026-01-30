package metric

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/consts"
	metricpoint "github.com/pfnet/kube-scheduler-evaluator/internal/metric/point"
	metricstore "github.com/pfnet/kube-scheduler-evaluator/internal/metric/store"
	"github.com/pfnet/kube-scheduler-evaluator/pkg/definition"
)

type Manager interface {
	ScenarioID() string
	SetScenarioID(scenarioID string)
	Iteration() int
	SetIteration(iteration int)
	Start(ctx context.Context)
	Close()
	Wait()
	Record(metric metricpoint.MetricPoint)
}

func NewManager(c consts.Consts) (Manager, error) {
	batchSize := 50
	metricCh := make(chan metricpoint.MetricPoint, batchSize*10)
	storeInfos := c.MetricStores()
	var stores []metricstore.Manager
	for _, info := range storeInfos {
		switch info.ID() {
		case definition.VictoriaMetrics:
			stores = append(stores, metricstore.NewVictoriaMetricsManager(info.URL()))
		default:
			return nil, fmt.Errorf("metric.NewManager: unknown metric store id %s", info.ID())
		}
	}
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}
	return &manager{
		consts:     c,
		batchSize:  batchSize,
		metricCh:   metricCh,
		stores:     stores,
		httpClient: httpClient,
	}, nil
}

type manager struct {
	consts     consts.Consts
	scenarioID string
	iteration  int
	batchSize  int
	metricCh   chan metricpoint.MetricPoint
	wg         sync.WaitGroup
	stores     []metricstore.Manager
	httpClient *http.Client
}

func (m *manager) Consts() consts.Consts {
	return m.consts
}

func (m *manager) ScenarioID() string {
	return m.scenarioID
}

func (m *manager) SetScenarioID(scenarioID string) {
	m.scenarioID = scenarioID
}

func (m *manager) Iteration() int {
	return m.iteration
}

func (m *manager) SetIteration(iteration int) {
	m.iteration = iteration
}

func (m *manager) Start(ctx context.Context) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.flushLoop(ctx)
	}()
}

func (m *manager) Close() {
	close(m.metricCh)
}

func (m *manager) Wait() {
	m.wg.Wait()
}

func (m *manager) Record(metric metricpoint.MetricPoint) {
	m.metricCh <- metric
}

func (m *manager) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var buff []metricpoint.MetricPoint
	for {
		select {
		case <-ctx.Done():
			for metric := range m.metricCh {
				buff = append(buff, metric)
			}
			m.flush(ctx, buff, reasonShutdown)
			slog.DebugContext(ctx, "metric.flushLoop: finished on context done")
			return
		case metric, ok := <-m.metricCh:
			if !ok {
				m.flush(ctx, buff, reasonChannel)
				slog.DebugContext(ctx, "metric.flushLoop: finished on channel closed")
				return
			}
			buff = append(buff, metric)
			if len(buff) >= m.batchSize {
				buff = m.flush(ctx, buff, reasonSize)
			}
		case <-ticker.C:
			buff = m.flush(ctx, buff, reasonTicker)
		}
	}
}

func (m *manager) flush(
	ctx context.Context,
	buff []metricpoint.MetricPoint,
	reason flushReason,
) []metricpoint.MetricPoint {
	if len(buff) > 0 {
		slog.DebugContext(ctx, "sending metrics batch", "reason", reason, "count", len(buff))
		for _, store := range m.stores {
			if err := store.Flush(ctx, buff, m.httpClient); err != nil {
				slog.ErrorContext(
					ctx, "metric.flush: failed to flush metrics to store",
					"store", store.Name(),
					"error", err,
				)
			}
		}
		buff = nil
	}
	return buff
}

type flushReason string

const (
	reasonShutdown flushReason = "shutdown"
	reasonChannel  flushReason = "channel closed"
	reasonSize     flushReason = "size"
	reasonTicker   flushReason = "ticker"
)
