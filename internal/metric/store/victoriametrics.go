package metricstore

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	metricpoint "github.com/pfnet/kube-scheduler-evaluator/internal/metric/point"
)

func NewVictoriaMetricsManager(url *url.URL) Manager {
	return &victoriaMetricsManager{
		url: url,
	}
}

type victoriaMetricsManager struct {
	url *url.URL
}

func (m *victoriaMetricsManager) Name() string {
	return victoriaMetricsStore.String()
}

func (m *victoriaMetricsManager) Flush(
	ctx context.Context,
	buff []metricpoint.MetricPoint,
	httpClient *http.Client,
) error {
	jsonl, err := toJSONLines(buff)
	if err != nil {
		return err
	}
	if jsonl == "" {
		return fmt.Errorf("metric.sendToVictoriaMetrics: no metrics to send")
	}
	endpoint := m.url.String() + "/api/v1/import"
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, endpoint, strings.NewReader(jsonl),
	)
	if err != nil {
		return fmt.Errorf("metric.sendToVictoriaMetrics: failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("metric.sendToVictoriaMetrics: failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("metric.sendToVictoriaMetrics: failed to read body: %w", err)
		}
		return fmt.Errorf(
			"metric.sendToVictoriaMetrics: unexpected response status %s, body: %s", resp.Status, string(body),
		)
	}
	slog.DebugContext(ctx, "successfully sent metrics to VictoriaMetrics", "count", len(buff))
	return nil
}
