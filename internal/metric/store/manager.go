package metricstore

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	metricpoint "github.com/pfnet/kube-scheduler-evaluator/internal/metric/point"
)

type Manager interface {
	Name() string
	Flush(context.Context, []metricpoint.MetricPoint, *http.Client) error
}

type storeID int

const (
	victoriaMetricsStore storeID = iota
)

func (s storeID) String() string {
	switch s {
	case victoriaMetricsStore:
		return "victoriametrics"
	default:
		return "unknown"
	}
}

func (s storeID) LogValue() slog.Value {
	return slog.StringValue(s.String())
}

func toJSONLines(points []metricpoint.MetricPoint) (string, error) {
	var lines []string
	for _, point := range points {
		if point == nil {
			return "", fmt.Errorf("metric.toJSONLines: nil MetricPoint found")
		}
		line, err := point.ToJSONLine()
		if err != nil {
			return "", fmt.Errorf("metric.toJSONLines: %w", err)
		}
		if line == "" {
			return "", fmt.Errorf("metric.toJSONLines: received an empty line from a metric point")
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n"), nil
}
