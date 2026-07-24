package point

import (
	"encoding/json"
	"fmt"
	"time"
)

// NewScalarMetricPoint creates a single Prometheus-style gauge sample.
func NewScalarMetricPoint(
	name string,
	labels map[string]string,
	value float64,
	timestamp time.Time,
) (MetricPoint, error) {
	if name == "" {
		return nil, fmt.Errorf("metricpoint.NewScalarMetricPoint: name is required")
	}
	if timestamp.IsZero() {
		return nil, fmt.Errorf("metricpoint.NewScalarMetricPoint: timestamp is required")
	}
	return &scalarMetricPoint{
		name:      name,
		labels:    labels,
		value:     value,
		timestamp: timestamp,
	}, nil
}

type scalarMetricPoint struct {
	name      string
	labels    map[string]string
	value     float64
	timestamp time.Time
}

func (p *scalarMetricPoint) Name() string {
	return p.name
}

func (p *scalarMetricPoint) ToJSONLine() (string, error) {
	data := jsonlData{
		Metric: metricMetadata{
			Name:   p.name,
			Labels: p.labels,
		},
		Values:     []float64{p.value},
		Timestamps: []int64{p.timestamp.UnixMilli()},
	}
	line, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(line), nil
}
