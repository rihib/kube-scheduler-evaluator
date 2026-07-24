package point

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// NewGPUAllocationMetricPoints creates the metrics needed to inspect GPU
// allocation at one point in virtual time. allocated is derived from Pod GPU
// requests; it is not a measurement of hardware utilization.
func NewGPUAllocationMetricPoints(
	userID string,
	evaluationID string,
	scenarioID string,
	iteration int,
	allocatable int64,
	allocated int64,
	virtualTime time.Time,
) ([]MetricPoint, error) {
	if userID == "" {
		return nil, fmt.Errorf("metricpoint.NewGPUAllocationMetricPoints: userID is required")
	}
	if evaluationID == "" {
		return nil, fmt.Errorf("metricpoint.NewGPUAllocationMetricPoints: evaluationID is required")
	}
	if scenarioID == "" {
		return nil, fmt.Errorf("metricpoint.NewGPUAllocationMetricPoints: scenarioID is required")
	}
	if iteration < 0 {
		return nil, fmt.Errorf("metricpoint.NewGPUAllocationMetricPoints: iteration must be non-negative")
	}
	if allocatable < 0 || allocated < 0 || allocated > allocatable {
		return nil, fmt.Errorf("metricpoint.NewGPUAllocationMetricPoints: invalid GPU allocation: allocated=%d allocatable=%d", allocated, allocatable)
	}
	if virtualTime.IsZero() {
		return nil, fmt.Errorf("metricpoint.NewGPUAllocationMetricPoints: virtualTime is required")
	}

	labels := map[string]string{
		"user_id":       userID,
		"evaluation_id": evaluationID,
		"scenario_id":   scenarioID,
		"iteration":     strconv.Itoa(iteration),
	}
	utilization := float64(0)
	if allocatable > 0 {
		utilization = float64(allocated) / float64(allocatable) * 100
	}
	return []MetricPoint{
		newGPUAllocationMetricPoint("allocatable", gpuUnit, float64(allocatable), labels, virtualTime),
		newGPUAllocationMetricPoint("allocated", gpuUnit, float64(allocated), labels, virtualTime),
		newGPUAllocationMetricPoint("free", gpuUnit, float64(allocatable-allocated), labels, virtualTime),
		newGPUAllocationMetricPoint("allocation", percentUnit, utilization, labels, virtualTime),
	}, nil
}

// NewGPUAverageAllocationMetricPoint records a scenario's time-weighted
// cluster GPU allocation average.
func NewGPUAverageAllocationMetricPoint(
	userID string,
	evaluationID string,
	scenarioID string,
	iteration int,
	average float64,
	virtualTime time.Time,
) (MetricPoint, error) {
	if userID == "" || evaluationID == "" || scenarioID == "" {
		return nil, fmt.Errorf("metricpoint.NewGPUAverageAllocationMetricPoint: metric labels are required")
	}
	if iteration < 0 {
		return nil, fmt.Errorf("metricpoint.NewGPUAverageAllocationMetricPoint: iteration must be non-negative")
	}
	if average < 0 || average > 100 {
		return nil, fmt.Errorf("metricpoint.NewGPUAverageAllocationMetricPoint: average must be between 0 and 100")
	}
	if virtualTime.IsZero() {
		return nil, fmt.Errorf("metricpoint.NewGPUAverageAllocationMetricPoint: virtualTime is required")
	}
	labels := map[string]string{
		"user_id":       userID,
		"evaluation_id": evaluationID,
		"scenario_id":   scenarioID,
		"iteration":     strconv.Itoa(iteration),
	}
	return newGPUAllocationMetricPoint("average_allocation", percentUnit, average, labels, virtualTime), nil
}

func newGPUAllocationMetricPoint(kind string, unit metricUnitType, value float64, labels map[string]string, virtualTime time.Time) MetricPoint {
	labelCopy := make(map[string]string, len(labels))
	for key, value := range labels {
		labelCopy[key] = value
	}
	return &gpuAllocationMetricPoint{
		kind:        kind,
		unit:        unit,
		value:       value,
		labels:      labelCopy,
		virtualTime: virtualTime,
	}
}

type gpuAllocationMetricPoint struct {
	kind        string
	unit        metricUnitType
	value       float64
	labels      map[string]string
	virtualTime time.Time
}

func (p *gpuAllocationMetricPoint) Name() string {
	return "gpu"
}

func (p *gpuAllocationMetricPoint) ToJSONLine() (string, error) {
	data := jsonlData{
		Metric: metricMetadata{
			Name:   metricNameOf(p.Name(), p.kind, p.unit),
			Labels: p.labels,
		},
		Values:     []float64{p.value},
		Timestamps: []int64{p.virtualTime.UnixMilli()},
	}
	line, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(line), nil
}
