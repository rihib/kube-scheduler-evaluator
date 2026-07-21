package point

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"
)

func NewGPUMetricPoint(
	kind GPUMetricKind,
	value float64,
	userID string,
	evaluationID string,
	scenarioID string,
	iteration int,
	virtualTime time.Time,
	realTime time.Time,
) (MetricPoint, error) {
	if kind.String() == "unknown" {
		return nil, fmt.Errorf("metricpoint.NewGPUMetricPoint: invalid kind")
	}
	if value < 0 {
		return nil, fmt.Errorf("metricpoint.NewGPUMetricPoint: value must be non-negative")
	}
	if userID == "" {
		return nil, fmt.Errorf("metricpoint.NewGPUMetricPoint: userID is required")
	}
	if evaluationID == "" {
		return nil, fmt.Errorf("metricpoint.NewGPUMetricPoint: evaluationID is required")
	}
	if scenarioID == "" {
		return nil, fmt.Errorf("metricpoint.NewGPUMetricPoint: scenarioID is required")
	}
	if iteration < 0 {
		return nil, fmt.Errorf("metricpoint.NewGPUMetricPoint: iteration must be non-negative")
	}
	if virtualTime.IsZero() {
		return nil, fmt.Errorf("metricpoint.NewGPUMetricPoint: virtualTime is required")
	}
	if realTime.IsZero() {
		return nil, fmt.Errorf("metricpoint.NewGPUMetricPoint: realTime is required")
	}
	return &gpuMetricPoint{
		kind:         kind,
		value:        value,
		userID:       userID,
		evaluationID: evaluationID,
		scenarioID:   scenarioID,
		iteration:    iteration,
		virtualTime:  virtualTime,
		realTime:     realTime,
	}, nil
}

type gpuMetricPoint struct {
	kind  GPUMetricKind
	value float64

	userID       string
	evaluationID string
	scenarioID   string
	iteration    int

	virtualTime time.Time
	realTime    time.Time
}

func (p *gpuMetricPoint) Name() string {
	return "gpu"
}

func (p *gpuMetricPoint) ToJSONLine() (string, error) {
	data := jsonlData{
		Metric: metricMetadata{
			Name: metricNameOf(p.Name(), p.kind.String(), p.kind.Unit()),
			Labels: map[string]string{
				"user_id":       p.userID,
				"evaluation_id": p.evaluationID,
				"scenario_id":   p.scenarioID,
				"iteration":     strconv.Itoa(p.iteration),
			},
		},
		Values:     []float64{p.value},
		Timestamps: []int64{p.realTime.UnixMilli()},
	}
	line, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(line), nil
}

type GPUMetricKind int

const (
	// GPUCapacity is the total number of allocatable GPUs on all nodes.
	GPUCapacity GPUMetricKind = iota
	// GPUAllocated is the number of GPUs requested by running (scheduled) pods.
	GPUAllocated
	// GPUPending is the number of GPUs requested by pending (unscheduled) pods.
	GPUPending
	// GPUUtilization is GPUAllocated / GPUCapacity in percent.
	GPUUtilization
)

func (k GPUMetricKind) String() string {
	switch k {
	case GPUCapacity:
		return "capacity"
	case GPUAllocated:
		return "allocated"
	case GPUPending:
		return "pending"
	case GPUUtilization:
		return "utilization"
	default:
		return "unknown"
	}
}

func (k GPUMetricKind) Unit() metricUnitType {
	if k == GPUUtilization {
		return percentUnit
	}
	return gpuCountUnit
}

func (k GPUMetricKind) LogValue() slog.Value {
	return slog.StringValue(k.String())
}
