package point

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

func NewPodCountMetricPoint(
	userID string,
	evaluationID string,
	scenarioID string,
	iteration int,
	runningCount int,
	pendingCount int,
	virtualTime time.Time,
	realTime time.Time,
) (MetricPoint, error) {
	if userID == "" {
		return nil, fmt.Errorf("metricpoint.NewPodCountMetricPoint: userID is required")
	}
	if evaluationID == "" {
		return nil, fmt.Errorf("metricpoint.NewPodCountMetricPoint: evaluationID is required")
	}
	if scenarioID == "" {
		return nil, fmt.Errorf("metricpoint.NewPodCountMetricPoint: scenarioID is required")
	}
	if iteration < 0 {
		return nil, fmt.Errorf("metricpoint.NewPodCountMetricPoint: iteration must be non-negative")
	}
	if runningCount < 0 {
		return nil, fmt.Errorf("metricpoint.NewPodCountMetricPoint: runningCount must be non-negative")
	}
	if pendingCount < 0 {
		return nil, fmt.Errorf("metricpoint.NewPodCountMetricPoint: pendingCount must be non-negative")
	}
	if virtualTime.IsZero() {
		return nil, fmt.Errorf("metricpoint.NewPodCountMetricPoint: virtualTime is required")
	}
	if realTime.IsZero() {
		return nil, fmt.Errorf("metricpoint.NewPodCountMetricPoint: realTime is required")
	}
	return &PodCountMetricPoint{
		userID:       userID,
		evaluationID: evaluationID,
		scenarioID:   scenarioID,
		iteration:    iteration,
		runningCount: runningCount,
		pendingCount: pendingCount,
		virtualTime:  virtualTime,
		realTime:     realTime,
	}, nil
}

type PodCountMetricPoint struct {
	userID       string
	evaluationID string
	scenarioID   string
	iteration    int

	runningCount int
	pendingCount int

	virtualTime time.Time
	realTime    time.Time
}

func (p *PodCountMetricPoint) Name() string {
	return "podcount"
}

func (p *PodCountMetricPoint) ToJSONLine() (string, error) {
	data := jsonlData{
		Metric: metricMetadata{
			Name: metricNameOf(p.Name(), "", podCountUnit),
			Labels: map[string]string{
				"user_id":       p.userID,
				"evaluation_id": p.evaluationID,
				"scenario_id":   p.scenarioID,
				"iteration":     strconv.Itoa(p.iteration),
			},
		},
		Values: []float64{
			float64(p.virtualTime.UnixMilli()),
			float64(p.runningCount),
			float64(p.pendingCount),
		},
		Timestamps: []int64{p.realTime.UnixMilli()},
	}
	line, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(line), nil
}
