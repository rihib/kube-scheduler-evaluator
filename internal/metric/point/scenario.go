package point

import (
	"strconv"
	"time"
)

const (
	MetricScenarioWallDurationSeconds    = "kube_scheduler_evaluator_scenario_wall_duration_seconds"
	MetricScenarioVirtualDurationSeconds = "kube_scheduler_evaluator_scenario_virtual_duration_seconds"
	MetricScenarioSpeedupRatio           = "kube_scheduler_evaluator_scenario_speedup_ratio"
)

func NewScenarioSummaryMetricPoints(
	userID, evaluationID, scenarioID string,
	iteration int,
	wallDuration, virtualDuration time.Duration,
	timestamp time.Time,
) ([]MetricPoint, error) {
	labels := map[string]string{
		"user_id":       userID,
		"evaluation_id": evaluationID,
		"scenario_id":   scenarioID,
		"iteration":     strconv.Itoa(iteration),
	}
	speedup := float64(0)
	if wallDuration > 0 {
		speedup = virtualDuration.Seconds() / wallDuration.Seconds()
	}
	samples := []struct {
		name  string
		value float64
	}{
		{name: MetricScenarioWallDurationSeconds, value: wallDuration.Seconds()},
		{name: MetricScenarioVirtualDurationSeconds, value: virtualDuration.Seconds()},
		{name: MetricScenarioSpeedupRatio, value: speedup},
	}
	points := make([]MetricPoint, 0, len(samples))
	for _, s := range samples {
		point, err := NewScalarMetricPoint(s.name, labels, s.value, timestamp)
		if err != nil {
			return nil, err
		}
		points = append(points, point)
	}
	return points, nil
}
