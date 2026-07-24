package point

import (
	"encoding/json"
	"testing"
	"time"
)

func TestScalarMetricPointToJSONLine(t *testing.T) {
	timestamp := time.UnixMilli(1_700_000_000_123)
	point, err := NewScalarMetricPoint(
		"demo_ratio",
		map[string]string{"scenario_id": "scenario-gpu2023"},
		0.625,
		timestamp,
	)
	if err != nil {
		t.Fatal(err)
	}
	line, err := point.ToJSONLine()
	if err != nil {
		t.Fatal(err)
	}
	var got jsonlData
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Values) != 1 || got.Values[0] != 0.625 {
		t.Fatalf("unexpected values: %v", got.Values)
	}
	if len(got.Timestamps) != 1 || got.Timestamps[0] != timestamp.UnixMilli() {
		t.Fatalf("unexpected timestamps: %v", got.Timestamps)
	}
}

func TestClusterSnapshotRatios(t *testing.T) {
	points, err := NewClusterSnapshotMetricPoints(
		"example.com", "evaluation-1", "scenario-gpu2023", 0,
		1523, 100, 5,
		100_000, 62_500,
		10, 7, 6_000,
		time.Unix(1_700_000_000, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 10 {
		t.Fatalf("got %d points, want 10", len(points))
	}
	values := metricValues(t, points)
	if got := values[MetricClusterCPUUtilizationRatio]; got != 0.625 {
		t.Fatalf("CPU ratio = %v, want 0.625", got)
	}
	if got := values[MetricClusterGPUUtilizationRatio]; got != 0.6 {
		t.Fatalf("GPU ratio = %v, want 0.6", got)
	}
}

func TestScenarioSummarySpeedup(t *testing.T) {
	points, err := NewScenarioSummaryMetricPoints(
		"example.com", "evaluation-1", "scenario-gpu2023", 0,
		2*time.Second, 100*time.Second,
		time.Unix(1_700_000_000, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	values := metricValues(t, points)
	if got := values[MetricScenarioSpeedupRatio]; got != 50 {
		t.Fatalf("speedup = %v, want 50", got)
	}
}

func metricValues(t *testing.T, points []MetricPoint) map[string]float64 {
	t.Helper()
	values := make(map[string]float64, len(points))
	for _, point := range points {
		line, err := point.ToJSONLine()
		if err != nil {
			t.Fatal(err)
		}
		var data jsonlData
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			t.Fatal(err)
		}
		values[point.Name()] = data.Values[0]
	}
	return values
}
