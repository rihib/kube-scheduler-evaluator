package point

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewGPUAllocationMetricPoints(t *testing.T) {
	virtualTime := time.Unix(1_700_000_000, 0)
	points, err := NewGPUAllocationMetricPoints(
		"example.com", "evaluation-1", "scenario-best-fit", 0, 8, 5, virtualTime,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 4 {
		t.Fatalf("got %d points, want 4", len(points))
	}

	wantNames := []string{
		"gpu_allocatable_gpus",
		"gpu_allocated_gpus",
		"gpu_free_gpus",
		"gpu_allocation_percent",
	}
	wantValues := []float64{8, 5, 3, 62.5}
	for i, point := range points {
		line, err := point.ToJSONLine()
		if err != nil {
			t.Fatal(err)
		}
		var data struct {
			Metric     map[string]string `json:"metric"`
			Values     []float64         `json:"values"`
			Timestamps []int64           `json:"timestamps"`
		}
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			t.Fatal(err)
		}
		if data.Metric["__name__"] != wantNames[i] {
			t.Errorf("point %d name = %q, want %q", i, data.Metric["__name__"], wantNames[i])
		}
		if len(data.Values) != 1 || data.Values[0] != wantValues[i] {
			t.Errorf("point %d values = %v, want [%v]", i, data.Values, wantValues[i])
		}
		if len(data.Timestamps) != 1 || data.Timestamps[0] != virtualTime.UnixMilli() {
			t.Errorf("point %d timestamps = %v, want [%d]", i, data.Timestamps, virtualTime.UnixMilli())
		}
		if _, ok := data.Metric["scope"]; ok {
			t.Errorf("point %d labels = %v", i, data.Metric)
		}
		if _, ok := data.Metric["node"]; ok {
			t.Errorf("point %d labels = %v", i, data.Metric)
		}
	}
}

func TestNewGPUAllocationMetricPointsRejectsOverAllocation(t *testing.T) {
	_, err := NewGPUAllocationMetricPoints(
		"example.com", "evaluation-1", "scenario-best-fit", 0, 4, 5, time.Now(),
	)
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestNewGPUAverageAllocationMetricPoint(t *testing.T) {
	virtualTime := time.Unix(1_700_000_000, 0)
	point, err := NewGPUAverageAllocationMetricPoint(
		"example.com", "evaluation-1", "scenario-best-fit", 0, 85.5, virtualTime,
	)
	if err != nil {
		t.Fatal(err)
	}
	line, err := point.ToJSONLine()
	if err != nil {
		t.Fatal(err)
	}
	var data struct {
		Metric map[string]string `json:"metric"`
		Values []float64         `json:"values"`
	}
	if err := json.Unmarshal([]byte(line), &data); err != nil {
		t.Fatal(err)
	}
	if data.Metric["__name__"] != "gpu_average_allocation_percent" {
		t.Errorf("name = %q", data.Metric["__name__"])
	}
	if len(data.Values) != 1 || data.Values[0] != 85.5 {
		t.Errorf("values = %v", data.Values)
	}
}
