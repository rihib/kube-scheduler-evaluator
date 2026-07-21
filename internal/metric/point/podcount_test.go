package point

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestPodCountMetricPointJSONLines(t *testing.T) {
	virtualTime := time.Unix(1_700_000_000, 0)
	point, err := NewPodCountMetricPoint(
		"example.com", "evaluation-1", "scenario-1", 0, 2, 1, virtualTime, time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	line, err := point.ToJSONLine()
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(line, "\n")
	if len(parts) != 2 {
		t.Fatalf("got %d JSON lines, want 2", len(parts))
	}
	got := map[string]float64{}
	for _, part := range parts {
		var data struct {
			Metric     map[string]string `json:"metric"`
			Values     []float64         `json:"values"`
			Timestamps []int64           `json:"timestamps"`
		}
		if err := json.Unmarshal([]byte(part), &data); err != nil {
			t.Fatal(err)
		}
		if len(data.Values) != 1 || len(data.Timestamps) != 1 {
			t.Fatalf("values=%v timestamps=%v", data.Values, data.Timestamps)
		}
		if data.Timestamps[0] != virtualTime.UnixMilli() {
			t.Errorf("timestamp=%d, want %d", data.Timestamps[0], virtualTime.UnixMilli())
		}
		got[data.Metric["phase"]] = data.Values[0]
	}
	if got["running"] != 2 || got["pending"] != 1 {
		t.Fatalf("got phase values %v", got)
	}
}
