package point

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
)

type MetricPoint interface {
	Name() string
	ToJSONLine() (string, error)
}

type metricUnitType int

const (
	podCountUnit metricUnitType = iota
	milisecondsUnit
	gpuUnit
	percentUnit
)

func (t metricUnitType) String() string {
	switch t {
	case podCountUnit:
		return "pods"
	case milisecondsUnit:
		return "miliseconds"
	case gpuUnit:
		return "gpus"
	case percentUnit:
		return "percent"
	default:
		return "unknown"
	}
}

func (t metricUnitType) LogValue() slog.Value {
	return slog.StringValue(t.String())
}

type jsonlData struct {
	Metric     metricMetadata `json:"metric"`
	Values     []float64      `json:"values"`
	Timestamps []int64        `json:"timestamps"`
}

type metricMetadata struct {
	Name   string
	Labels map[string]string
}

func (m metricMetadata) MarshalJSON() ([]byte, error) {
	raw := map[string]json.RawMessage{
		"__name__": json.RawMessage(strconv.Quote(m.Name)),
	}
	for k, v := range m.Labels {
		raw[k] = json.RawMessage(strconv.Quote(v))
	}
	return json.Marshal(raw)
}

// e.g. virtualtime_created_at_miliseconds
// e.g. podcount_pods
func metricNameOf(name, kind string, unit metricUnitType) string {
	if kind == "" {
		return fmt.Sprintf("%s_%s", name, unit.String())
	}
	return fmt.Sprintf("%s_%s_%s", name, kind, unit.String())
}
