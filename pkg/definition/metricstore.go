package definition

import (
	"fmt"
	"log/slog"
	"net/url"
)

// MetricStore represents a storage backend for metrics,
// such as VictoriaMetrics.
type MetricStore interface {
	ID() MetricStoreID // ID returns the MetricStoreID.
	URL() *url.URL     // URL returns the URL of the MetricStore.
}

// NewMetricStore creates a new MetricStore instance.
func NewMetricStore(id MetricStoreID, storeURL string) MetricStore {
	if id.String() == "unknown" {
		panic("metricstore.MetricStore: invalid metric store id")
	}
	u, err := url.Parse(storeURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		panic(fmt.Errorf("metricstore.MetricStore.Validate: invalid metric store URL: %w", err))
	}
	return metricStore{
		id:  id,
		url: u,
	}
}

type metricStore struct {
	id  MetricStoreID
	url *url.URL
}

func (m metricStore) ID() MetricStoreID {
	return m.id
}

func (m metricStore) URL() *url.URL {
	return m.url
}

type MetricStoreID int

const (
	VictoriaMetrics MetricStoreID = iota
)

func (t MetricStoreID) String() string {
	switch t {
	case VictoriaMetrics:
		return "victoriametrics"
	default:
		return "unknown"
	}
}

func (t MetricStoreID) LogValue() slog.Value {
	return slog.StringValue(t.String())
}
