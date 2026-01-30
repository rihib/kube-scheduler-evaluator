package consts

import (
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/pkg/definition"
)

const (
	DefaultTimeout      = 5 * time.Minute
	DefaultShortTimeout = 5 * time.Second
)

type Consts interface {
	IsExternalMode() bool
	UserID() string
	EvaluationID() string
	Scenarios() []definition.Scenario
	StartedAt() time.Time
	MetricStores() []definition.MetricStore
}

type ConstsExternalMode interface {
	Consts
	KubeconfigPath() string
	EtcdPrefix() string
	EtcdURL() *url.URL
}

func NewConsts(
	userID string,
	scenarios []definition.Scenario,
	startedAt time.Time,
	metricStores []definition.MetricStore,
) (Consts, error) {
	// userID
	if userID == "" {
		return nil, fmt.Errorf("consts.NewConsts: userID is required")
	}
	userIDMatched, err := regexp.MatchString(`^[a-z][a-z0-9.]*$`, userID)
	if err != nil {
		return nil, fmt.Errorf("consts.NewConsts: %w", err)
	}
	if !userIDMatched {
		return nil, fmt.Errorf("consts.NewConsts: userID must contain only lowercase alphanumeric characters and dots, and start with an alphabet")
	}
	// evaluationID
	evaluationID := fmt.Sprintf(
		"evaluation-%s-%s", userID, startedAt.Format("20060102150405"),
	)
	// scenarios
	if len(scenarios) == 0 {
		return nil, fmt.Errorf("consts.NewConsts: scenarios are required")
	}
	return &consts{
		externalMode: false,
		userID:       userID,
		evaluationID: evaluationID,
		scenarios:    scenarios,
		startedAt:    startedAt,
		metricStores: metricStores,
	}, nil
}

type consts struct {
	externalMode bool
	userID       string
	evaluationID string
	metricStores []definition.MetricStore
	scenarios    []definition.Scenario
	startedAt    time.Time
}

func (c *consts) IsExternalMode() bool {
	return c.externalMode
}

func (c *consts) UserID() string {
	return c.userID
}

func (c *consts) EvaluationID() string {
	return c.evaluationID
}

func (c *consts) MetricStores() []definition.MetricStore {
	return c.metricStores
}

func (c *consts) Scenarios() []definition.Scenario {
	return c.scenarios
}

func (c *consts) StartedAt() time.Time {
	return c.startedAt
}

func NewConstsExternalMode(
	userID string,
	scenarios []definition.Scenario,
	startedAt time.Time,
	metricStores []definition.MetricStore,
	kubeconfigPath, etcdPrefix, etcdURLStr string,
) (ConstsExternalMode, error) {
	// consts
	c, err := NewConsts(
		userID,
		scenarios,
		startedAt,
		metricStores,
	)
	if err != nil {
		return nil, err
	}
	// externalMode
	c.(*consts).externalMode = true
	// kubeconfigPath
	if kubeconfigPath == "" {
		return nil, fmt.Errorf("consts.NewConsts: kubeconfigPath is required")
	}
	// etcdPrefix
	if etcdPrefix == "" {
		slog.Info("etcdPrefix is not set, using default value '/registry'")
		etcdPrefix = "/registry"
	}
	etcdPrefixMatched, err := regexp.MatchString(`^/[a-z0-9-]+$`, etcdPrefix)
	if err != nil {
		return nil, fmt.Errorf("consts.NewConsts: %w", err)
	}
	if !etcdPrefixMatched {
		return nil, fmt.Errorf("consts.NewConsts: etcdPrefix must start with '/' and contain only lowercase letters, numbers, and hyphens")
	}
	// etcdURL
	etcdURL, err := url.Parse(etcdURLStr)
	if err != nil || etcdURL.Scheme == "" || etcdURL.Host == "" {
		return nil, fmt.Errorf("consts.NewConsts: invalid etcd URL: %w", err)
	}
	return &constsExternalMode{
		consts:         c.(*consts),
		kubeconfigPath: kubeconfigPath,
		etcdPrefix:     etcdPrefix,
		etcdURL:        etcdURL,
	}, nil
}

type constsExternalMode struct {
	*consts
	kubeconfigPath string
	etcdPrefix     string
	etcdURL        *url.URL
}

func (c *constsExternalMode) KubeconfigPath() string {
	return c.kubeconfigPath
}

func (c *constsExternalMode) EtcdPrefix() string {
	return c.etcdPrefix
}

func (c *constsExternalMode) EtcdURL() *url.URL {
	return c.etcdURL
}
