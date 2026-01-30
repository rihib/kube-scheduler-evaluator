package evaluator

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/cluster"
	"github.com/pfnet/kube-scheduler-evaluator/internal/consts"
	"github.com/pfnet/kube-scheduler-evaluator/internal/evaluation"
	"github.com/pfnet/kube-scheduler-evaluator/internal/metric"
	"github.com/pfnet/kube-scheduler-evaluator/internal/util"
	"github.com/pfnet/kube-scheduler-evaluator/pkg/definition"
	"k8s.io/kubernetes/pkg/scheduler"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

// Evaluator is the main interface for running the evaluator.
type Evaluator interface {
	Run(context.Context) int // Run starts the evaluator and runs the evaluation process.
}

// New creates a new Evaluator instance.
func New(
	config config,
	metricStores []definition.MetricStore,
	scenarios ...definition.Scenario,
) (Evaluator, error) {
	c, err := consts.NewConsts(
		config.userID,
		scenarios,
		time.Now(),
		metricStores,
	)
	if config.externalMode {
		c, err = consts.NewConstsExternalMode(
			config.userID,
			scenarios,
			time.Now(),
			metricStores,
			config.kubeconfigPath,
			config.etcdPrefix,
			config.etcdURL,
		)
	}
	if err != nil {
		return nil, err
	}
	metricManager, err := metric.NewManager(c)
	if err != nil {
		return nil, err
	}
	resourceManager, err := cluster.NewManager(c, metricManager)
	if err != nil {
		return nil, err
	}
	evaluationManager, err := evaluation.NewManager(c, resourceManager, metricManager)
	if err != nil {
		return nil, err
	}
	return &evaluator{
		evaluationManager: evaluationManager,
		metricManager:     metricManager,
		consts:            c,
		slogLevel:         config.slogLevel,
	}, nil
}

type evaluator struct {
	evaluationManager evaluation.Manager
	metricManager     metric.Manager
	consts            consts.Consts
	slogLevel         slog.Level
}

func (e *evaluator) Run(ctx context.Context) int {
	ctx = util.WithLogAttrs(ctx, "evaluationID", e.consts.EvaluationID())
	util.SetupLogger(e.slogLevel, e.consts.EvaluationID())
	slog.InfoContext(ctx, "starting kube-scheduler-evaluator")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		slog.InfoContext(ctx, "shutting down kube-scheduler-evaluator")
		cancel()
	}()

	e.metricManager.Start(ctx)
	if err := e.evaluationManager.Run(ctx); err != nil {
		slog.ErrorContext(ctx, "cmd.Run: failed to run evaluator", "error", err)
		return 1
	}
	e.metricManager.Close()
	e.metricManager.Wait()

	finishedAt := time.Now()
	slog.InfoContext(ctx, "kube-scheduler-evaluator finished successfully",
		"startedAt", e.consts.StartedAt(),
		"finishedAt", finishedAt,
		"duration", finishedAt.Sub(e.consts.StartedAt()).String(),
	)
	return 0
}

type config struct {
	externalMode   bool
	userID         string
	slogLevel      slog.Level
	kubeconfigPath string
	etcdPrefix     string
	etcdURL        string
}

// WithConfig creates a config instance for the evaluator.
func WithConfig(
	userID, kubeconfigPath, etcdPrefix, etcdURL string,
	externalMode bool,
	slogLevel slog.Level,
) config {
	cfg := config{
		externalMode:   externalMode,
		userID:         userID,
		slogLevel:      slogLevel,
		kubeconfigPath: kubeconfigPath,
		etcdPrefix:     etcdPrefix,
		etcdURL:        etcdURL,
	}
	return cfg
}

// WithMetricStores creates a slice of MetricStore instances.
func WithMetricStores(stores ...definition.MetricStore) []definition.MetricStore {
	return stores
}

// WithScenario creates a Scenario instance.
func WithScenario(
	name string, iteration int, generator generatorFn,
	registry runtime.Registry, opts ...scheduler.Option,
) definition.Scenario {
	return definition.NewScenario(name, iteration, generator, registry, opts...)
}

type generatorFn func(chan<- definition.Event)
