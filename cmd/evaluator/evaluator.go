package evaluator

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/cluster"
	"github.com/pfnet/kube-scheduler-evaluator/internal/consts"
	"github.com/pfnet/kube-scheduler-evaluator/internal/evaluation"
	"github.com/pfnet/kube-scheduler-evaluator/internal/metric"
	"github.com/pfnet/kube-scheduler-evaluator/internal/record"
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
	writerFactory, err := config.resolvedRecordWriterFactory()
	if err != nil {
		return nil, err
	}
	resourceManager, err := cluster.NewManager(
		c, metricManager,
		writerFactory,
		config.resolvedCommitProvider(),
		config.resolvedHardwareInfoProvider(),
	)
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

	// recordOutputDir is the folder the transition record (research.md section 11.5)
	// is written to in internal execution mode (units/U01.md). It is unused in
	// external mode, where this record is out of scope. WithConfig requires either it
	// or a RecordWriterFactory override in internal mode.
	recordOutputDir string
	// recordWriterFactory, commitProvider and hardwareInfoProvider are overridable so
	// that tests can pass Fakes for where the record is written, the evaluator's own
	// commit, and the machine's hardware (units/U01.md). Real implementations are used
	// when a caller does not set them.
	recordWriterFactory  RecordWriterFactory
	commitProvider       CommitProvider
	hardwareInfoProvider HardwareInfoProvider
}

// RecordWriter writes one run's transition record.
type RecordWriter = record.Writer

// RecordWriterFactory opens the RecordWriter for one run.
type RecordWriterFactory = record.WriterFactory

// RecordRunInfo identifies the run a RecordWriterFactory is opening a writer for.
type RecordRunInfo = record.RunInfo

// CommitProvider returns the evaluator's own commit, for the transition record's header.
type CommitProvider = record.CommitProvider

// HardwareInfoProvider returns a description of the machine running the evaluator,
// for the transition record's header.
type HardwareInfoProvider = record.HardwareInfoProvider

// ConfigOption customizes a config returned by WithConfig.
type ConfigOption func(*config)

// WithRecordOutputDir sets the folder the transition record is written to
// (research.md section 11.5, units/U01.md). Required in internal execution mode,
// unless WithRecordWriterFactory is also given.
func WithRecordOutputDir(dir string) ConfigOption {
	return func(c *config) { c.recordOutputDir = dir }
}

// WithRecordWriterFactory overrides how the transition record is written. Tests use
// this to pass a Fake instead of writing to a file (units/U01.md).
func WithRecordWriterFactory(fn RecordWriterFactory) ConfigOption {
	return func(c *config) { c.recordWriterFactory = fn }
}

// WithCommitProvider overrides how the evaluator's own commit is obtained for the
// transition record's header. Tests use this to pass a Fake (units/U01.md).
func WithCommitProvider(fn CommitProvider) ConfigOption {
	return func(c *config) { c.commitProvider = fn }
}

// WithHardwareInfoProvider overrides how the machine's hardware information is
// obtained for the transition record's header. Tests use this to pass a Fake
// (units/U01.md).
func WithHardwareInfoProvider(fn HardwareInfoProvider) ConfigOption {
	return func(c *config) { c.hardwareInfoProvider = fn }
}

// WithConfig creates a config instance for the evaluator. In internal execution mode,
// opts must include WithRecordOutputDir or WithRecordWriterFactory: without one of
// them, New would only fail once the first run tries to write the transition record,
// with a confusing "no such file or directory" (units/U01.md requires this record).
func WithConfig(
	userID, kubeconfigPath, etcdPrefix, etcdURL string,
	externalMode bool,
	slogLevel slog.Level,
	opts ...ConfigOption,
) config {
	cfg := config{
		externalMode:   externalMode,
		userID:         userID,
		slogLevel:      slogLevel,
		kubeconfigPath: kubeconfigPath,
		etcdPrefix:     etcdPrefix,
		etcdURL:        etcdURL,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func (c config) resolvedRecordWriterFactory() (record.WriterFactory, error) {
	if c.recordWriterFactory != nil {
		return c.recordWriterFactory, nil
	}
	if !c.externalMode && c.recordOutputDir == "" {
		return nil, fmt.Errorf("evaluator.WithConfig: internal execution mode requires WithRecordOutputDir or WithRecordWriterFactory")
	}
	return record.NewFileWriterFactory(c.recordOutputDir), nil
}

func (c config) resolvedCommitProvider() record.CommitProvider {
	if c.commitProvider != nil {
		return c.commitProvider
	}
	return record.EvaluatorCommit
}

func (c config) resolvedHardwareInfoProvider() record.HardwareInfoProvider {
	if c.hardwareInfoProvider != nil {
		return c.hardwareInfoProvider
	}
	return record.HardwareInfo
}

// WithMetricStores creates a slice of MetricStore instances.
func WithMetricStores(stores ...definition.MetricStore) []definition.MetricStore {
	return stores
}

// WithScenario creates a Scenario instance. identity identifies this scenario's runs
// in the transition record (research.md section 11.5); the evaluator cannot derive it
// itself (units/U01.md).
func WithScenario(
	name string, iteration int, generator generatorFn,
	registry runtime.Registry, identity definition.RunIdentity, opts ...scheduler.Option,
) definition.Scenario {
	return definition.NewScenario(name, iteration, generator, registry, identity, opts...)
}

type generatorFn func(chan<- definition.Event)
