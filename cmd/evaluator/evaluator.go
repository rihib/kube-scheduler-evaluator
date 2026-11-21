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
	resourceManager, err := cluster.NewManager(
		c, metricManager,
		config.recordWriterFactory(),
		config.recordCommitProvider(),
		config.recordHardwareInfoProvider(),
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
	// is written to in internal execution mode (decisions.md). It is unused in
	// external mode, where this record is out of scope.
	recordOutputDir string
	// recordWriterFactory, commitProvider and hardwareInfoProvider are overridable so
	// that tests can pass Fakes for where the record is written, the evaluator's own
	// commit, and the machine's hardware (decisions.md). Real implementations are used
	// when a caller does not set them.
	recordWriterFactoryOverride RecordWriterFactory
	commitProvider              CommitProvider
	hardwareInfoProvider        HardwareInfoProvider
}

// RecordWriter writes one run's transition record. See record.Writer.
type RecordWriter interface {
	WriteLine(v any) error
	Close() error
}

// RecordWriterFactory opens the RecordWriter for one run.
type RecordWriterFactory func(info RecordRunInfo) (RecordWriter, error)

// RecordRunInfo identifies the run a RecordWriterFactory is opening a writer for. See record.RunInfo.
type RecordRunInfo = record.RunInfo

// CommitProvider returns the evaluator's own commit, for the transition record's header.
type CommitProvider func() (string, error)

// HardwareInfoProvider returns a description of the machine running the evaluator,
// for the transition record's header.
type HardwareInfoProvider func() (string, error)

// ConfigOption customizes a config returned by WithConfig.
type ConfigOption func(*config)

// WithRecordOutputDir sets the folder the transition record is written to
// (research.md section 11.5, decisions.md). Required in internal execution mode.
func WithRecordOutputDir(dir string) ConfigOption {
	return func(c *config) { c.recordOutputDir = dir }
}

// WithRecordWriterFactory overrides how the transition record is written. Tests use
// this to pass a Fake instead of writing to a file (decisions.md).
func WithRecordWriterFactory(fn RecordWriterFactory) ConfigOption {
	return func(c *config) { c.recordWriterFactoryOverride = fn }
}

// WithCommitProvider overrides how the evaluator's own commit is obtained for the
// transition record's header. Tests use this to pass a Fake (decisions.md).
func WithCommitProvider(fn CommitProvider) ConfigOption {
	return func(c *config) { c.commitProvider = fn }
}

// WithHardwareInfoProvider overrides how the machine's hardware information is
// obtained for the transition record's header. Tests use this to pass a Fake
// (decisions.md).
func WithHardwareInfoProvider(fn HardwareInfoProvider) ConfigOption {
	return func(c *config) { c.hardwareInfoProvider = fn }
}

// WithConfig creates a config instance for the evaluator.
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

func (c config) recordWriterFactory() record.WriterFactory {
	if c.recordWriterFactoryOverride != nil {
		return func(info record.RunInfo) (record.Writer, error) {
			return c.recordWriterFactoryOverride(info)
		}
	}
	return record.NewFileWriterFactory(c.recordOutputDir)
}

func (c config) recordCommitProvider() record.CommitProvider {
	if c.commitProvider != nil {
		return record.CommitProvider(c.commitProvider)
	}
	return record.EvaluatorCommit
}

func (c config) recordHardwareInfoProvider() record.HardwareInfoProvider {
	if c.hardwareInfoProvider != nil {
		return record.HardwareInfoProvider(c.hardwareInfoProvider)
	}
	return record.HardwareInfo
}

// WithMetricStores creates a slice of MetricStore instances.
func WithMetricStores(stores ...definition.MetricStore) []definition.MetricStore {
	return stores
}

// WithScenario creates a Scenario instance. identity identifies this scenario's runs
// in the transition record (research.md section 11.5); the evaluator cannot derive it
// itself (decisions.md).
func WithScenario(
	name string, iteration int, generator generatorFn,
	registry runtime.Registry, identity definition.RunIdentity, opts ...scheduler.Option,
) definition.Scenario {
	return definition.NewScenario(name, iteration, generator, registry, identity, opts...)
}

type generatorFn func(chan<- definition.Event)
