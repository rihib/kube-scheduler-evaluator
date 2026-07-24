package evaluation

import (
	"context"
	"log/slog"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/clock"
	"github.com/pfnet/kube-scheduler-evaluator/internal/cluster"
	"github.com/pfnet/kube-scheduler-evaluator/internal/consts"
	"github.com/pfnet/kube-scheduler-evaluator/internal/metric"
	metricpoint "github.com/pfnet/kube-scheduler-evaluator/internal/metric/point"
	"github.com/pfnet/kube-scheduler-evaluator/internal/scenario"
	"github.com/pfnet/kube-scheduler-evaluator/internal/util"
	"github.com/pfnet/kube-scheduler-evaluator/pkg/definition"
)

type Manager interface {
	Run(context.Context) error
}

func NewManager(
	c consts.Consts,
	resourceManager cluster.Manager,
	metricManager metric.Manager,
) (Manager, error) {
	return &manager{
		consts:             c,
		newScenarioManager: scenario.NewManager,
		resourceManager:    resourceManager,
		metricManager:      metricManager,
	}, nil
}

type manager struct {
	consts             consts.Consts
	newScenarioManager newScenarioManagerFn
	resourceManager    cluster.Manager
	metricManager      metric.Manager
}

type newScenarioManagerFn func(context.Context, definition.Scenario, consts.Consts, cluster.Manager, metric.Manager) scenario.Manager

func (m *manager) Run(ctx context.Context) error {
	slog.InfoContext(ctx, "start evaluation")

	for _, s := range m.consts.Scenarios() {
		ctx = util.WithLogAttrs(ctx, "scenarioID", s.ScenarioID())
		m.metricManager.SetScenarioID(s.ScenarioID())
		for i := range s.Iteration() {
			ctx = util.WithLogAttrs(ctx, "iteration", i)
			m.metricManager.SetIteration(i)
			clock.SetVirtualTime(m.consts.StartedAt())
			scenarioWallStartedAt := time.Now()
			scenarioVirtualStartedAt := clock.VirtualTime()

			startPoint, err := metricpoint.NewVirtualTimeMetricPoint(
				metricpoint.VirtualCreatedAt,
				metricpoint.ScenarioKind,
				"",
				"", // objName should be "UserID + EvaluationID + ScenarioID + iteration" to uniquely identify the scenario run, but it's redundant so leave it blank.
				metricpoint.NoOwnerKind,
				"",
				m.consts.UserID(),
				m.consts.EvaluationID(),
				m.metricManager.ScenarioID(),
				m.metricManager.Iteration(),
				clock.VirtualTime(),
				time.Now(),
			)
			if err != nil {
				return err
			}
			m.metricManager.Record(startPoint)
			slog.DebugContext(
				ctx, "recorded virtual scenario start time",
				"scenarioID", m.metricManager.ScenarioID(),
				"iteration", m.metricManager.Iteration(),
				"virtualCreatedAt", clock.VirtualTime(),
			)

			// To regenerate the scenario definitions from the first,
			// a new scenarioManager must be created each time.
			scenarioManager := m.newScenarioManager(ctx, s, m.consts, m.resourceManager, m.metricManager)
			if err := scenarioManager.Run(ctx); err != nil {
				return err
			}

			endPoint, err := metricpoint.NewVirtualTimeMetricPoint(
				metricpoint.VirtualDeletedAt,
				metricpoint.ScenarioKind,
				"",
				"",
				metricpoint.NoOwnerKind,
				"",
				m.consts.UserID(),
				m.consts.EvaluationID(),
				m.metricManager.ScenarioID(),
				m.metricManager.Iteration(),
				clock.VirtualTime(), // scenario should end with a deletion event, send clock.VirtualTime() as the virtual end time.
				time.Now(),
			)
			if err != nil {
				return err
			}
			m.metricManager.Record(endPoint)
			summaryPoints, err := metricpoint.NewScenarioSummaryMetricPoints(
				m.consts.UserID(),
				m.consts.EvaluationID(),
				m.metricManager.ScenarioID(),
				m.metricManager.Iteration(),
				time.Since(scenarioWallStartedAt),
				clock.VirtualTime().Sub(scenarioVirtualStartedAt),
				clock.VirtualTime(),
			)
			if err != nil {
				return err
			}
			for _, point := range summaryPoints {
				m.metricManager.Record(point)
			}
			slog.DebugContext(
				ctx, "recorded virtual scenario end time",
				"scenarioID", m.metricManager.ScenarioID(),
				"iteration", m.metricManager.Iteration(),
				"virtualDeletedAt", clock.VirtualTime(),
			)
		}
	}
	return nil
}
