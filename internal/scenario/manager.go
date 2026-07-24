package scenario

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/clock"
	"github.com/pfnet/kube-scheduler-evaluator/internal/cluster"
	"github.com/pfnet/kube-scheduler-evaluator/internal/consts"
	"github.com/pfnet/kube-scheduler-evaluator/internal/event"
	"github.com/pfnet/kube-scheduler-evaluator/internal/metric"
	metricpoint "github.com/pfnet/kube-scheduler-evaluator/internal/metric/point"
	"github.com/pfnet/kube-scheduler-evaluator/internal/util"
	"github.com/pfnet/kube-scheduler-evaluator/pkg/definition"
	corev1 "k8s.io/api/core/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

type Manager interface {
	Run(context.Context) error
}

func NewManager(
	ctx context.Context,
	s definition.Scenario,
	c consts.Consts,
	resourceManager cluster.Manager,
	metricManager metric.Manager,
) Manager {
	return &manager{
		consts:              c,
		metricManager:       metricManager,
		nextEventDefinition: s.NextEvent,
		resourceManager:     resourceManager,
		schedulerRegistry:   s.SchedulerRegistry(),
		schedulerOptions:    s.SchedulerOptions(),
	}
}

type manager struct {
	consts              consts.Consts
	metricManager       metric.Manager
	nextEventDefinition nextEventDefinitionFn
	resourceManager     cluster.Manager
	nextEventID         int
	nextDefinitionID    int
	userDefinedEvent    event.Event
	nextRelativeTime    time.Duration
	schedulerRegistry   runtime.Registry
	schedulerOptions    []scheduler.Option
	nodeCount           int
	cpuAllocatableMilli int64
	gpuAllocatable      int64
	eventsSinceSnapshot int
}

const clusterSnapshotEveryEvents = 25

type nextEventDefinitionFn func(context.Context) <-chan definition.Event

func (m *manager) Run(ctx context.Context) error {
	slog.InfoContext(ctx, "start new scenario")

	if !m.consts.IsExternalMode() {
		if err := cluster.InitCluster(ctx, m.resourceManager, m.schedulerRegistry, m.schedulerOptions); err != nil {
			return err
		}
	} else {
		if err := cluster.InitClusterExternalMode(ctx, m.resourceManager); err != nil {
			return err
		}
	}
	if err := m.initializeClusterCapacity(ctx); err != nil {
		return err
	}

	ch := m.nextEventDefinition(ctx)
	var i int
	initialBackoff := 15 * time.Second
	if !m.consts.IsExternalMode() {
		initialBackoff = 100 * time.Millisecond
	}
	retryCount, maxRetry, backoff := 0, 3, initialBackoff
	for {
		slog.DebugContext(ctx, fmt.Sprintf("next scenario loop %d", i))
		i++
		// When a Pod is deleted, another Pod may be scheduled in the freed space,
		// and the deletion time of that Pod may be earlier than the userDefinedEvent.
		// Therefore, it is necessary to retrieve podsToBeDeleted every time a Pod is deleted.
		if m.userDefinedEvent != nil {
			deleteEvent, err := m.nextDeleteEventsBefore(ctx, m.userDefinedEvent)
			if err != nil {
				return err
			}
			if deleteEvent != nil {
				slog.DebugContext(ctx, "there is a delete event before user defined event")
				retryCount, maxRetry, backoff = 0, 3, initialBackoff // reset when making progress
				if err := m.processEvent(ctx, deleteEvent); err != nil {
					return err
				}
				continue
			}
			if m.userDefinedEvent.EventType() == definition.EventTypeFinish {
				slog.InfoContext(ctx, "finish event reached, finishing scenario")
				isRemaining, err := m.resourceManager.IsAnyPodsRemaining()
				if err != nil {
					return err
				}
				// Retry in case it takes time for a Pending pod to transition to the Running state.
				if isRemaining {
					if retryCount < maxRetry {
						slog.WarnContext(
							ctx, "there are remaining pods after finish event, retrying",
							"retryCount", retryCount,
						)
						time.Sleep(backoff)
						retryCount++
						backoff *= 2
						continue
					}
					return fmt.Errorf("scenario.Run: there are remaining pods after finish event, this is a bugs")
				}
				if m.resourceManager.IsAnyScheduledAtRemaining(ctx) {
					return fmt.Errorf("scenario.Run: there are remaining scheduledAt after finish event, this is a bugs")
				}
				if count := m.resourceManager.LengthOfPodsWithDeletedOwner(); count == 0 {
					slog.InfoContext(ctx, "no pods with deleted owner remaining")
				} else {
					slog.WarnContext(ctx, "podsWithDeletedOwner remaining", "count", count)
				}
				return nil
			}
			slog.DebugContext(ctx, "processing user defined event")
			if err := m.processEvent(ctx, m.userDefinedEvent); err != nil {
				return err
			}
			m.userDefinedEvent = nil
			continue
		}
		// TODO: With the current implementation, when m.userDefinedEvent != nil, even if ctx.Done() is triggered,
		// the process does not terminate immediately. Fix this.
		select {
		case <-ctx.Done():
			slog.DebugContext(ctx, "context cancelled while running scenario")
			return ctx.Err()
		case d, ok := <-ch:
			if !ok {
				return fmt.Errorf("no more event definitions without a finish event, this is a bug")
			}
			// definitionID in ctx is retained from when the definition is retrieved until it is applied.
			// During this period, a delete event unrelated to the definition may occur by NextDeleteEventsBefore.
			ctx = util.WithLogAttrs(ctx, "definitionID", m.allocateDefinitionID())
			e, err := m.newEventFromDefinition(d)
			if err != nil {
				return err
			}
			m.userDefinedEvent = e
		}
	}
}

func (m *manager) nextDeleteEventsBefore(ctx context.Context, e event.Event) (event.Event, error) {
	candidates, err := m.resourceManager.DeletionCandidatesOf(ctx, e.VirtualTimestamp())
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	deletePod := candidates[0]
	deletionTime := deletePod.DeletionTime()
	if deletionTime.Before(clock.VirtualTime()) {
		// Under high load, when fetching deletionCandidates, scheduledAt may not yet be registered and may be unobtainable.
		// As a countermeasure, retries are performed. However, if retrieval still fails, the pod is skipped to continue execution of the evaluation.
		// However, if scheduledAt is subsequently registered, the next retrieval may return a Pod with a deletionTime earlier than the current virtualTime.
		// To prevent errors in subsequent validations, the deletionTime is adjusted to match the current virtualTime.
		// However, since this deviates from the user-defined scenario, it is logged as an error.
		slog.ErrorContext(
			ctx, "scenario.nextDeleteEventsBefore: deletionTime is before current virtual time, this is not expected scenario. sorry!!",
			"oldDeletionTime", deletionTime,
			"newDeletionTime", clock.VirtualTime(),
		)
		deletionTime = clock.VirtualTime()
	}
	deleteEvent, err := event.NewEvent(
		definition.EventTypeDelete,
		deletePod.Pod(),
		deletionTime,
	)
	if err != nil {
		return nil, err
	}
	return deleteEvent, nil
}

func (m *manager) processEvent(ctx context.Context, e event.Event) error {
	if e.VirtualTimestamp().Before(clock.VirtualTime()) {
		return fmt.Errorf("scenario.processEvent: event's virtual time is before current virtual time, this is a bug. event: %v, current: %v", e.VirtualTimestamp(), clock.VirtualTime())
	}
	clock.SetVirtualTime(e.VirtualTimestamp())
	slog.DebugContext(
		ctx, "virtual clock updated",
		"virtualTime", clock.VirtualTime(),
		"updatedAt", clock.UpdatedAt(),
	)
	e.SetEventID(m.allocateEventID())
	if err := m.resourceManager.ProcessEvent(ctx, e); err != nil {
		return err
	}
	if err := m.resourceManager.WaitForScheduler(ctx); err != nil {
		return err
	}
	if err := m.updateClusterCapacity(e); err != nil {
		return err
	}

	runningPods, err := m.resourceManager.ListPodsByPhase(corev1.PodRunning)
	if err != nil {
		return err
	}
	pendingPods, err := m.resourceManager.ListPodsByPhase(corev1.PodPending)
	if err != nil {
		return err
	}
	m.eventsSinceSnapshot++
	isFinalPodDeletion := e.EventType() == definition.EventTypeDelete &&
		e.UnstructuredObject().GetKind() == "Pod" &&
		len(runningPods) == 0 && len(pendingPods) == 0
	if m.eventsSinceSnapshot < clusterSnapshotEveryEvents && !isFinalPodDeletion {
		return nil
	}
	m.eventsSinceSnapshot = 0
	cpuRequestedMilli, gpuRequested, gpuRequestedMilli := requestedResources(
		runningPods,
		m.consts.UserID(),
	)
	points, err := metricpoint.NewClusterSnapshotMetricPoints(
		m.consts.UserID(), m.consts.EvaluationID(),
		m.metricManager.ScenarioID(), m.metricManager.Iteration(),
		m.nodeCount, len(runningPods), len(pendingPods),
		m.cpuAllocatableMilli, cpuRequestedMilli,
		m.gpuAllocatable, gpuRequested, gpuRequestedMilli,
		clock.VirtualTime(),
	)
	if err != nil {
		return err
	}
	for _, point := range points {
		m.metricManager.Record(point)
	}
	slog.InfoContext(
		ctx, "found running pods and pending pods",
		"running pods count", len(runningPods),
		"pending pods count", len(pendingPods),
	)

	return nil
}

func (m *manager) initializeClusterCapacity(ctx context.Context) error {
	nodes, err := m.resourceManager.ListNodes(ctx)
	if err != nil {
		return err
	}
	m.nodeCount = len(nodes)
	for _, node := range nodes {
		cpu, gpu := nodeCapacity(node)
		m.cpuAllocatableMilli += cpu
		m.gpuAllocatable += gpu
	}
	return nil
}

func (m *manager) updateClusterCapacity(e event.Event) error {
	obj := e.UnstructuredObject()
	if obj.GetKind() != "Node" {
		return nil
	}
	var node corev1.Node
	if err := apiruntime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &node); err != nil {
		return fmt.Errorf("scenario.updateClusterCapacity: %w", err)
	}
	cpu, gpu := nodeCapacity(&node)
	switch e.EventType() {
	case definition.EventTypeCreate:
		m.nodeCount++
		m.cpuAllocatableMilli += cpu
		m.gpuAllocatable += gpu
	case definition.EventTypeDelete:
		m.nodeCount--
		m.cpuAllocatableMilli -= cpu
		m.gpuAllocatable -= gpu
	}
	return nil
}

func nodeCapacity(node *corev1.Node) (cpuMilli, gpu int64) {
	cpu := node.Status.Allocatable.Cpu()
	if cpu != nil {
		cpuMilli = cpu.MilliValue()
	}
	gpuQuantity, ok := node.Status.Allocatable[corev1.ResourceName("nvidia.com/gpu")]
	if ok {
		gpu = gpuQuantity.Value()
	}
	return cpuMilli, gpu
}

func requestedResources(pods []*corev1.Pod, userID string) (cpuMilli, gpu, gpuMilli int64) {
	gpuMilliAnnotationKey := fmt.Sprintf("%s/gpu-milli", userID)
	for _, pod := range pods {
		var podCPU, podGPU int64
		for _, container := range pod.Spec.Containers {
			podCPU += container.Resources.Requests.Cpu().MilliValue()
			gpuQuantity, ok := container.Resources.Requests[corev1.ResourceName("nvidia.com/gpu")]
			if ok {
				podGPU += gpuQuantity.Value()
			}
		}
		cpuMilli += podCPU
		gpu += podGPU
		podGPUMilli := podGPU * 1000
		if raw, ok := pod.Annotations[gpuMilliAnnotationKey]; ok && podGPU > 0 {
			if requestedMilli, err := strconv.ParseInt(raw, 10, 64); err == nil {
				podGPUMilli = podGPU * requestedMilli
			}
		}
		gpuMilli += podGPUMilli
	}
	return cpuMilli, gpu, gpuMilli
}

func (m *manager) allocateDefinitionID() int {
	id := m.nextDefinitionID
	m.nextDefinitionID++
	return id
}

func (m *manager) allocateEventID() int {
	id := m.nextEventID
	m.nextEventID++
	return id
}

func (m *manager) newEventFromDefinition(d definition.Event) (event.Event, error) {
	if d.EventType() == definition.EventTypeFinish {
		m.nextRelativeTime = time.Duration(math.MaxInt)
	} else {
		if m.nextRelativeTime >= time.Duration(math.MaxInt)-d.Interval() {
			return nil, fmt.Errorf("scenario.newEventFromDefinition: relativeTime would overflow")
		}
		m.nextRelativeTime += d.Interval()
	}
	return event.NewEvent(
		d.EventType(),
		d.Object(),
		m.consts.StartedAt().Add(m.nextRelativeTime),
	)
}
