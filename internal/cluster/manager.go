package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/cluster/etcd"
	"github.com/pfnet/kube-scheduler-evaluator/internal/cluster/pod"
	"github.com/pfnet/kube-scheduler-evaluator/internal/consts"
	"github.com/pfnet/kube-scheduler-evaluator/internal/event"
	"github.com/pfnet/kube-scheduler-evaluator/internal/metric"
	"github.com/pfnet/kube-scheduler-evaluator/internal/record"
	"github.com/pfnet/kube-scheduler-evaluator/internal/util"
	"github.com/pfnet/kube-scheduler-evaluator/pkg/definition"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

const (
	defaultResyncDuration = 30 * time.Second
	fieldManagerSuffix    = "kube-scheduler-evaluator"
)

type Manager interface {
	ProcessEvent(context.Context, event.Event) error
	WaitForScheduler(context.Context) error
	DeletionCandidatesOf(context.Context, time.Time) ([]*pod.DeletionCandidate, error)
	ListPodsByPhase(corev1.PodPhase) ([]*corev1.Pod, error)
	LengthOfPodsWithDeletedOwner() int
	IsAnyPodsRemaining() (bool, error)
	IsAnyScheduledAtRemaining(context.Context) bool
}

func NewManager(
	c consts.Consts,
	metricManager metric.Manager,
	recordWriterFactory record.WriterFactory,
	recordCommitProvider record.CommitProvider,
	recordHardwareInfoProvider record.HardwareInfoProvider,
) (Manager, error) {
	m := &manager{
		consts:        c,
		metricManager: metricManager,
	}
	if c.IsExternalMode() {
		ext, ok := c.(consts.ConstsExternalMode)
		if !ok {
			return nil, fmt.Errorf("cluster.NewManager: consts must implement ConstsExternalMode in external mode")
		}
		config, err := clientcmd.BuildConfigFromFlags("", ext.KubeconfigPath())
		if err != nil {
			return nil, fmt.Errorf("cluster.NewManager: failed to build kubeconfig from %s: %w", ext.KubeconfigPath(), err)
		}
		client, err := kubernetes.NewForConfig(config)
		if err != nil {
			return nil, fmt.Errorf("cluster.NewManager: %w", err)
		}
		m.client = client
		etcdManager, err := etcd.NewManager(ext)
		if err != nil {
			return nil, err
		}
		mExt := &managerExternalMode{
			manager:     m,
			etcdManager: etcdManager,
		}
		return mExt, nil
	}
	m.recordManager = record.NewManager(recordWriterFactory, recordCommitProvider, recordHardwareInfoProvider)
	// The fake API server and the Pod recorder belong to a run, so InitCluster creates
	// them; until then this manager has none (see resetFakeCluster).
	return m, nil
}

// resetFakeCluster gives the run that is starting its own fake API server and Pod recorder.
//
// A run's scenario is regenerated from scratch every iteration (evaluation.Manager),
// so its Node and ReplicaSet creation events run again from the start too. Without this
// reset, a Node from a previous run would stay registered in the fake API server,
// visible to the scheduler, with no register_node record in the new run's transition
// record: the record would then miss part of "すべての mutable state の値" at the
// start of the observation period (units/U01.md), or the run would fail outright on a
// Node whose name the scenario fixes. Creating the Pod recorder with it also clears
// tasksCreated and completionMarked, which otherwise kept growing for the life of the
// evaluator and could carry a stale completion mark into the next run.
func (m *manager) resetFakeCluster() {
	fakeClient := fakeclientset.NewSimpleClientset()
	m.client = fakeClient
	m.podRecorder = pod.NewRecorder(m.consts, m.metricManager, m.recordManager)

	// PrependReactor puts a reactor in front of the chain, so the reactors registered later run earlier.
	prependPodRecordReactor(fakeClient, m.podRecorder)
	prependNodeRecordReactor(fakeClient, m.recordManager)
	prependObjectCreationReactor(fakeClient, "nodes")
	prependObjectCreationReactor(fakeClient, "pods")
	prependObjectCreationReactor(fakeClient, "replicasets")
	prependPodBindingReactor(fakeClient, m.podRecorder)
	prependPodPatchReactor(fakeClient)
	prependDebugReactor(fakeClient)
}

type manager struct {
	// Initialized in NewManager
	consts        consts.Consts
	metricManager metric.Manager
	recordManager record.Manager // internal mode only

	// In external mode, client is initialized in NewManager. In internal mode, client
	// and podRecorder are the fake API server and the recorder of one run, created for
	// each run by resetFakeCluster, so both are nil until the first InitCluster.
	client      kubernetes.Interface
	podRecorder *pod.Recorder // internal mode only

	// Initialized in InitCluster or InitClusterExternalMode for each run, and released in StopCluster.
	// A run is one iteration of a scenario.
	runCancel        context.CancelFunc
	informerFactory  informers.SharedInformerFactory
	podManager       pod.PodManager
	scheduler        *scheduler.Scheduler // internal mode only
	schedulerWg      sync.WaitGroup
	eventBroadcaster events.EventBroadcasterAdapter // internal mode only
	handlerTracker   *handlerTracker                // internal mode only
}

// To prevent managerExternalMode from accessing the InitCluster method, InitCluster is not defined as a method of manager.
func InitCluster(
	ctx context.Context, mng Manager,
	schedulerRegistry runtime.Registry, schedulerOpts []scheduler.Option,
	runIdentity definition.RunIdentity,
) error {
	slog.DebugContext(ctx, "initializing cluster")

	m, ok := mng.(*manager)
	if !ok {
		return fmt.Errorf("cluster.InitCluster: manager is not of type manager")
	}
	if err := m.recordManager.BeginRun(ctx, record.RunInfo{
		EvaluationID: m.consts.EvaluationID(),
		ScenarioID:   m.metricManager.ScenarioID(),
		Iteration:    m.metricManager.Iteration(),
		Identity:     runIdentity,
	}); err != nil {
		return fmt.Errorf("cluster.InitCluster: %w", err)
	}
	// Each run gets its own fake API server and Pod recorder; see resetFakeCluster.
	m.resetFakeCluster()
	runCtx := m.beginRun(ctx)
	if err := initInformers(runCtx, m); err != nil {
		return err
	}
	if err := m.initScheduler(runCtx, schedulerRegistry, schedulerOpts); err != nil {
		return err
	}
	slog.DebugContext(ctx, "cluster initialized successfully")
	return nil
}

// StopCluster stops the informers and the scheduler started for the current run.
// It must be called at the end of each run, so that the informers and the pod manager of the run
// do not keep recording the Pods of the following runs.
//
// It returns the error of closing the run's transition record. A run whose record lost
// lines is not a record of the run that happened, and nothing downstream can tell the
// difference (U04 aggregates it as if it were complete), so the caller must fail the
// run instead of reporting success.
func StopCluster(ctx context.Context, mng Manager) error {
	var m *manager
	switch v := mng.(type) {
	case *manager:
		m = v
	case *managerExternalMode:
		m = v.manager
	default:
		return nil
	}
	m.stopRun()
	if m.recordManager != nil {
		if err := m.recordManager.EndRun(); err != nil {
			return fmt.Errorf("cluster.StopCluster: failed to close the transition record: %w", err)
		}
	}
	slog.DebugContext(ctx, "cluster stopped")
	return nil
}

// beginRun stops the previous run if it is still running, and returns the context of a new run.
func (m *manager) beginRun(ctx context.Context) context.Context {
	m.stopRun()
	runCtx, cancel := context.WithCancel(ctx)
	m.runCancel = cancel
	return runCtx
}

func (m *manager) stopRun() {
	if m.runCancel == nil {
		return
	}
	m.runCancel()
	// scheduler.Run returns after closing the scheduling queue.
	m.schedulerWg.Wait()
	if m.informerFactory != nil {
		// Shutdown blocks until the goroutines of all informers, including their event handlers, return.
		m.informerFactory.Shutdown()
	}
	if m.eventBroadcaster != nil {
		m.eventBroadcaster.Shutdown()
	}
	m.runCancel = nil
	m.informerFactory = nil
	m.podManager = nil
	m.scheduler = nil
	m.eventBroadcaster = nil
	m.handlerTracker = nil
}

// initInformers is defined as a function rather than a method of manager
// because it is used by both InitCluster and InitClusterExternalMode.
// ctx must be the context of the run, so that the informers stop at the end of the run.
func initInformers(ctx context.Context, mng Manager) error {
	var m *manager
	switch v := mng.(type) {
	case *manager:
		m = v
	case *managerExternalMode:
		m = v.manager
	default:
		return fmt.Errorf("cluster.initInformers: manager is not of type manager or managerExternalMode")
	}
	informerFactory := informers.NewSharedInformerFactory(m.client, defaultResyncDuration)
	nodeInformer := informerFactory.Core().V1().Nodes()
	nodeInformer.Informer()

	podManager, err := pod.NewPodManager(ctx, m.consts, informerFactory, m.metricManager, m.podRecorder)
	if err != nil {
		return err
	}
	m.podManager = podManager
	m.informerFactory = informerFactory

	informerFactory.Start(ctx.Done())
	for v, ok := range informerFactory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return fmt.Errorf("cluster.initInformers: failed to sync informer cache for %v", v)
		}
	}
	return nil
}

func (m *manager) initScheduler(ctx context.Context, schedulerRegistry runtime.Registry, schedulerOpts []scheduler.Option) error {
	opts := []scheduler.Option{
		// disable backoff
		scheduler.WithPodInitialBackoffSeconds(0),
		scheduler.WithPodMaxBackoffSeconds(0),
	}
	if schedulerRegistry != nil {
		opts = append([]scheduler.Option{scheduler.WithFrameworkOutOfTreeRegistry(schedulerRegistry)}, opts...)
	}
	if len(schedulerOpts) > 0 {
		opts = append(opts, schedulerOpts...)
	}
	m.eventBroadcaster = events.NewEventBroadcasterAdapterWithContext(ctx, m.client)
	tracker := &handlerTracker{}
	sched, err := scheduler.New(
		ctx,
		m.client,
		newTrackedInformerFactory(m.informerFactory, tracker),
		nil,
		m.recorderFactory(),
		opts...,
	)
	if err != nil {
		return err
	}
	m.scheduler = sched
	m.handlerTracker = tracker
	m.runScheduler(ctx)
	return nil
}

func (m *manager) recorderFactory() profile.RecorderFactory {
	return func(name string) events.EventRecorder {
		return m.eventBroadcaster.NewRecorder(name)
	}
}

func (m *manager) runScheduler(ctx context.Context) {
	sched := m.scheduler
	m.schedulerWg.Add(1)
	go func() {
		defer m.schedulerWg.Done()
		sched.Run(ctx)
	}()
}

const (
	schedulerPollMinInterval = 100 * time.Microsecond
	schedulerPollMaxInterval = 10 * time.Millisecond
)

// WaitForScheduler waits until the scheduler has finished reacting to the events processed so far.
//
// In internal mode, the scheduler is considered to have finished when its state matches the fake API server
// and nothing is left to schedule (see schedulerStateOf). It does not wait for a scheduling result of each Pod,
// because the scheduler does not always send one: kube-scheduler does not patch a Pod that stays unschedulable
// with the same condition.
func (m *manager) WaitForScheduler(ctx context.Context) error {
	slog.DebugContext(ctx, "waiting for the scheduler to stabilize")

	if m.consts.IsExternalMode() {
		ctx, cancel := context.WithTimeout(ctx, consts.DefaultTimeout)
		defer cancel()
		// Changes to nodes and other components ultimately lead to changes in pod state,
		// so only monitor pod state changes.
		return m.podManager.WaitForPodActivityStable(ctx)
	}
	if err := m.syncReplicaSet(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, consts.DefaultTimeout)
	defer cancel()
	interval := schedulerPollMinInterval
	for {
		state, err := m.schedulerStateOf(ctx)
		if err != nil {
			return err
		}
		if state == "" {
			slog.DebugContext(ctx, "the scheduler has stabilized")
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("cluster.WaitForScheduler: the scheduler did not stabilize (%s): %w", state, ctx.Err())
		case <-time.After(interval):
		}
		interval = min(interval*2, schedulerPollMaxInterval)
	}
}

// schedulerStateOf returns an empty string if the scheduler has finished reacting to the events processed so far,
// and otherwise a description of what is left.
//
// The scheduler has finished when all of the following hold at once:
//   - The scheduling queue has no Pod in activeQ or backoffQ, and no Pod is being scheduled.
//   - Every unbound Pod for the scheduler is in the unschedulable queue, and the API server shows it as unschedulable.
//   - The scheduler cache has every bound Pod on its node, no Pod is assumed (waiting for binding),
//     and the cache has no Pod or node that the API server does not have.
//   - No event handler of the scheduler is running, and none has returned while the state above was being read.
//
// The last condition covers a handler that has updated the cache but not yet moved Pods in the queue.
// The state of the API server is read first. The state of the scheduler only moves toward the API server,
// so any change in the API server or the scheduler during the check shows up as a difference.
func (m *manager) schedulerStateOf(ctx context.Context) (string, error) {
	tracker := m.handlerTracker
	sched := m.scheduler
	if running := tracker.running.Load(); running > 0 {
		return fmt.Sprintf("%d event handlers running", running), nil
	}
	finishedBefore := tracker.finished.Load()

	podList, err := m.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("cluster.WaitForScheduler: failed to list pods: %w", err)
	}
	nodeList, err := m.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("cluster.WaitForScheduler: failed to list nodes: %w", err)
	}

	// Scheduling queue
	queuedPods, summary := sched.SchedulingQueue.PendingPods()
	var activeQ, backoffQ, unschedulable int
	if _, err := fmt.Sscanf(summary, "activeQ:%d; backoffQ:%d; unschedulablePods:%d", &activeQ, &backoffQ, &unschedulable); err != nil {
		return "", fmt.Errorf("cluster.WaitForScheduler: failed to parse the summary of pending pods %q: %w", summary, err)
	}
	if activeQ > 0 || backoffQ > 0 {
		return summary, nil
	}
	if inFlight := sched.SchedulingQueue.InFlightPods(); len(inFlight) > 0 {
		return fmt.Sprintf("%d pods being scheduled", len(inFlight)), nil
	}
	queued := make(map[string]struct{}, len(queuedPods))
	for _, p := range queuedPods {
		queued[util.ResourceKeyOf(p).String()] = struct{}{}
	}

	// Pods in the API server
	var boundPods []*corev1.Pod
	waitingPods := 0
	nodeNames := make(map[string]struct{}, len(nodeList.Items))
	for i := range nodeList.Items {
		nodeNames[nodeList.Items[i].Name] = struct{}{}
	}
	for i := range podList.Items {
		p := &podList.Items[i]
		if p.Spec.NodeName != "" {
			boundPods = append(boundPods, p)
			// The scheduler cache keeps a node entry while it has Pods, even after the node is deleted.
			nodeNames[p.Spec.NodeName] = struct{}{}
			continue
		}
		if p.Status.Phase != corev1.PodPending || !sched.Profiles.HandlesSchedulerName(p.Spec.SchedulerName) {
			continue
		}
		waitingPods++
		if _, ok := queued[util.ResourceKeyOf(p).String()]; !ok {
			return fmt.Sprintf("pod %s is not in the scheduling queue", util.ResourceKeyOf(p)), nil
		}
		// The scheduler patches the condition after putting the Pod back to the queue.
		// A Pod blocked by a scheduling gate is never patched.
		cond := podScheduledConditionOf(p)
		if cond == nil || cond.Status != corev1.ConditionFalse {
			if info, ok := sched.SchedulingQueue.GetPod(p.Name, p.Namespace); !ok || !info.Gated {
				return fmt.Sprintf("pod %s is not marked as unschedulable yet", util.ResourceKeyOf(p)), nil
			}
		}
	}
	if len(queued) != waitingPods {
		return fmt.Sprintf("%d pods in the scheduling queue, %d unbound pods in the API server", len(queued), waitingPods), nil
	}

	// Scheduler cache
	for _, p := range boundPods {
		cached, err := sched.Cache.GetPod(p)
		if err != nil {
			return fmt.Sprintf("pod %s is not in the scheduler cache", util.ResourceKeyOf(p)), nil
		}
		if cached.Spec.NodeName != p.Spec.NodeName {
			return fmt.Sprintf("pod %s is on node %q in the scheduler cache", util.ResourceKeyOf(p), cached.Spec.NodeName), nil
		}
		if assumed, err := sched.Cache.IsAssumedPod(p); err != nil || assumed {
			return fmt.Sprintf("pod %s is waiting for binding", util.ResourceKeyOf(p)), nil
		}
	}
	podCount, err := sched.Cache.PodCount()
	if err != nil {
		return "", fmt.Errorf("cluster.WaitForScheduler: failed to count pods in the scheduler cache: %w", err)
	}
	if podCount != len(boundPods) {
		return fmt.Sprintf("%d pods in the scheduler cache, %d bound pods in the API server", podCount, len(boundPods)), nil
	}
	if nodeCount := sched.Cache.NodeCount(); nodeCount != len(nodeNames) {
		return fmt.Sprintf("%d nodes in the scheduler cache, %d nodes in the API server", nodeCount, len(nodeNames)), nil
	}

	if running := tracker.running.Load(); running > 0 {
		return fmt.Sprintf("%d event handlers running", running), nil
	}
	if tracker.finished.Load() != finishedBefore {
		return "event handlers ran during the check", nil
	}
	return "", nil
}

func podScheduledConditionOf(p *corev1.Pod) *corev1.PodCondition {
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == corev1.PodScheduled {
			return &p.Status.Conditions[i]
		}
	}
	return nil
}

func (m *manager) DeletionCandidatesOf(
	ctx context.Context,
	asOfTime time.Time,
) ([]*pod.DeletionCandidate, error) {
	var pods []*pod.DeletionCandidate
	if err := util.WithRetry(ctx, "cluster.DeletionCandidatesOf", 3, 15*time.Second, func() error {
		var err error
		pods, err = m.podManager.DeletionCandidatesOf(ctx, asOfTime)
		return err
	}); err != nil {
		if pods == nil {
			return nil, fmt.Errorf("cluster.DeletionCandidatesOf: failed to list pods to be deleted before %v after retries: %w", asOfTime, err)
		}
		slog.WarnContext(
			ctx, "cluster.DeletionCandidatesOf: some pods might be missing due to errors",
			"error", err,
		)
	}
	return pods, nil
}

func (m *manager) ListPodsByPhase(phase corev1.PodPhase) ([]*corev1.Pod, error) {
	return m.podManager.ListPodsByPhase(phase)
}

func (m *manager) LengthOfPodsWithDeletedOwner() int {
	return m.podManager.LengthOfPodsWithDeletedOwner()
}

func (m *manager) IsAnyPodsRemaining() (bool, error) {
	pendingPods, err := m.podManager.ListPodsByPhase(corev1.PodPending)
	if err != nil {
		return false, err
	}
	for _, pod := range pendingPods {
		if !m.podManager.PodHasDeletedOwner(util.ResourceKeyOf(pod)) {
			return true, nil
		}
	}
	runningPods, err := m.podManager.ListPodsByPhase(corev1.PodRunning)
	if err != nil {
		return false, err
	}
	for _, pod := range runningPods {
		if !m.podManager.PodHasDeletedOwner(util.ResourceKeyOf(pod)) {
			return true, nil
		}
	}
	unknownPods, err := m.podManager.ListPodsByPhase(corev1.PodUnknown)
	if err != nil {
		return false, err
	}
	for _, pod := range unknownPods {
		if !m.podManager.PodHasDeletedOwner(util.ResourceKeyOf(pod)) {
			return true, nil
		}
	}
	return false, nil
}

func (m *manager) IsAnyScheduledAtRemaining(ctx context.Context) bool {
	return m.podManager.IsAnyScheduledAtRemaining(ctx)
}

type managerExternalMode struct {
	*manager
	etcdManager etcd.Manager // Initialized in NewManager
}

func InitClusterExternalMode(ctx context.Context, mng Manager) error {
	slog.DebugContext(ctx, "initializing cluster")

	m, ok := mng.(*managerExternalMode)
	if !ok {
		return fmt.Errorf("cluster.InitClusterExternalMode: manager is not of type managerExternalMode")
	}
	if err := m.initEtcd(ctx); err != nil {
		return err
	}
	runCtx := m.beginRun(ctx)
	if err := initInformers(runCtx, m); err != nil {
		return err
	}
	return nil
}

func (m *managerExternalMode) initEtcd(ctx context.Context) error {
	if m.etcdManager == nil {
		return fmt.Errorf("cluster.InitClusterExternalMode: etcdManager is nil")
	}
	if err := m.etcdManager.Delete(ctx); err != nil {
		return err
	}
	if err := m.etcdManager.SetSeed(ctx); err != nil {
		return err
	}
	return nil
}
