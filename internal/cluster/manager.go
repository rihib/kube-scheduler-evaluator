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
	"github.com/pfnet/kube-scheduler-evaluator/internal/util"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

func NewManager(c consts.Consts, metricManager metric.Manager) (Manager, error) {
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
	fakeClient := fakeclientset.NewSimpleClientset()
	resultCh := make(chan scheduleResult, 100)
	m.client = fakeClient
	m.scheduleResultCh = resultCh

	prependDebugReactor(fakeClient)
	prependObjectCreationReactor(fakeClient, "nodes")
	prependObjectCreationReactor(fakeClient, "pods")
	prependObjectCreationReactor(fakeClient, "replicasets")
	prependPodBindingReactor(fakeClient, m.scheduleResultCh)
	prependPodPatchReactor(fakeClient, m.scheduleResultCh)

	return m, nil
}

type manager struct {
	// Initialized in NewManager
	consts        consts.Consts
	client        kubernetes.Interface
	metricManager metric.Manager

	// Initialized in InitCluster
	informerFactory     informers.SharedInformerFactory
	podManager          pod.PodManager
	scheduler           *scheduler.Scheduler
	schedulerCtx        context.Context
	schedulerCancelFunc context.CancelFunc
	schedulerWg         sync.WaitGroup
	scheduleResultCh    chan scheduleResult
}

type scheduleResult struct {
	podName     string
	isScheduled bool // true: Bound, false: Unschedulable
	reason      string
}

// To prevent managerExternalMode from accessing the InitCluster method, InitCluster is not defined as a method of manager.
func InitCluster(ctx context.Context, mng Manager, schedulerRegistry runtime.Registry, schedulerOpts []scheduler.Option) error {
	slog.DebugContext(ctx, "initializing cluster")

	m, ok := mng.(*manager)
	if !ok {
		return fmt.Errorf("cluster.InitCluster: manager is not of type manager")
	}
	if err := initInformers(ctx, m); err != nil {
		return err
	}
	if err := m.initScheduler(schedulerRegistry, schedulerOpts); err != nil {
		return err
	}
	slog.DebugContext(ctx, "cluster initialized successfully")
	return nil
}

// initInformers is defined as a function rather than a method of manager
// because it is used by both InitCluster and InitClusterExternalMode.
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

	podManager, err := pod.NewPodManager(ctx, m.consts, informerFactory, m.metricManager)
	if err != nil {
		return err
	}
	m.podManager = podManager

	informerFactory.Start(ctx.Done())
	for v, ok := range informerFactory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return fmt.Errorf("cluster.initInformers: failed to sync informer cache for %v", v)
		}
	}
	m.informerFactory = informerFactory
	return nil
}

func (m *manager) initScheduler(schedulerRegistry runtime.Registry, schedulerOpts []scheduler.Option) error {
	if m.schedulerCancelFunc != nil {
		m.schedulerCancelFunc()
		m.schedulerWg.Wait()
	}
	schedulerCtx, schedulerCancel := context.WithCancel(context.Background())
	m.schedulerCtx = schedulerCtx
	m.schedulerCancelFunc = schedulerCancel
	var sched *scheduler.Scheduler
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
	sched, err := scheduler.New(
		schedulerCtx,
		m.client,
		m.informerFactory,
		nil,
		m.recorderFactory(),
		opts...,
	)
	if err != nil {
		return err
	}
	m.scheduler = sched
	m.runScheduler()
	return nil
}

func (m *manager) recorderFactory() profile.RecorderFactory {
	broadcaster := events.NewEventBroadcasterAdapter(m.client)
	return func(name string) events.EventRecorder {
		return broadcaster.NewRecorder(name)
	}
}

func (m *manager) runScheduler() {
	m.schedulerWg.Add(1)
	go func() {
		defer m.schedulerWg.Done()
		m.scheduler.Run(m.schedulerCtx)
	}()
}

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
	// Get the list of pending pods directly to avoid latency of informer cache
	podList, err := m.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		// fakeClient does not support FieldSelector
	})
	if err != nil {
		return fmt.Errorf("cluster.WaitForScheduler: failed to list pods directly: %w", err)
	}
	pendingPods := make(map[string]struct{})
	for _, pod := range podList.Items {
		if pod.Status.Phase != corev1.PodPending {
			continue
		}
		// Skip pods that are already marked as Unschedulable
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodScheduled &&
				cond.Status == corev1.ConditionFalse &&
				cond.Reason == corev1.PodReasonUnschedulable {
				continue
			}
		}
		slog.DebugContext(ctx, "pending pod candidate", "name", pod.Name)
		pendingPods[pod.Name] = struct{}{}
	}
	if len(pendingPods) == 0 {
		return nil
	}
	for {
		slog.DebugContext(ctx, "waiting for pending pods to be scheduled", "count", len(pendingPods))
		select {
		case result := <-m.scheduleResultCh:
			if _, ok := pendingPods[result.podName]; !ok {
				continue
			}
			delete(pendingPods, result.podName)
			slog.DebugContext(ctx, "pod scheduling result", "pod", result.podName, "isScheduled", result.isScheduled, "reason", result.reason)
			for p := range pendingPods {
				_, err := m.client.CoreV1().Pods("").Get(ctx, p, metav1.GetOptions{})
				if err != nil {
					if apierrors.IsNotFound(err) {
						slog.DebugContext(ctx, "pending pod not found", "name", p)
						delete(pendingPods, p)
						continue
					}
					return err
				}
				slog.DebugContext(ctx, "still pending pod", "name", p)
			}
			if len(pendingPods) == 0 {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
	if err := initInformers(ctx, m); err != nil {
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
