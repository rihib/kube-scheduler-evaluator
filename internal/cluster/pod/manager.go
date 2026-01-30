package pod

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/consts"
	"github.com/pfnet/kube-scheduler-evaluator/internal/metric"
	"github.com/pfnet/kube-scheduler-evaluator/internal/util"
	"github.com/pfnet/kube-scheduler-evaluator/pkg/definition"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

const (
	indexNodeName  = "spec.nodeName"
	indexPhase     = "status.phase"
	stableDuration = time.Second
)

type PodManager interface {
	WaitForPodActivityStable(context.Context) error
	ListPodsByPhase(corev1.PodPhase) ([]*corev1.Pod, error)
	DeletionCandidatesOf(context.Context, time.Time) ([]*DeletionCandidate, error)
	SetPodWithDeletedOwner(*util.ResourceKey)
	PodHasDeletedOwner(*util.ResourceKey) bool
	LengthOfPodsWithDeletedOwner() int
	IsAnyScheduledAtRemaining(ctx context.Context) bool
}

func NewPodManager(
	ctx context.Context,
	c consts.Consts,
	informerFactory informers.SharedInformerFactory,
	metricManager metric.Manager,
) (PodManager, error) {
	podInformer := informerFactory.Core().V1().Pods().Informer()
	podActivityCh := make(chan struct{}, 1)
	scheduledAtMap := newScheduledAtMapper()
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			pod, _ := obj.(*corev1.Pod)
			handleVirtualCreatedAt(ctx, pod, c, metricManager)
			select {
			case podActivityCh <- struct{}{}:
			default:
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldPod, _ := oldObj.(*corev1.Pod)
			newPod, _ := newObj.(*corev1.Pod)
			handleVirtualScheduledAt(ctx, oldPod, newPod, c, metricManager, scheduledAtMap)
			wasDeleted := oldPod.DeletionTimestamp != nil
			isDeleted := newPod.DeletionTimestamp != nil
			if !wasDeleted && isDeleted {
				slog.DebugContext(
					ctx, "pod has been marked for deletion",
					"pod", util.ResourceKeyOf(newPod),
				)
			}
			select {
			case podActivityCh <- struct{}{}:
			default:
			}
		},
		DeleteFunc: func(obj any) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					return
				}
				pod, ok = tombstone.Obj.(*corev1.Pod)
				if !ok {
					return
				}
			}
			if pod == nil {
				return
			}
			handleVirtualDeletedAt(ctx, pod, c, metricManager, scheduledAtMap)
			select {
			case podActivityCh <- struct{}{}:
			default:
			}
		},
	})
	if err := podInformer.AddIndexers(map[string]cache.IndexFunc{
		indexNodeName: func(obj any) ([]string, error) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return nil, fmt.Errorf("pod.newPodManager: expected a Pod, got %T", obj)
			}
			return []string{pod.Spec.NodeName}, nil
		},
		indexPhase: func(obj any) ([]string, error) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return nil, fmt.Errorf("pod.newPodManager: expected a Pod, got %T", obj)
			}
			return []string{string(pod.Status.Phase)}, nil
		},
	}); err != nil {
		return nil, fmt.Errorf("pod.newPodManager: %w", err)
	}
	podIndexer := podInformer.GetIndexer()

	return &podManager{
		consts:               c,
		metricManager:        metricManager,
		podIndexer:           podIndexer,
		podActivityCh:        podActivityCh,
		scheduledAtMap:       scheduledAtMap,
		podsWithDeletedOwner: sets.New[string](),
	}, nil
}

type podManager struct {
	consts               consts.Consts
	metricManager        metric.Manager
	podIndexer           cache.Indexer
	podActivityCh        chan struct{}
	scheduledAtMap       *scheduledAtMapper
	podsWithDeletedOwner sets.Set[string]
}

func (pm *podManager) WaitForPodActivityStable(ctx context.Context) error {
	// If return early when all Pods are Pending,
	// the scheduler will proceed to the next event without waiting for stability
	// after executing a delete operation when all Pods are Running.
	timer := time.NewTimer(stableDuration)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.DebugContext(
				ctx, "context cancelled while waiting for pod scheduled condition to stabilize",
			)
			return ctx.Err()
		case <-timer.C:
			slog.DebugContext(ctx, "no state transition detected for stableDuration")
			return nil
		case <-pm.podActivityCh:
			slog.DebugContext(ctx, "detected PodScheduled condition transition, resetting timer")
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(stableDuration)
		}
	}
}

func (pm *podManager) ListPodsByPhase(phase corev1.PodPhase) ([]*corev1.Pod, error) {
	objs, err := pm.podIndexer.ByIndex(indexPhase, string(phase))
	if err != nil {
		return nil, fmt.Errorf("pod.listPodsByPhase: %w", err)
	}
	pods, err := castToPods(objs)
	if err != nil {
		return nil, err
	}
	return pods, nil
}

func castToPods(objs []any) ([]*corev1.Pod, error) {
	pods := make([]*corev1.Pod, 0, len(objs))
	for _, obj := range objs {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return nil, fmt.Errorf("pod.castToPods: expected *corev1.Pod, got %T", obj)
		}
		pods = append(pods, pod)
	}
	return pods, nil
}

// Caller may be retried this multiple times if this returns an error,
// so this must be idempotent
func (pm *podManager) DeletionCandidatesOf(
	ctx context.Context,
	asOfTime time.Time,
) ([]*DeletionCandidate, error) {
	runningPods, err := pm.ListPodsByPhase(corev1.PodRunning)
	if err != nil {
		return nil, err
	}
	var candidates []*DeletionCandidate
	if len(runningPods) == 0 {
		return candidates, nil
	}
	var errs []error
	for _, pod := range runningPods {
		if pm.PodHasDeletedOwner(util.ResourceKeyOf(pod)) {
			slog.DebugContext(
				ctx, "skipping pod with deleted owner",
				"pod", util.ResourceKeyOf(pod),
			)
			continue
		}
		deletionTime, err := pm.deletionTimeOf(pod)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if deletionTime.After(asOfTime) {
			continue
		}
		candidates = append(candidates, &DeletionCandidate{
			pod:          pod,
			deletionTime: deletionTime,
		})
		slog.DebugContext(
			ctx, "found deletion candidate",
			"pod", util.ResourceKeyOf(pod),
			"deletionTime", deletionTime,
		)
	}
	var errLine error
	for _, e := range errs {
		errLine = fmt.Errorf("%v; %w", errLine, e)
	}
	slog.DebugContext(
		ctx, "collected deletion candidates",
		"count", len(candidates),
		"asOfTime", asOfTime,
	)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].deletionTime.Before(candidates[j].deletionTime)
	})
	return candidates, errLine
}

type DeletionCandidate struct {
	pod          *corev1.Pod
	deletionTime time.Time
}

func (p *DeletionCandidate) Pod() *corev1.Pod {
	return p.pod
}

func (p *DeletionCandidate) DeletionTime() time.Time {
	return p.deletionTime
}

func (pm *podManager) deletionTimeOf(pod *corev1.Pod) (time.Time, error) {
	executionDuration, err := pm.podExecutionDurationOf(pod)
	if err != nil {
		return time.Time{}, err
	}
	scheduledAt, err := pm.scheduledAtOf(pod)
	if err != nil {
		return time.Time{}, err
	}
	deletionTime := scheduledAt.Add(executionDuration)
	return deletionTime, nil
}

func (pm *podManager) podExecutionDurationOf(pod *corev1.Pod) (time.Duration, error) {
	annotations := pod.GetAnnotations()
	if annotations == nil {
		return 0, fmt.Errorf("pod.getPodExecutionDuration: pod %q has no annotations", pod.Name)
	}
	annotationKey := definition.ExecutionDurationAnnotationKey(pm.consts.UserID())
	val, ok := annotations[annotationKey]
	if !ok {
		return 0, fmt.Errorf("pod.getPodExecutionDuration: pod %q has no annotation %q", pod.Name, annotationKey)
	}
	duration, err := time.ParseDuration(val)
	if err != nil {
		return 0, fmt.Errorf("pod.getPodExecutionDuration: pod %q: invalid executionDuration annotation: %v", pod.Name, err)
	}
	return duration, nil
}

func (pm *podManager) scheduledAtOf(pod *corev1.Pod) (time.Time, error) {
	scheduledAt, ok := pm.scheduledAtMap.get(util.ResourceKeyOf(pod))
	// TODO:
	// Currently, scheduledAt is set by event handlers, but there is a lag between when a Pod is actually scheduled and when the event handler is called.
	// In NonExternalMode, in-memory processing happens quickly, so scheduledAt may not be set yet when we try to retrieve it here.
	// Therefore, we currently perform retries with Sleep, but in the future we want to eliminate this retry logic by setting scheduledAt with prependReactor.
	if !ok && !pm.consts.IsExternalMode() {
		backoff := time.Millisecond * 10
		for range 5 {
			time.Sleep(backoff)
			scheduledAt, ok = pm.scheduledAtMap.get(util.ResourceKeyOf(pod))
			if ok {
				break
			}
			backoff *= 2
		}
	}
	if !ok {
		return time.Time{}, fmt.Errorf("pod.getPodScheduledAt: pod %q has no scheduledAt in scheduledAtMap", util.ResourceKeyOf(pod))
	}
	return scheduledAt, nil
}

func (pm *podManager) SetPodWithDeletedOwner(podKey *util.ResourceKey) {
	pm.podsWithDeletedOwner.Insert(podKey.String())
}

func (pm *podManager) PodHasDeletedOwner(podKey *util.ResourceKey) bool {
	if !pm.podsWithDeletedOwner.Has(podKey.String()) {
		return false
	}
	pm.podsWithDeletedOwner.Delete(podKey.String())
	return true
}

func (pm *podManager) LengthOfPodsWithDeletedOwner() int {
	return pm.podsWithDeletedOwner.Len()
}

func (pm *podManager) IsAnyScheduledAtRemaining(ctx context.Context) bool {
	keys := pm.scheduledAtMap.listKeys()
	for _, key := range keys {
		resourceKey, err := util.ParseResourceKey(key)
		if err != nil {
			slog.ErrorContext(
				ctx, "podManager.IsAnyScheduledAtRemaining: failed to parse resource key",
				"key", key,
				"error", err,
			)
			continue
		}
		if !pm.PodHasDeletedOwner(resourceKey) {
			return true
		}
	}
	return false
}

type scheduledAtMapper struct {
	mu               sync.RWMutex
	podToScheduledAt map[string]time.Time
}

func newScheduledAtMapper() *scheduledAtMapper {
	return &scheduledAtMapper{
		podToScheduledAt: make(map[string]time.Time),
	}
}

func (sm *scheduledAtMapper) get(podKey *util.ResourceKey) (time.Time, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	t, ok := sm.podToScheduledAt[podKey.String()]
	return t, ok
}

func (sm *scheduledAtMapper) set(podKey *util.ResourceKey, t time.Time) bool {
	exist := false
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, ok := sm.podToScheduledAt[podKey.String()]; ok {
		exist = true
	}
	sm.podToScheduledAt[podKey.String()] = t
	return exist
}

func (sm *scheduledAtMapper) delete(podKey *util.ResourceKey) (exist bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, ok := sm.podToScheduledAt[podKey.String()]; !ok {
		return false
	}
	delete(sm.podToScheduledAt, podKey.String())
	return true
}

func (sm *scheduledAtMapper) listKeys() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	keys := make([]string, 0, len(sm.podToScheduledAt))
	for k := range sm.podToScheduledAt {
		keys = append(keys, k)
	}
	return keys
}
