package pod

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/clock"
	"github.com/pfnet/kube-scheduler-evaluator/internal/consts"
	"github.com/pfnet/kube-scheduler-evaluator/internal/metric"
	metricpoint "github.com/pfnet/kube-scheduler-evaluator/internal/metric/point"
	"github.com/pfnet/kube-scheduler-evaluator/internal/util"
	corev1 "k8s.io/api/core/v1"
)

// Recorder records the transitions of Pods in internal mode.
//
// The reactors of the fake API server call it synchronously when they accept a change of a Pod.
// The virtual clock only advances after the scheduler has finished reacting to an event,
// so every transition gets the virtual time of the event that caused it,
// regardless of how long the scheduler or the informers take in real time.
//
// Recorder lives as long as the cluster manager, because the reactors are registered once
// on the fake API server, while a PodManager is created for each run.
type Recorder struct {
	consts         consts.Consts
	metricManager  metric.Manager
	scheduledAtMap *scheduledAtMapper

	mu            sync.Mutex
	runCtx        context.Context // carries the log attributes of the current run
	creationOrder map[string]uint64
	nextOrder     uint64
}

func NewRecorder(c consts.Consts, metricManager metric.Manager) *Recorder {
	return &Recorder{
		consts:         c,
		metricManager:  metricManager,
		scheduledAtMap: newScheduledAtMapper(),
		runCtx:         context.Background(),
		creationOrder:  make(map[string]uint64),
	}
}

// setRunContext sets the context whose log attributes (scenario and iteration) the records carry.
func (r *Recorder) setRunContext(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runCtx = ctx
}

func (r *Recorder) logContext() context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runCtx
}

// PodCreated is called after the fake API server has stored a new Pod.
func (r *Recorder) PodCreated(pod *corev1.Pod) {
	key := util.ResourceKeyOf(pod).String()
	r.mu.Lock()
	r.creationOrder[key] = r.nextOrder
	r.nextOrder++
	r.mu.Unlock()
	recordPodVirtualTime(
		r.logContext(), metricpoint.VirtualCreatedAt, pod,
		clock.VirtualTime(), time.Now(), r.consts, r.metricManager,
	)
}

// PodScheduled is called after the fake API server has bound a Pod to a node.
func (r *Recorder) PodScheduled(pod *corev1.Pod) {
	ctx := r.logContext()
	virtualScheduledAt := clock.VirtualTime()
	if exist := r.scheduledAtMap.set(util.ResourceKeyOf(pod), virtualScheduledAt); exist {
		slog.WarnContext(ctx, "pod already has virtualScheduledAt in scheduledAtMap", "pod", util.ResourceKeyOf(pod))
	}
	recordPodVirtualTime(
		ctx, metricpoint.VirtualScheduledAt, pod,
		virtualScheduledAt, time.Now(), r.consts, r.metricManager,
	)
}

// PodDeleted is called after the fake API server has deleted a Pod.
func (r *Recorder) PodDeleted(pod *corev1.Pod) {
	ctx := r.logContext()
	key := util.ResourceKeyOf(pod)
	if exist := r.scheduledAtMap.delete(key); !exist {
		slog.WarnContext(ctx, "pod.Recorder.PodDeleted: pod not found in scheduledAtMap", "pod", key)
	}
	r.mu.Lock()
	delete(r.creationOrder, key.String())
	r.mu.Unlock()
	recordPodVirtualTime(
		ctx, metricpoint.VirtualDeletedAt, pod,
		clock.VirtualTime(), time.Now(), r.consts, r.metricManager,
	)
}

// creationOrderOf returns the position of the Pod among the Pods created through the fake API server.
func (r *Recorder) creationOrderOf(pod *corev1.Pod) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	order, ok := r.creationOrder[util.ResourceKeyOf(pod).String()]
	return order, ok
}
