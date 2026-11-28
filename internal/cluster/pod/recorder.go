package pod

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/clock"
	"github.com/pfnet/kube-scheduler-evaluator/internal/consts"
	"github.com/pfnet/kube-scheduler-evaluator/internal/metric"
	metricpoint "github.com/pfnet/kube-scheduler-evaluator/internal/metric/point"
	"github.com/pfnet/kube-scheduler-evaluator/internal/record"
	"github.com/pfnet/kube-scheduler-evaluator/internal/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	resourcehelper "k8s.io/component-helpers/resource"
)

// Recorder records the transitions of Pods in internal mode.
//
// The reactors of the fake API server call it synchronously when they accept a change of a Pod.
// The virtual clock only advances after the scheduler has finished reacting to an event,
// so every transition gets the virtual time of the event that caused it,
// regardless of how long the scheduler or the informers take in real time.
//
// A Recorder is created fresh for each run, together with the fake API server its
// reactors are registered on (cluster.manager.resetFakeCluster), so that state such as
// tasksCreated and completionMarked does not leak from one run into the next.
type Recorder struct {
	consts         consts.Consts
	metricManager  metric.Manager
	scheduledAtMap *scheduledAtMapper
	recordManager  record.Manager

	mu               sync.Mutex
	runCtx           context.Context // carries the log attributes of the current run
	creationOrder    map[string]uint64
	nextOrder        uint64
	tasksCreated     map[string]struct{} // taskID (owner UID) of tasks that already have a recorded instance
	completionMarked map[string]struct{} // pod keys whose next deletion is a "complete", not a "delete"
	bindErr          error               // the first error PodScheduled held computing a bound Pod's allocation, if any
}

func NewRecorder(c consts.Consts, metricManager metric.Manager, recordManager record.Manager) *Recorder {
	return &Recorder{
		consts:           c,
		metricManager:    metricManager,
		scheduledAtMap:   newScheduledAtMapper(),
		recordManager:    recordManager,
		runCtx:           context.Background(),
		creationOrder:    make(map[string]uint64),
		tasksCreated:     make(map[string]struct{}),
		completionMarked: make(map[string]struct{}),
	}
}

// MarkCompletion marks that the next deletion of the Pod identified by key is a
// "complete" (research.md section 11.5), not a "delete". PodDeleted consumes the mark.
func (r *Recorder) MarkCompletion(key *util.ResourceKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completionMarked[key.String()] = struct{}{}
}

func (r *Recorder) consumeCompletionMark(key *util.ResourceKey) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, marked := r.completionMarked[key.String()]
	delete(r.completionMarked, key.String())
	return marked
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
	r.recordTaskCreation(pod)
}

// recordTaskCreation records create_task for a task's first instance, or recreate for
// a later one (research.md section 11.1.1). The task is the Pod's owning ReplicaSet
// (units/U01.md): a Pod without one is not part of this experiment's model and is skipped.
func (r *Recorder) recordTaskCreation(pod *corev1.Pod) {
	if r.recordManager == nil {
		return
	}
	ownerRef := metav1.GetControllerOf(pod)
	if ownerRef == nil {
		return
	}
	taskID := string(ownerRef.UID)
	instanceID := string(pod.UID)

	r.mu.Lock()
	_, seen := r.tasksCreated[taskID]
	r.tasksCreated[taskID] = struct{}{}
	r.mu.Unlock()

	if seen {
		r.recordManager.Recreate(taskID, instanceID)
		return
	}
	r.recordManager.CreateTask(taskID, instanceID, taskImmutableOf(pod, taskID))
}

// taskImmutableOf maps a Pod's spec to the task's immutable state (research.md section
// 10.2). job_id is set to taskID, because this experiment maps one openb row to one job
// containing exactly one task (units/U01.md), so job and task are in a 1:1
// correspondence and no separate job object exists to carry its own identifier. taskID
// is a parameter rather than derived again from pod here, because every caller already
// has it from the owner reference it required to call this function at all.
func taskImmutableOf(pod *corev1.Pod, taskID string) record.TaskImmutable {
	var priority int32
	if pod.Spec.Priority != nil {
		priority = *pod.Spec.Priority
	}
	var constraint map[string]string
	if len(pod.Spec.NodeSelector) > 0 {
		constraint = make(map[string]string, len(pod.Spec.NodeSelector))
		for k, v := range pod.Spec.NodeSelector {
			constraint[k] = v
		}
	}
	return record.TaskImmutable{
		Owner:               "", // Kubernetes has no attribute for this (research.md section 10.2).
		Group:               pod.Namespace,
		JobID:               taskID,
		ResourceRequest:     resourceMapOf(resourcehelper.PodRequests(pod, podResourceOptions)),
		ResourceLimit:       resourceMapOf(resourcehelper.PodLimits(pod, podResourceOptions)),
		PlacementConstraint: constraint,
		Priority:            priority,
	}
}

// podResourceOptions makes resourcehelper compute a Pod's resources the way
// kube-scheduler does for the node it is placed on
// (scheduler/framework.NodeInfo's calculateResource), with the feature gates this
// experiment runs with. SkipPodLevelResources is set explicitly because PodLevelResources
// is disabled. In-place vertical scaling is also disabled, which resourcehelper's default
// already matches: UseStatusResources stays false, so the spec, not the status, is
// authoritative.
var podResourceOptions = resourcehelper.PodResourcesOptions{SkipPodLevelResources: true}

// resourceMapOf renders a resource list as the record's string-keyed map, or nil when
// the Pod asks for nothing.
func resourceMapOf(list corev1.ResourceList) map[string]string {
	if len(list) == 0 {
		return nil
	}
	m := make(map[string]string, len(list))
	for name, qty := range list {
		m[string(name)] = qty.String()
	}
	return m
}

// PodScheduled is called after the fake API server has bound a Pod to a node.
//
// If the Pod's GPU allocation annotations cannot be read, PodScheduled does not return
// the error to its caller (the binding reactor): returning it there would make
// kube-scheduler treat the bind as a retryable failure and keep retrying it instead of
// surfacing anything (round1 Must 3), so the first such error is held and logged
// instead, for Err to report once the run ends — the same "hold the first failure,
// surface it at the end" shape record.Manager uses for a write failure (writeErr,
// internal/record/record.go). The Pod is still recorded as bound with no allocation of
// its own, which is fine because Err failing the run at StopCluster makes the whole
// record untrusted anyway.
func (r *Recorder) PodScheduled(pod *corev1.Pod) {
	allocated, err := allocatedResourceOf(pod)
	ctx := r.logContext()
	if err != nil {
		wrapped := fmt.Errorf("pod.Recorder.PodScheduled: %w", err)
		r.mu.Lock()
		if r.bindErr == nil {
			r.bindErr = wrapped
		}
		r.mu.Unlock()
		slog.ErrorContext(ctx, "pod.Recorder.PodScheduled: failed to compute the Pod's allocated resource", "pod", util.ResourceKeyOf(pod), "error", err)
		allocated = nil
	}
	virtualScheduledAt := clock.VirtualTime()
	if exist := r.scheduledAtMap.set(util.ResourceKeyOf(pod), virtualScheduledAt); exist {
		slog.WarnContext(ctx, "pod already has virtualScheduledAt in scheduledAtMap", "pod", util.ResourceKeyOf(pod))
	}
	recordPodVirtualTime(
		ctx, metricpoint.VirtualScheduledAt, pod,
		virtualScheduledAt, time.Now(), r.consts, r.metricManager,
	)
	if r.recordManager != nil {
		if ownerRef := metav1.GetControllerOf(pod); ownerRef != nil {
			r.recordManager.Bind(string(ownerRef.UID), string(pod.UID), pod.Spec.NodeName, allocated)
		}
	}
}

// Err returns the first error PodScheduled held while computing a bound Pod's
// allocation, if any. cluster.StopCluster checks it once a run ends, so a run that hit
// an annotation it could not parse fails instead of reporting success with an
// incomplete record.
func (r *Recorder) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bindErr
}

// allocatedResourceOf returns the resources a bound Pod holds on its node
// (research.md section 10.5's allocated_resource, keyed by instance_id). It is the
// Pod's request as kube-scheduler computes it, so that the record says what the
// scheduler actually reserved on the node: the sum over the regular containers, or the
// largest init container if that is bigger, plus the Pod's overhead. It also includes
// one entry per GPU (gpuAllocatedResourceOf, units/U03.md) if the Pod carries the GPU
// allocation annotations (decisions.md), regardless of which entry point placed it.
//
// Neither units/U01.md nor decisions.md models an instance's allocation changing after
// it is assigned a node, and this pilot's scenarios never resize a running Pod, so the
// request at bind time is the instance's allocation for the whole run.
func allocatedResourceOf(pod *corev1.Pod) (map[string]string, error) {
	allocated := resourceMapOf(resourcehelper.PodRequests(pod, podResourceOptions))
	gpuAllocated, err := gpuAllocatedResourceOf(pod.GetAnnotations())
	if err != nil {
		return nil, fmt.Errorf("pod.allocatedResourceOf: %w", err)
	}
	if len(gpuAllocated) == 0 {
		return allocated, nil
	}
	if allocated == nil {
		allocated = make(map[string]string, len(gpuAllocated))
	}
	for k, v := range gpuAllocated {
		allocated[k] = v
	}
	return allocated, nil
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
	if r.recordManager != nil {
		if ownerRef := metav1.GetControllerOf(pod); ownerRef != nil {
			taskID, instanceID := string(ownerRef.UID), string(pod.UID)
			if r.consumeCompletionMark(key) {
				r.recordManager.Complete(taskID, instanceID)
			} else {
				r.recordManager.Delete(taskID, instanceID)
			}
		}
	}
}

// creationOrderOf returns the position of the Pod among the Pods created through the fake API server.
func (r *Recorder) creationOrderOf(pod *corev1.Pod) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	order, ok := r.creationOrder[util.ResourceKeyOf(pod).String()]
	return order, ok
}
