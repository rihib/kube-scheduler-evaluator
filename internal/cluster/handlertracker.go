package cluster

import (
	"sync/atomic"
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/informers/core"
	corev1informers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
)

// handlerTracker counts the executions of the event handlers that the scheduler registers
// on the Pod and Node informers.
//
// A handler of the scheduler applies one event in several steps.
// For example, deletePodFromCache calls Cache.RemovePod and then MoveAllToActiveOrBackoffQueue.
// Between the two calls, the cache already matches the API server while the Pods that the deletion
// made schedulable are still in the unschedulable queue, so comparing the cache and the queue
// with the API server cannot tell that the scheduler is still reacting to the deletion.
// WaitForScheduler uses handlerTracker to exclude such moments.
type handlerTracker struct {
	running  atomic.Int64  // handlers being executed
	finished atomic.Uint64 // handlers that have returned
}

func (t *handlerTracker) begin() {
	t.running.Add(1)
}

// end increments finished before decrementing running,
// so that a handler that returns while WaitForScheduler is observing the scheduler
// is seen either as running or as a change of finished.
func (t *handlerTracker) end() {
	t.finished.Add(1)
	t.running.Add(-1)
}

type trackedHandler struct {
	inner   cache.ResourceEventHandler
	tracker *handlerTracker
}

func (h *trackedHandler) OnAdd(obj any, isInInitialList bool) {
	h.tracker.begin()
	defer h.tracker.end()
	h.inner.OnAdd(obj, isInInitialList)
}

func (h *trackedHandler) OnUpdate(oldObj, newObj any) {
	h.tracker.begin()
	defer h.tracker.end()
	h.inner.OnUpdate(oldObj, newObj)
}

func (h *trackedHandler) OnDelete(obj any) {
	h.tracker.begin()
	defer h.tracker.end()
	h.inner.OnDelete(obj)
}

// trackedInformerFactory is the informer factory passed to the scheduler.
// It wraps the event handlers that the scheduler registers on the Pod and Node informers,
// and passes everything else through to the shared informer factory.
type trackedInformerFactory struct {
	informers.SharedInformerFactory
	tracker *handlerTracker
}

func newTrackedInformerFactory(f informers.SharedInformerFactory, t *handlerTracker) *trackedInformerFactory {
	return &trackedInformerFactory{SharedInformerFactory: f, tracker: t}
}

func (f *trackedInformerFactory) Core() core.Interface {
	return &trackedCore{Interface: f.SharedInformerFactory.Core(), tracker: f.tracker}
}

type trackedCore struct {
	core.Interface
	tracker *handlerTracker
}

func (c *trackedCore) V1() corev1informers.Interface {
	return &trackedCoreV1{Interface: c.Interface.V1(), tracker: c.tracker}
}

type trackedCoreV1 struct {
	corev1informers.Interface
	tracker *handlerTracker
}

func (v *trackedCoreV1) Pods() corev1informers.PodInformer {
	return &trackedPodInformer{PodInformer: v.Interface.Pods(), tracker: v.tracker}
}

func (v *trackedCoreV1) Nodes() corev1informers.NodeInformer {
	return &trackedNodeInformer{NodeInformer: v.Interface.Nodes(), tracker: v.tracker}
}

type trackedPodInformer struct {
	corev1informers.PodInformer
	tracker *handlerTracker
}

func (p *trackedPodInformer) Informer() cache.SharedIndexInformer {
	return &trackedInformer{SharedIndexInformer: p.PodInformer.Informer(), tracker: p.tracker}
}

type trackedNodeInformer struct {
	corev1informers.NodeInformer
	tracker *handlerTracker
}

func (n *trackedNodeInformer) Informer() cache.SharedIndexInformer {
	return &trackedInformer{SharedIndexInformer: n.NodeInformer.Informer(), tracker: n.tracker}
}

type trackedInformer struct {
	cache.SharedIndexInformer
	tracker *handlerTracker
}

func (i *trackedInformer) AddEventHandler(
	handler cache.ResourceEventHandler,
) (cache.ResourceEventHandlerRegistration, error) {
	return i.SharedIndexInformer.AddEventHandler(&trackedHandler{inner: handler, tracker: i.tracker})
}

func (i *trackedInformer) AddEventHandlerWithResyncPeriod(
	handler cache.ResourceEventHandler,
	resyncPeriod time.Duration,
) (cache.ResourceEventHandlerRegistration, error) {
	return i.SharedIndexInformer.AddEventHandlerWithResyncPeriod(
		&trackedHandler{inner: handler, tracker: i.tracker}, resyncPeriod,
	)
}
