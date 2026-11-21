package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/consts"
	"github.com/pfnet/kube-scheduler-evaluator/internal/event"
	metricpoint "github.com/pfnet/kube-scheduler-evaluator/internal/metric/point"
	"github.com/pfnet/kube-scheduler-evaluator/internal/util"
	"github.com/pfnet/kube-scheduler-evaluator/pkg/definition"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func (m *manager) ProcessEvent(ctx context.Context, e event.Event) error {
	ctx = util.WithLogAttrs(ctx, "eventID", e.EventID())
	obj := e.UnstructuredObject()
	slog.DebugContext(
		ctx, "processing event",
		"eventType", e.EventType(),
		"object", util.ResourceKeyOf(obj),
		"virtualTimestamp", e.VirtualTimestamp(),
	)
	ctx, cancel := context.WithTimeout(ctx, consts.DefaultTimeout)
	defer cancel()

	var err error
	switch e.EventType() {
	case definition.EventTypeCreate:
		var created *unstructured.Unstructured
		opt := metav1.CreateOptions{
			FieldManager: fmt.Sprintf("%s/%s", m.consts.UserID(), fieldManagerSuffix),
		}
		if err := util.WithRetry(
			ctx, "cluster.ProcessEvent.create",
			3, 15*time.Second,
			func() error {
				created, err = m.create(ctx, obj, opt)
				return err
			},
		); err != nil {
			return fmt.Errorf("cluster.ProcessEvent: failed to create object %s after retries: %w", util.ResourceKeyOf(obj).String(), err)
		}
		slog.InfoContext(
			ctx, "created object",
			"eventType", e.EventType(),
			"object", util.ResourceKeyOf(created),
			"virtualTimestamp", e.VirtualTimestamp(),
		)
		if created.GetKind() == "Node" {
			break
		}
		now := time.Now()
		if err := handleVirtualTimeMetric(
			ctx,
			created,
			metricpoint.VirtualCreatedAt,
			e.VirtualTimestamp(),
			now,
			m,
		); err != nil {
			return err
		}
		annotationKey := definition.DeadlineDurationAnnotationKey(m.consts.UserID())
		annotations := created.GetAnnotations()
		if annotations == nil {
			return fmt.Errorf("cluster.ProcessEvent: created object %q has no annotations", util.ResourceKeyOf(created).String())
		}
		val, ok := annotations[annotationKey]
		if !ok {
			return fmt.Errorf("cluster.ProcessEvent: created object %q has no annotation %q", util.ResourceKeyOf(created).String(), annotationKey)
		}
		deadlineDuration, err := time.ParseDuration(val)
		if err != nil {
			return fmt.Errorf("cluster.ProcessEvent: failed to parse annotation %q of object %q: %w", annotationKey, util.ResourceKeyOf(created).String(), err)
		}
		deadline := e.VirtualTimestamp().Add(deadlineDuration)
		if err := handleVirtualTimeMetric(
			ctx,
			created,
			metricpoint.VirtualDeadline,
			deadline,
			now,
			m,
		); err != nil {
			return err
		}
	case definition.EventTypeDelete:
		ownerRef := metav1.GetControllerOf(obj)
		// If the object to be deleted is not a Pod or does not have an owner,
		// delete it directly.
		if obj.GetKind() != "Pod" || ownerRef == nil {
			opt := metav1.DeleteOptions{}
			if err := util.WithRetry(
				ctx, "cluster.ProcessEvent.delete1",
				3, 15*time.Second,
				func() error {
					err = m.delete(ctx, obj, obj.GetName(), opt)
					if errors.IsNotFound(err) {
						slog.WarnContext(
							ctx, "object not found, might be already deleted",
							"object", util.ResourceKeyOf(obj),
						)
						return nil
					}
					return err
				},
			); err != nil {
				return fmt.Errorf(
					"cluster.ProcessEvent: failed to delete object %s after retries: %w",
					util.ResourceKeyOf(obj).String(), err,
				)
			}
			slog.InfoContext(
				ctx, "deleted object which is not a pod or has no owner",
				"eventType", e.EventType(),
				"object", util.ResourceKeyOf(obj),
				"virtualTimestamp", e.VirtualTimestamp(),
			)
			break
		}
		// If the object to be deleted is a Pod and has an owner, delete the owner instead.
		// The garbage collector will then delete the Pod.
		// Assuming the owner has only one Pod.
		if m.podRecorder != nil && e.IsCompletion() {
			m.podRecorder.MarkCompletion(util.ResourceKeyOf(obj))
		}
		ownerGVK := schema.FromAPIVersionAndKind(ownerRef.APIVersion, ownerRef.Kind)
		ownerObj := &unstructured.Unstructured{}
		ownerObj.SetGroupVersionKind(ownerGVK)
		ownerObj.SetNamespace(obj.GetNamespace())
		ownerObj.SetName(ownerRef.Name)
		slog.DebugContext(
			ctx, "deleting owner object",
			"pod", util.ResourceKeyOf(obj),
			"owner", util.ResourceKeyOf(ownerObj),
		)
		// Delete the owner with a propagation policy to ensure dependents are also deleted.
		propagationPolicy := metav1.DeletePropagationBackground
		opt := metav1.DeleteOptions{PropagationPolicy: &propagationPolicy}
		if err := util.WithRetry(
			ctx, "cluster.ProcessEvent.delete2",
			3, 15*time.Second,
			func() error {
				err := m.delete(ctx, ownerObj, ownerRef.Name, opt)
				// When load increases, pods may remain in a Running state even after deletion,
				// potentially causing them to be repeatedly selected for next delete event.
				// To prevent this, pods whose owner cannot be found are cached
				// and treated as already deleted, ensuring they are not selected again.
				//
				// TODO: This implementation risks causing the podsWithDeletedOwner map size
				// to grow excessively, so we should use an LRU cache.
				// When the size exceeds the limit,
				// items that have not been accessedshould be removed.
				if errors.IsNotFound(err) {
					slog.WarnContext(
						ctx, "owner not found, might be already deleted",
						"owner", util.ResourceKeyOf(ownerObj),
					)
					m.podManager.SetPodWithDeletedOwner(util.ResourceKeyOf(obj))
					return nil
				}
				return err
			},
		); err != nil {
			return fmt.Errorf("cluster.ProcessEvent: failed to delete owner object %s after retries: %w", util.ResourceKeyOf(ownerObj).String(), err)
		}
		slog.DebugContext(ctx, "deleting pod", "pod", util.ResourceKeyOf(obj))
		opt = metav1.DeleteOptions{}
		if err := util.WithRetry(
			ctx, "cluster.ProcessEvent.delete3",
			3, 15*time.Second,
			func() error {
				err = m.delete(ctx, obj, obj.GetName(), opt)
				if errors.IsNotFound(err) {
					slog.WarnContext(
						ctx, "pod not found, might be already deleted",
						"object", util.ResourceKeyOf(obj),
					)
					return nil
				}
				return err
			},
		); err != nil {
			return fmt.Errorf(
				"cluster.ProcessEvent: failed to delete object %s after retries: %w",
				util.ResourceKeyOf(obj).String(), err,
			)
		}
		slog.InfoContext(
			ctx, "deleted owner and pod",
			"eventType", e.EventType(),
			"owner", util.ResourceKeyOf(ownerObj),
			"pod", util.ResourceKeyOf(obj),
			"virtualTimestamp", e.VirtualTimestamp(),
		)
		now := time.Now()
		if err := handleVirtualTimeMetric(
			ctx,
			ownerObj,
			metricpoint.VirtualDeletedAt,
			e.VirtualTimestamp(),
			now,
			m,
		); err != nil {
			return err
		}
	case definition.EventTypeFinish:
		return fmt.Errorf("cluster.ProcessEvent: cannot create finish event")
	default:
		return fmt.Errorf("cluster.ProcessEvent: unknown event type")
	}

	return nil
}

func (m *manager) create(
	ctx context.Context,
	obj *unstructured.Unstructured,
	options metav1.CreateOptions,
) (*unstructured.Unstructured, error) {
	switch obj.GetKind() {
	case "Node":
		var nodeObj *corev1.Node
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &nodeObj)
		if err != nil {
			return nil, fmt.Errorf("cluster.create: failed to convert unstructured to Node: %w", err)
		}
		createdNode, err := m.client.CoreV1().Nodes().Create(ctx, nodeObj, options)
		if err != nil {
			return nil, err
		}
		createdNode.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Node"))
		objMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(createdNode)
		if err != nil {
			return nil, err
		}
		return &unstructured.Unstructured{Object: objMap}, nil
	case "Pod":
		var podObj *corev1.Pod
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &podObj)
		if err != nil {
			return nil, fmt.Errorf("cluster.create: failed to convert unstructured to Pod: %w", err)
		}
		createdPod, err := m.client.CoreV1().Pods(podObj.Namespace).Create(ctx, podObj, options)
		if err != nil {
			return nil, err
		}
		createdPod.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
		objMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(createdPod)
		if err != nil {
			return nil, err
		}
		return &unstructured.Unstructured{Object: objMap}, nil
	case "ReplicaSet":
		var rsObj *appsv1.ReplicaSet
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &rsObj)
		if err != nil {
			return nil, fmt.Errorf("cluster.create: failed to convert unstructured to ReplicaSet: %w", err)
		}
		createdRS, err := m.client.AppsV1().ReplicaSets(rsObj.Namespace).Create(ctx, rsObj, options)
		if err != nil {
			return nil, err
		}
		createdRS.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))
		objMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(createdRS)
		if err != nil {
			return nil, err
		}
		return &unstructured.Unstructured{Object: objMap}, nil
	default:
		return nil, fmt.Errorf("cluster.create: unsupported kind %q", obj.GetKind())
	}
}

func (m *manager) delete(ctx context.Context, obj *unstructured.Unstructured, name string, options metav1.DeleteOptions) error {
	switch obj.GetKind() {
	case "Node":
		return m.client.CoreV1().Nodes().Delete(ctx, name, options)
	case "Pod":
		return m.client.CoreV1().Pods(obj.GetNamespace()).Delete(ctx, name, options)
	case "ReplicaSet":
		return m.client.AppsV1().ReplicaSets(obj.GetNamespace()).Delete(ctx, name, options)
	default:
		return fmt.Errorf("cluster.delete: unsupported kind %q", obj.GetKind())
	}
}

func handleVirtualTimeMetric(
	ctx context.Context,
	obj *unstructured.Unstructured,
	virtualTimeName metricpoint.VirtualTimeName,
	virtualTimestamp time.Time,
	now time.Time,
	m *manager,
) error {
	point, err := metricpoint.NewVirtualTimeMetricPoint(
		virtualTimeName,
		metricpoint.KindTypeFromString(obj.GetKind()),
		obj.GetNamespace(),
		obj.GetName(),
		metricpoint.NoOwnerKind,
		"",
		m.consts.UserID(),
		m.consts.EvaluationID(),
		m.metricManager.ScenarioID(),
		m.metricManager.Iteration(),
		virtualTimestamp,
		now,
	)
	if err != nil {
		return err
	}
	m.metricManager.Record(point)
	slog.DebugContext(
		ctx, "recorded virtual time metric point",
		"object", util.ResourceKeyOf(obj),
		"virtualTimeName", virtualTimeName,
		"virtualTimestamp", virtualTimestamp,
	)
	return nil
}
