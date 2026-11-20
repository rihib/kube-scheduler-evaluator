package pod

import (
	"context"
	"log/slog"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/clock"
	"github.com/pfnet/kube-scheduler-evaluator/internal/consts"
	"github.com/pfnet/kube-scheduler-evaluator/internal/metric"
	metricpoint "github.com/pfnet/kube-scheduler-evaluator/internal/metric/point"
	"github.com/pfnet/kube-scheduler-evaluator/internal/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The handlers in this file are used in external mode only.
// A real cluster takes real time to react to an event, so the virtual time of a Pod transition
// is the virtual time of the preceding event plus the real time elapsed since that event.
// In internal mode, Recorder records the transitions instead.
func externalVirtualTimeNow() (virtualTime, realTime time.Time) {
	now := time.Now()
	return clock.VirtualTime().Add(now.Sub(clock.UpdatedAt())), now
}

func handleVirtualCreatedAt(
	ctx context.Context,
	pod *corev1.Pod,
	c consts.Consts,
	metricManager metric.Manager,
) {
	virtualCreatedAt, now := externalVirtualTimeNow()
	slog.DebugContext(
		ctx, "pod has been created",
		"pod", util.ResourceKeyOf(pod),
		"virtualCreatedAt", virtualCreatedAt,
	)
	recordPodVirtualTime(ctx, metricpoint.VirtualCreatedAt, pod, virtualCreatedAt, now, c, metricManager)
}

func handleVirtualScheduledAt(
	ctx context.Context,
	oldPod, newPod *corev1.Pod,
	c consts.Consts,
	metricManager metric.Manager,
	scheduledAtMap *scheduledAtMapper,
) {
	oldCond := podConditionByTypeOf(oldPod, corev1.PodScheduled)
	newCond := podConditionByTypeOf(newPod, corev1.PodScheduled)
	wasScheduled := oldCond != nil && oldCond.Status == corev1.ConditionTrue
	isScheduled := newCond != nil && newCond.Status == corev1.ConditionTrue
	if wasScheduled || !isScheduled {
		return
	}
	// The Pod's schedule is considered to be triggered by the preceding event.
	// For example, a Pod might be created and scheduled by a create event,
	// or another Pod might be scheduled onto a node which resources freed by a delete event.
	virtualScheduledAt, now := externalVirtualTimeNow()
	slog.DebugContext(
		ctx, "pod has been scheduled",
		"pod", util.ResourceKeyOf(newPod),
		"node", newPod.Spec.NodeName,
		"virtualScheduledAt", virtualScheduledAt,
	)
	if exist := scheduledAtMap.set(util.ResourceKeyOf(newPod), virtualScheduledAt); exist {
		slog.WarnContext(
			ctx, "pod already has virtualScheduledAt in scheduledAtMap",
			"pod", util.ResourceKeyOf(newPod),
		)
	}
	recordPodVirtualTime(ctx, metricpoint.VirtualScheduledAt, newPod, virtualScheduledAt, now, c, metricManager)
}

// https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-conditions
func podConditionByTypeOf(
	pod *corev1.Pod,
	condType corev1.PodConditionType,
) *corev1.PodCondition {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == condType {
			return &cond
		}
	}
	return nil
}

func handleVirtualDeletedAt(
	ctx context.Context,
	pod *corev1.Pod,
	c consts.Consts,
	metricManager metric.Manager,
	scheduledAtMap *scheduledAtMapper,
) {
	virtualDeletedAt, now := externalVirtualTimeNow()
	slog.DebugContext(
		ctx, "pod has been deleted",
		"pod", util.ResourceKeyOf(pod),
		"virtualDeletedAt", virtualDeletedAt,
	)
	if exist := scheduledAtMap.delete(util.ResourceKeyOf(pod)); !exist {
		slog.WarnContext(
			ctx, "pod.handleVirtualDeletedAt: pod not found in scheduledAtMap",
			"pod", util.ResourceKeyOf(pod),
		)
	}
	recordPodVirtualTime(ctx, metricpoint.VirtualDeletedAt, pod, virtualDeletedAt, now, c, metricManager)
}

// recordPodVirtualTime records a transition of a Pod at the given virtual time.
func recordPodVirtualTime(
	ctx context.Context,
	virtualTimeName metricpoint.VirtualTimeName,
	pod *corev1.Pod,
	virtualTime time.Time,
	now time.Time,
	c consts.Consts,
	metricManager metric.Manager,
) {
	ownerRef := metav1.GetControllerOf(pod)
	ownerName := ""
	ownerKind := metricpoint.NoOwnerKind
	if ownerRef != nil {
		ownerName = ownerRef.Name
		ownerKind = metricpoint.KindTypeFromString(ownerRef.Kind)
	}
	point, err := metricpoint.NewVirtualTimeMetricPoint(
		virtualTimeName,
		metricpoint.PodKind,
		pod.Namespace,
		pod.Name,
		ownerKind,
		ownerName,
		c.UserID(),
		c.EvaluationID(),
		metricManager.ScenarioID(),
		metricManager.Iteration(),
		virtualTime,
		now,
	)
	if err != nil {
		slog.ErrorContext(
			ctx, "pod.recordPodVirtualTime: failed to create metric point",
			"virtualTimeName", virtualTimeName,
			"pod", util.ResourceKeyOf(pod),
			"ownerKind", ownerKind,
			"ownerName", ownerName,
			"error", err,
		)
		return
	}
	slog.DebugContext(
		ctx, "recorded "+metricNameOf(virtualTimeName)+" metric",
		"pod", util.ResourceKeyOf(pod),
		"virtualTime", virtualTime,
	)
	metricManager.Record(point)
}

func metricNameOf(virtualTimeName metricpoint.VirtualTimeName) string {
	switch virtualTimeName {
	case metricpoint.VirtualCreatedAt:
		return "VirtualCreatedAt"
	case metricpoint.VirtualScheduledAt:
		return "VirtualScheduledAt"
	case metricpoint.VirtualDeletedAt:
		return "VirtualDeletedAt"
	default:
		return virtualTimeName.String()
	}
}
