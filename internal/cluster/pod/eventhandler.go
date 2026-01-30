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

func handleVirtualCreatedAt(
	ctx context.Context,
	pod *corev1.Pod,
	c consts.Consts,
	metricManager metric.Manager,
) {
	now := time.Now()
	elapsedTime := now.Sub(clock.UpdatedAt())
	virtualCreatedAt := clock.VirtualTime().Add(elapsedTime)
	slog.DebugContext(
		ctx, "pod has been created",
		"pod", util.ResourceKeyOf(pod),
		"virtualCreatedAt", virtualCreatedAt,
	)

	ownerRef := metav1.GetControllerOf(pod)
	ownerName := ""
	ownerKind := metricpoint.NoOwnerKind
	if ownerRef != nil {
		ownerName = ownerRef.Name
		ownerKind = metricpoint.KindTypeFromString(ownerRef.Kind)
	}
	point, err := metricpoint.NewVirtualTimeMetricPoint(
		metricpoint.VirtualCreatedAt,
		metricpoint.PodKind,
		pod.Namespace,
		pod.Name,
		ownerKind,
		ownerName,
		c.UserID(),
		c.EvaluationID(),
		metricManager.ScenarioID(),
		metricManager.Iteration(),
		virtualCreatedAt,
		now,
	)
	if err != nil {
		slog.ErrorContext(
			ctx, "pod.handleVirtualCreatedAt: failed to create metric point",
			"pod", util.ResourceKeyOf(pod),
			"ownerKind", ownerKind,
			"ownerName", ownerName,
			"error", err,
		)
		return
	}
	slog.DebugContext(
		ctx, "pushing VirtualCreatedAt metric",
		"pod", util.ResourceKeyOf(pod),
		"virtualCreatedAt", virtualCreatedAt,
	)
	metricManager.Record(point)
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
	// Therefore, detect when a Pod is scheduled and calculate virtualScheduledAt
	// based on the elapsed time from current VirtualTime.
	now := time.Now()
	elapsedTime := now.Sub(clock.UpdatedAt())
	virtualScheduledAt := clock.VirtualTime().Add(elapsedTime)
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

	ownerRef := metav1.GetControllerOf(newPod)
	ownerName := ""
	ownerKind := metricpoint.NoOwnerKind
	if ownerRef != nil {
		ownerName = ownerRef.Name
		ownerKind = metricpoint.KindTypeFromString(ownerRef.Kind)
	}
	point, err := metricpoint.NewVirtualTimeMetricPoint(
		metricpoint.VirtualScheduledAt,
		metricpoint.PodKind,
		newPod.Namespace,
		newPod.Name,
		ownerKind,
		ownerName,
		c.UserID(),
		c.EvaluationID(),
		metricManager.ScenarioID(),
		metricManager.Iteration(),
		virtualScheduledAt,
		now,
	)
	if err != nil {
		slog.ErrorContext(
			ctx, "pod.handleVirtualScheduledAt: failed to create metric point",
			"pod", util.ResourceKeyOf(newPod),
			"ownerKind", ownerKind,
			"ownerName", ownerName,
			"error", err,
		)
		return
	}
	slog.DebugContext(
		ctx, "recorded VirtualScheduledAt metric",
		"pod", util.ResourceKeyOf(newPod),
		"virtualScheduledAt", virtualScheduledAt,
	)
	metricManager.Record(point)
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
	now := time.Now()
	elapsedTime := now.Sub(clock.UpdatedAt())
	virtualDeletedAt := clock.VirtualTime().Add(elapsedTime)
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

	ownerRef := metav1.GetControllerOf(pod)
	ownerName := ""
	ownerKind := metricpoint.NoOwnerKind
	if ownerRef != nil {
		ownerName = ownerRef.Name
		ownerKind = metricpoint.KindTypeFromString(ownerRef.Kind)
	}
	point, err := metricpoint.NewVirtualTimeMetricPoint(
		metricpoint.VirtualDeletedAt,
		metricpoint.PodKind,
		pod.Namespace,
		pod.Name,
		ownerKind,
		ownerName,
		c.UserID(),
		c.EvaluationID(),
		metricManager.ScenarioID(),
		metricManager.Iteration(),
		virtualDeletedAt,
		now,
	)
	if err != nil {
		slog.ErrorContext(
			ctx, "pod.handleVirtualDeletedAt: failed to create metric point",
			"pod", util.ResourceKeyOf(pod),
			"ownerKind", ownerKind,
			"ownerName", ownerName,
			"error", err,
		)
		return
	}
	slog.DebugContext(
		ctx, "recorded VirtualDeletedAt metric",
		"pod", util.ResourceKeyOf(pod),
		"virtualDeletedAt", virtualDeletedAt,
	)
	metricManager.Record(point)
}
