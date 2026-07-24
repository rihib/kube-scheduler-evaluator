package cluster

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestPodNeedsSchedulingResult(t *testing.T) {
	tests := []struct {
		name string
		pod  corev1.Pod
		want bool
	}{
		{
			name: "new pending Pod",
			pod:  corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending}},
			want: true,
		},
		{
			name: "already unschedulable Pod",
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{{
					Type:   corev1.PodScheduled,
					Status: corev1.ConditionFalse,
					Reason: corev1.PodReasonUnschedulable,
				}},
			}},
			want: false,
		},
		{
			name: "pending Pod with another condition",
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{{
					Type:   corev1.PodReady,
					Status: corev1.ConditionFalse,
				}},
			}},
			want: true,
		},
		{
			name: "running Pod",
			pod:  corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := podNeedsSchedulingResult(&tt.pod); got != tt.want {
				t.Fatalf("podNeedsSchedulingResult() = %v, want %v", got, tt.want)
			}
		})
	}
}
