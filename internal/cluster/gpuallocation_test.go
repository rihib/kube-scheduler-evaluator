package cluster

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCalculateGPUAllocations(t *testing.T) {
	nodes := []corev1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "small"},
			Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
				gpuResourceName: resource.MustParse("1"),
			}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "large"},
			Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
				gpuResourceName: resource.MustParse("8"),
			}},
		},
	}
	pods := []corev1.Pod{
		{
			Spec: corev1.PodSpec{
				NodeName: "large",
				Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					gpuResourceName: resource.MustParse("4"),
				}}}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				gpuResourceName: resource.MustParse("4"),
			}}}}},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		},
	}

	got := calculateGPUAllocation(nodes, pods)
	want := GPUAllocation{Allocatable: 9, Allocated: 4}
	if got != want {
		t.Errorf("allocation = %+v, want %+v", got, want)
	}
}
