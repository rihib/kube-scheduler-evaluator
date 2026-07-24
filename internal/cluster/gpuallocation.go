package cluster

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	resourcehelper "k8s.io/component-helpers/resource"
)

const gpuResourceName = corev1.ResourceName("nvidia.com/gpu")

// GPUAllocation is the scheduler-visible GPU allocation of the cluster.
type GPUAllocation struct {
	Allocatable int64
	Allocated   int64
}

func (m *manager) GPUAllocation(ctx context.Context) (GPUAllocation, error) {
	nodeList, err := m.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return GPUAllocation{}, fmt.Errorf("cluster.GPUAllocation: failed to list nodes: %w", err)
	}
	podList, err := m.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return GPUAllocation{}, fmt.Errorf("cluster.GPUAllocation: failed to list pods: %w", err)
	}
	return calculateGPUAllocation(nodeList.Items, podList.Items), nil
}

func calculateGPUAllocation(nodes []corev1.Node, pods []corev1.Pod) GPUAllocation {
	allocation := GPUAllocation{}
	for i := range nodes {
		allocatableGPU := nodes[i].Status.Allocatable[gpuResourceName]
		allocation.Allocatable += allocatableGPU.Value()
	}
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName == "" || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		requests := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})
		requestedGPU := requests[gpuResourceName]
		allocation.Allocated += requestedGPU.Value()
	}
	return allocation
}
