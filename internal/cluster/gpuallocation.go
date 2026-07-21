package cluster

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	resourcehelper "k8s.io/component-helpers/resource"
)

const gpuResourceName = corev1.ResourceName("nvidia.com/gpu")

// GPUAllocation is the scheduler-visible GPU allocation of a node. An entry
// with an empty NodeName contains the cluster aggregate.
type GPUAllocation struct {
	NodeName    string
	Allocatable int64
	Allocated   int64
}

func (m *manager) GPUAllocations(ctx context.Context) ([]GPUAllocation, error) {
	nodeList, err := m.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("cluster.GPUAllocations: failed to list nodes: %w", err)
	}
	podList, err := m.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("cluster.GPUAllocations: failed to list pods: %w", err)
	}
	return calculateGPUAllocations(nodeList.Items, podList.Items), nil
}

func calculateGPUAllocations(nodes []corev1.Node, pods []corev1.Pod) []GPUAllocation {
	byNode := make(map[string]int64, len(nodes))
	for i := range pods {
		pod := &pods[i]
		if pod.Spec.NodeName == "" || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		requests := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})
		requestedGPU := requests[gpuResourceName]
		byNode[pod.Spec.NodeName] += requestedGPU.Value()
	}

	allocations := make([]GPUAllocation, 0, len(nodes)+1)
	cluster := GPUAllocation{}
	for i := range nodes {
		node := &nodes[i]
		allocatableGPU := node.Status.Allocatable[gpuResourceName]
		allocation := GPUAllocation{
			NodeName:    node.Name,
			Allocatable: allocatableGPU.Value(),
			Allocated:   byNode[node.Name],
		}
		allocations = append(allocations, allocation)
		cluster.Allocatable += allocation.Allocatable
		cluster.Allocated += allocation.Allocated
	}
	allocations = append(allocations, cluster)
	return allocations
}
