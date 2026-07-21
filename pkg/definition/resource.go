package definition

import (
	corev1 "k8s.io/api/core/v1"
)

// GPUResourceName is the extended resource name that kube-scheduler-evaluator
// aggregates to produce the gpu_* metrics (capacity, allocated, pending, utilization).
const GPUResourceName corev1.ResourceName = "nvidia.com/gpu"
