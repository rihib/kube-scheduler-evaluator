package point

import (
	"strconv"
	"time"
)

const (
	MetricClusterNodes               = "kube_scheduler_evaluator_cluster_nodes"
	MetricClusterPods                = "kube_scheduler_evaluator_cluster_pods"
	MetricClusterCPUAllocatableCores = "kube_scheduler_evaluator_cluster_cpu_allocatable_cores"
	MetricClusterCPURequestedCores   = "kube_scheduler_evaluator_cluster_cpu_requested_cores"
	MetricClusterCPUUtilizationRatio = "kube_scheduler_evaluator_cluster_cpu_utilization_ratio"
	MetricClusterGPUAllocatable      = "kube_scheduler_evaluator_cluster_gpu_allocatable"
	MetricClusterGPURequested        = "kube_scheduler_evaluator_cluster_gpu_requested"
	MetricClusterGPURequestedMilli   = "kube_scheduler_evaluator_cluster_gpu_requested_milli"
	MetricClusterGPUUtilizationRatio = "kube_scheduler_evaluator_cluster_gpu_utilization_ratio"
)

// NewClusterSnapshotMetricPoints builds the aggregate cluster samples used by
// the live demo dashboard. CPU and GPU utilization are request-based because
// the evaluator simulates scheduling rather than workload execution.
func NewClusterSnapshotMetricPoints(
	userID, evaluationID, scenarioID string,
	iteration int,
	nodes, runningPods, pendingPods int,
	cpuAllocatableMilli, cpuRequestedMilli int64,
	gpuAllocatable, gpuRequested, gpuRequestedMilli int64,
	timestamp time.Time,
) ([]MetricPoint, error) {
	labels := map[string]string{
		"user_id":       userID,
		"evaluation_id": evaluationID,
		"scenario_id":   scenarioID,
		"iteration":     strconv.Itoa(iteration),
	}
	type sample struct {
		name   string
		labels map[string]string
		value  float64
	}
	cpuUtilization := float64(0)
	if cpuAllocatableMilli > 0 {
		cpuUtilization = float64(cpuRequestedMilli) / float64(cpuAllocatableMilli)
	}
	gpuUtilization := float64(0)
	if gpuAllocatable > 0 {
		gpuUtilization = float64(gpuRequestedMilli) / float64(gpuAllocatable*1000)
	}
	samples := []sample{
		{name: MetricClusterNodes, labels: labels, value: float64(nodes)},
		{name: MetricClusterPods, labels: withLabel(labels, "phase", "running"), value: float64(runningPods)},
		{name: MetricClusterPods, labels: withLabel(labels, "phase", "pending"), value: float64(pendingPods)},
		{name: MetricClusterCPUAllocatableCores, labels: labels, value: float64(cpuAllocatableMilli) / 1000},
		{name: MetricClusterCPURequestedCores, labels: labels, value: float64(cpuRequestedMilli) / 1000},
		{name: MetricClusterCPUUtilizationRatio, labels: labels, value: cpuUtilization},
		{name: MetricClusterGPUAllocatable, labels: labels, value: float64(gpuAllocatable)},
		{name: MetricClusterGPURequested, labels: labels, value: float64(gpuRequested)},
		{name: MetricClusterGPURequestedMilli, labels: labels, value: float64(gpuRequestedMilli)},
		{name: MetricClusterGPUUtilizationRatio, labels: labels, value: gpuUtilization},
	}
	points := make([]MetricPoint, 0, len(samples))
	for _, s := range samples {
		point, err := NewScalarMetricPoint(s.name, s.labels, s.value, timestamp)
		if err != nil {
			return nil, err
		}
		points = append(points, point)
	}
	return points, nil
}

func withLabel(labels map[string]string, key, value string) map[string]string {
	result := make(map[string]string, len(labels)+1)
	for k, v := range labels {
		result[k] = v
	}
	result[key] = value
	return result
}
