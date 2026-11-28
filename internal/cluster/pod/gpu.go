package pod

import (
	"fmt"
	"strconv"
	"strings"
)

// GPU annotation keys (decisions.md "GPU の annotation の形"): the same annotation
// format FGD's public implementation uses for 1-GPU-milli sharing. A Pod carries its
// own request (MilliAnnotationKey, CountAnnotationKey); the scheduler that places it
// writes the GPUs it chose to IndexAnnotationKey, their numbers joined by "-"
// (decisions.md). A Node carries the number of GPUs it has under CountAnnotationKey
// too (units/U03.md: decisions.md does not name a separate key for the Node side).
const (
	MilliAnnotationKey = "alibabacloud.com/gpu-milli"
	CountAnnotationKey = "alibabacloud.com/gpu-count"
	IndexAnnotationKey = "alibabacloud.com/gpu-index"

	// gpuCapacityMilli is the capacity of one GPU, in the same unit as
	// MilliAnnotationKey (decisions.md: "1 枚の容量は 1000 milli とする").
	gpuCapacityMilli = 1000
)

// gpuResourceKey returns the resource_capacity / allocated_resource map key
// (research.md section 10.5) for the GPU at index i (units/U03.md: capacity and
// allocation are recorded per GPU, not as one combined quantity).
func gpuResourceKey(index int) string {
	return fmt.Sprintf("gpu-%d", index)
}

// NodeGPUCapacityOf returns the per-GPU resource_capacity entries (units/U03.md) for
// a Node whose CountAnnotationKey annotation names its number of GPUs, each holding
// gpuCapacityMilli. It returns nil, nil if the Node carries no CountAnnotationKey
// annotation (not a GPU node).
func NodeGPUCapacityOf(annotations map[string]string) (map[string]string, error) {
	raw, ok := annotations[CountAnnotationKey]
	if !ok {
		return nil, nil
	}
	count, err := strconv.Atoi(raw)
	if err != nil || count < 0 {
		return nil, fmt.Errorf("pod.NodeGPUCapacityOf: invalid %s annotation %q", CountAnnotationKey, raw)
	}
	capacity := make(map[string]string, count)
	for i := 0; i < count; i++ {
		capacity[gpuResourceKey(i)] = strconv.Itoa(gpuCapacityMilli)
	}
	return capacity, nil
}

// gpuAllocatedResourceOf returns the per-GPU allocated_resource entries
// (units/U03.md) for a bound Pod whose IndexAnnotationKey annotation names the GPUs
// it was placed on (the scheduler's allocation result), each holding the share named
// by its MilliAnnotationKey annotation. It returns nil, nil if the Pod carries no
// IndexAnnotationKey annotation (not placed on a GPU), regardless of what placed it
// (units/U03.md: the kube-scheduler plugin path and the new entry point both set
// this annotation before the Pod is recorded as bound, so this function does not
// need to know which one did).
func gpuAllocatedResourceOf(annotations map[string]string) (map[string]string, error) {
	indexRaw, ok := annotations[IndexAnnotationKey]
	if !ok || indexRaw == "" {
		return nil, nil
	}
	milli, ok := annotations[MilliAnnotationKey]
	if !ok {
		return nil, fmt.Errorf("pod.gpuAllocatedResourceOf: %s is set but %s is missing", IndexAnnotationKey, MilliAnnotationKey)
	}
	allocated := make(map[string]string)
	for _, part := range strings.Split(indexRaw, "-") {
		index, err := strconv.Atoi(part)
		if err != nil || index < 0 {
			return nil, fmt.Errorf("pod.gpuAllocatedResourceOf: invalid GPU index %q in %s %q", part, IndexAnnotationKey, indexRaw)
		}
		allocated[gpuResourceKey(index)] = milli
	}
	return allocated, nil
}
