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
// too (units/U03.md "Node 側の GPU の枚数も alibabacloud.com/gpu-count annotation で
// 持つ": decisions.md names no separate key for the Node side, so this unit reuses the
// Pod side's key for the Node's own "how many it has", distinct from the Pod's "how
// many it wants").
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

// parseNonNegativeCount parses an annotation's value as a non-negative integer (a
// count of GPUs, or a GPU's milli share), returning an error named after the
// annotation key if it is not one. GPU annotations never carry a "-" of their own
// (IndexAnnotationKey reserves "-" as the separator between GPU numbers, decisions.md),
// so any value this rejects is a genuinely malformed annotation, not a valid negative
// quantity that happens to look wrong.
func parseNonNegativeCount(key, raw string) (int, error) {
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid %s annotation %q", key, raw)
	}
	return n, nil
}

// NodeGPUCapacityOf reads a Node's annotations, even though it otherwise lives beside
// the Pod-side helpers in this file: it is exported from package pod, not from
// package cluster where its only caller (capacityOf) lives, so that it can share
// CountAnnotationKey, gpuResourceKey and gpuCapacityMilli with gpuAllocatedResourceOf
// below without exporting those details to cluster too, and without cluster and pod
// importing each other's GPU constants back and forth.
//
// NodeGPUCapacityOf returns the per-GPU resource_capacity entries (units/U03.md) for
// a Node whose CountAnnotationKey annotation names its number of GPUs, each holding
// gpuCapacityMilli. It returns nil, nil if the Node carries no CountAnnotationKey
// annotation (not a GPU node).
func NodeGPUCapacityOf(annotations map[string]string) (map[string]string, error) {
	raw, ok := annotations[CountAnnotationKey]
	if !ok {
		return nil, nil
	}
	count, err := parseNonNegativeCount(CountAnnotationKey, raw)
	if err != nil {
		return nil, fmt.Errorf("pod.NodeGPUCapacityOf: %w", err)
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
// by its MilliAnnotationKey annotation. It returns nil, nil if the Pod carries none of
// MilliAnnotationKey, CountAnnotationKey or IndexAnnotationKey (not a GPU Pod at all).
//
// A Pod that asked for a GPU (MilliAnnotationKey or CountAnnotationKey set) must
// already carry IndexAnnotationKey by the time this is called (at bind time): the
// kube-scheduler plugin path and the new entry point both set it before the Pod is
// recorded as bound (units/U03.md), so this function does not need to know which one
// did, but a GPU Pod reaching bind with no index yet is a bug upstream rather than a
// Pod this experiment modeled as "using no GPU" — recording it as such
// would let a GPU-bound Pod silently vanish from the aggregation's GPU usage.
func gpuAllocatedResourceOf(annotations map[string]string) (map[string]string, error) {
	indexRaw, hasIndex := annotations[IndexAnnotationKey]
	_, hasMilli := annotations[MilliAnnotationKey]
	countRaw, hasCount := annotations[CountAnnotationKey]

	if !hasIndex || indexRaw == "" {
		if hasMilli || hasCount {
			return nil, fmt.Errorf(
				"pod.gpuAllocatedResourceOf: %s or %s is set but %s is missing",
				MilliAnnotationKey, CountAnnotationKey, IndexAnnotationKey,
			)
		}
		return nil, nil
	}
	milliRaw, ok := annotations[MilliAnnotationKey]
	if !ok {
		return nil, fmt.Errorf("pod.gpuAllocatedResourceOf: %s is set but %s is missing", IndexAnnotationKey, MilliAnnotationKey)
	}
	milli, err := parseNonNegativeCount(MilliAnnotationKey, milliRaw)
	if err != nil {
		return nil, fmt.Errorf("pod.gpuAllocatedResourceOf: %w", err)
	}
	// Record the parsed number, not the annotation's own spelling: the capacity side
	// writes gpuCapacityMilli through strconv.Itoa (NodeGPUCapacityOf), so a Pod
	// annotated "+500" or "0500" would otherwise leave the record comparing shares in
	// two different spellings of the same quantity.
	milliValue := strconv.Itoa(milli)

	parts := strings.Split(indexRaw, "-")
	if hasCount {
		count, err := parseNonNegativeCount(CountAnnotationKey, countRaw)
		if err != nil {
			return nil, fmt.Errorf("pod.gpuAllocatedResourceOf: %w", err)
		}
		if len(parts) != count {
			return nil, fmt.Errorf(
				"pod.gpuAllocatedResourceOf: %s %q names %d GPUs but %s is %q",
				IndexAnnotationKey, indexRaw, len(parts), CountAnnotationKey, countRaw,
			)
		}
	}

	allocated := make(map[string]string, len(parts))
	for _, part := range parts {
		// strings.Split on "-" never hands back a part that itself starts with "-"
		// (IndexAnnotationKey's own separator), so Atoi failing here always means a
		// non-numeric part, never a valid negative GPU number to additionally reject.
		index, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("pod.gpuAllocatedResourceOf: invalid GPU index %q in %s %q", part, IndexAnnotationKey, indexRaw)
		}
		key := gpuResourceKey(index)
		if _, dup := allocated[key]; dup {
			return nil, fmt.Errorf("pod.gpuAllocatedResourceOf: %s %q names GPU %d more than once", IndexAnnotationKey, indexRaw, index)
		}
		allocated[key] = milliValue
	}
	return allocated, nil
}
