package util

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

type RetryableFunc func() error

func WithRetry(
	ctx context.Context, caller string,
	maxAttempts int, initialBackoff time.Duration,
	fn RetryableFunc,
) error {
	var err error
	backoff := initialBackoff
	for i := range maxAttempts {
		err = fn()
		if err == nil {
			return nil
		}
		if i == maxAttempts-1 {
			break
		}
		slog.WarnContext(
			ctx, "retrying after error",
			"caller", caller,
			"attempt", i+1,
			"max_attempts", maxAttempts,
			"backoff", backoff.String(),
			"error", err,
		)
		time.Sleep(backoff)
		backoff *= 2
	}
	return err
}

type ResourceKey struct {
	Kind           string
	NamespacedName types.NamespacedName
}

func (k *ResourceKey) String() string {
	name := k.NamespacedName.Name
	if name == "" {
		name = "<name>"
	}
	if k.NamespacedName.Namespace == "" {
		return fmt.Sprintf("%s/%s", k.Kind, name)
	}
	return fmt.Sprintf("%s/%s/%s", k.Kind, k.NamespacedName.Namespace, name)
}

func (k *ResourceKey) LogValue() slog.Value {
	return slog.StringValue(k.String())
}

func ResourceKeyOf(obj any) *ResourceKey {
	switch obj := obj.(type) {
	case *unstructured.Unstructured:
		return unstructuredResourceKeyOf(obj)
	case *corev1.Pod:
		return podResourceKeyOf(obj)
	default:
		return nil
	}
}

func ParseResourceKey(s string) (*ResourceKey, error) {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	if start <= len(s) {
		parts = append(parts, s[start:])
	}

	var rk ResourceKey
	if len(parts) == 2 {
		rk.Kind = parts[0]
		rk.NamespacedName = types.NamespacedName{Name: parts[1]}
		return &rk, nil
	}
	if len(parts) == 3 {
		rk.Kind = parts[0]
		rk.NamespacedName = types.NamespacedName{Namespace: parts[1], Name: parts[2]}
		return &rk, nil
	}
	return nil, fmt.Errorf("invalid resource key format: %s", s)
}

func unstructuredResourceKeyOf(obj *unstructured.Unstructured) *ResourceKey {
	kind := obj.GetKind()
	// If event type is a create event and using generateName,
	// obj.GetName() returns an empty string.
	name := obj.GetName()
	namespace := obj.GetNamespace()
	// Node does not have namespace
	if kind != "Node" && namespace == "" {
		slog.Warn("object has no namespace", "object", fmt.Sprintf("%s/%s", kind, name))
	}
	return &ResourceKey{
		Kind:           kind,
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
	}
}

func podResourceKeyOf(pod *corev1.Pod) *ResourceKey {
	return &ResourceKey{
		Kind:           "Pod", // pod.Kind is always empty unless explicitly set
		NamespacedName: types.NamespacedName{Namespace: pod.GetNamespace(), Name: pod.GetName()},
	}
}
