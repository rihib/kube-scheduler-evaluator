package event

import (
	"fmt"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/pkg/definition"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type Event interface {
	EventID() int
	SetEventID(id int)
	EventType() definition.EventType
	UnstructuredObject() *unstructured.Unstructured
	VirtualTimestamp() time.Time
	SetVirtualTimestamp(time.Time)
}

func NewEvent(
	eventType definition.EventType,
	object any,
	virtualTimestamp time.Time,
) (Event, error) {
	// Event IDs are not assigned here, they are assigned in execution order.
	if eventType.String() == "Unknown" {
		return nil, fmt.Errorf("event.NewEvent: invalid event type: %d", eventType)
	}
	var unstructuredObject *unstructured.Unstructured
	if eventType != definition.EventTypeFinish {
		if object == nil {
			return nil, fmt.Errorf(
				"event.NewEvent: object must not be nil for non-finish event types",
			)
		}
		objMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
		if err != nil {
			return nil, err
		}
		unstructuredObject = &unstructured.Unstructured{Object: objMap}
	}
	if eventType == definition.EventTypeDelete {
		if _, ok := object.(*corev1.Pod); ok {
			unstructuredObject.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			})
		}
	}
	return &event{
		eventType:          eventType,
		unstructuredObject: unstructuredObject,
		virtualTimestamp:   virtualTimestamp,
	}, nil
}

type event struct {
	eventID            int
	eventType          definition.EventType
	unstructuredObject *unstructured.Unstructured
	virtualTimestamp   time.Time
}

func (e *event) EventID() int {
	return e.eventID
}

func (e *event) SetEventID(id int) {
	e.eventID = id
}

func (e *event) EventType() definition.EventType {
	return e.eventType
}

func (e *event) UnstructuredObject() *unstructured.Unstructured {
	return e.unstructuredObject
}

func (e *event) VirtualTimestamp() time.Time {
	return e.virtualTimestamp
}

func (e *event) SetVirtualTimestamp(timestamp time.Time) {
	e.virtualTimestamp = timestamp
}
