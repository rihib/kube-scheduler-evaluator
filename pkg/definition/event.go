package definition

import (
	"log/slog"
	"time"
)

// Event is a basic unit that composes a scenario.
type Event interface {
	EventType() EventType    // EventType returns the type of the event, such as "Create", "Delete".
	Object() any             // Object returns the object associated with the event, such as ReplicaSet, Pod, Node, etc.
	Interval() time.Duration // Interval returns the interval duration before the next event.
}

// NewEvent creates a new Event instance.
func NewEvent(
	eventType EventType,
	object any,
	interval time.Duration,
) Event {
	if eventType == EventTypeFinish {
		panic("use newFinishEvent for finish event")
	}
	if eventType != EventTypeCreate && eventType != EventTypeDelete {
		panic("invalid eventType")
	}
	if object == nil {
		panic("object must not be nil")
	}
	if interval < 0 {
		panic("interval must not be negative")
	}
	return &event{
		eventType: eventType,
		object:    object,
		interval:  interval,
	}
}

func newFinishEvent() Event {
	return &event{
		eventType: EventTypeFinish,
	}
}

type event struct {
	eventType EventType
	object    any
	interval  time.Duration
}

func (e *event) EventType() EventType {
	return e.eventType
}

func (e *event) Object() any {
	return e.object
}

func (e *event) Interval() time.Duration {
	return e.interval
}

type EventType int

const (
	EventTypeCreate EventType = iota
	EventTypeDelete
	EventTypeFinish
)

func (e EventType) String() string {
	switch e {
	case EventTypeCreate:
		return "Create"
	case EventTypeDelete:
		return "Delete"
	case EventTypeFinish:
		return "Finish"
	default:
		return "Unknown"
	}
}

func (e EventType) LogValue() slog.Value {
	return slog.StringValue(e.String())
}
