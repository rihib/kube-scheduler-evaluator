package definition

import (
	"context"
	"fmt"
	"regexp"

	"k8s.io/kubernetes/pkg/scheduler"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

// Scenario defines a sequence of events.
type Scenario interface {
	ScenarioID() string // ScenarioID returns the unique identifier of the scenario.
	Iteration() int     // Iteration returns the iteration number of the scenario.
	NextEvent(context.Context) <-chan Event
	SchedulerRegistry() runtime.Registry
	SchedulerOptions() []scheduler.Option
	// RunIdentity returns the information that identifies each run of this scenario
	// in the transition record (research.md section 11.5). The evaluator cannot derive
	// this itself: it does not know how the input was produced or how the scheduler
	// profile was configured, so the caller supplies it.
	RunIdentity() RunIdentity
}

// NewScenario creates a new Scenario instance.
func NewScenario(
	scenarioID string, iteration int, generator func(chan<- Event),
	registry runtime.Registry, identity RunIdentity, opts ...scheduler.Option,
) Scenario {
	matched, err := regexp.MatchString(`^[a-zA-Z][a-zA-Z0-9-]*$`, scenarioID)
	if err != nil {
		panic(err)
	}
	if !matched {
		panic(fmt.Sprintf("scenarioID '%s' must start with an alphabet and contain only alphanumeric characters or hyphens", scenarioID))
	}
	return &scenario{
		scenarioID:        fmt.Sprintf("scenario-%s", scenarioID),
		iteration:         iteration,
		generator:         generator,
		schedulerRegistry: registry,
		schedulerOptions:  opts,
		runIdentity:       identity,
	}
}

type scenario struct {
	scenarioID        string
	iteration         int
	generator         func(chan<- Event)
	schedulerRegistry runtime.Registry
	schedulerOptions  []scheduler.Option
	runIdentity       RunIdentity
}

func (s *scenario) ScenarioID() string {
	return s.scenarioID
}

func (s *scenario) Iteration() int {
	return s.iteration
}

func (s *scenario) NextEvent(ctx context.Context) <-chan Event {
	ch := make(chan Event)
	go func() {
		defer close(ch)
		s.generator(ch)
		finish := newFinishEvent()
		ch <- finish
	}()
	return ch
}

func (s *scenario) SchedulerRegistry() runtime.Registry {
	return s.schedulerRegistry
}

func (s *scenario) SchedulerOptions() []scheduler.Option {
	return s.schedulerOptions
}

func (s *scenario) RunIdentity() RunIdentity {
	return s.runIdentity
}
