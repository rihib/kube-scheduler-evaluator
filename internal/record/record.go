// Package record writes the transition record described in research.md section 11.5.
//
// In internal execution mode, the evaluator records every operation it can cause
// (research.md section 11.5) as one JSON line per changed mutable state, together with
// a header line that identifies the run. Out of scope: operations the evaluator cannot
// cause yet (reject, fail, suspend, resume, replicate, change_allocation, stop_node,
// recover_node, update_capacity, update_node_attribute), and external execution mode.
package record

import (
	"context"
	"fmt"
	"sync"

	"github.com/pfnet/kube-scheduler-evaluator/internal/clock"
	"github.com/pfnet/kube-scheduler-evaluator/pkg/definition"
)

// Operation names. They double as the column names of the update rule tables in
// research.md section 11, so the mapping to research.md's own operation names is:
//
//	create_task      -> タスクを作る
//	recreate         -> 再作成する
//	assign_node      -> 実行先ノードを割り当てる
//	start            -> 実行を開始する
//	complete         -> 完了する
//	delete           -> 削除する
//	release          -> リソースを解放する
//	register_node    -> ノードを登録する
//	deregister_node  -> ノードを削除する
const (
	OpCreateTask     = "create_task"
	OpRecreate       = "recreate"
	OpAssignNode     = "assign_node"
	OpStart          = "start"
	OpComplete       = "complete"
	OpDelete         = "delete"
	OpRelease        = "release"
	OpRegisterNode   = "register_node"
	OpDeregisterNode = "deregister_node"
)

// Mutable state names (research.md sections 10 and 11).
const (
	StateInstanceStatus    = "instance_status"
	StateAssignedNode      = "assigned_node"
	StateNodeInformation   = "node_information"
	StateAvailable         = "available"
	StateResourceCapacity  = "resource_capacity"
	StateAllocatedResource = "allocated_resource"
	StateNodeAttribute     = "node_attribute"
)

// TaskImmutable is the subset of a task's immutable state (research.md section 10.2)
// that create_task records alongside the task's first instance.
type TaskImmutable struct {
	Owner               string            `json:"owner"`
	Group               string            `json:"group"`
	JobID               string            `json:"job_id"`
	ResourceRequest     map[string]string `json:"resource_request"`
	ResourceLimit       map[string]string `json:"resource_limit"`
	PlacementConstraint map[string]string `json:"placement_constraint,omitempty"`
	PlacementPreference map[string]string `json:"placement_preference,omitempty"`
	Priority            int32             `json:"priority"`
}

// RunInfo is the information written to the header line of one run's record file.
type RunInfo struct {
	EvaluationID string
	ScenarioID   string
	Iteration    int
	Identity     definition.RunIdentity
}

type header struct {
	EvaluationID string        `json:"evaluation_id"`
	ScenarioID   string        `json:"scenario_id"`
	Iteration    int           `json:"iteration"`
	Input        inputInfo     `json:"input"`
	Execution    executionInfo `json:"execution"`
	Scheduler    schedulerInfo `json:"scheduler"`
}

type inputInfo struct {
	TraceName       string  `json:"trace_name"`
	TraceVersion    string  `json:"trace_version"`
	ReferenceCommit string  `json:"reference_commit"`
	InputSeed       string  `json:"input_seed"`
	Rho             float64 `json:"rho"`
}

type executionInfo struct {
	EvaluatorCommit string `json:"evaluator_commit"`
	ReferenceCommit string `json:"reference_commit"`
	Hardware        string `json:"hardware"`
}

type schedulerInfo struct {
	Profile string `json:"profile"`
}

// Transition is one line of the record body (research.md section 11.5).
type Transition struct {
	Time          string         `json:"time"`
	Seq           uint64         `json:"seq"`
	Operation     string         `json:"operation"`
	ComponentID   string         `json:"component_id"`
	TaskID        string         `json:"task_id,omitempty"`
	State         string         `json:"state"`
	StateKey      string         `json:"state_key,omitempty"`
	Before        any            `json:"before,omitempty"`
	After         any            `json:"after,omitempty"`
	TaskImmutable *TaskImmutable `json:"task_immutable,omitempty"`
}

// Writer writes one run's record file. One Writer is used for exactly one run.
type Writer interface {
	WriteLine(v any) error
	Close() error
}

// WriterFactory opens the Writer for one run.
type WriterFactory func(info RunInfo) (Writer, error)

// CommitProvider returns the evaluator's own commit.
type CommitProvider func() (string, error)

// HardwareInfoProvider returns a description of the machine running the evaluator.
type HardwareInfoProvider func() (string, error)

// Manager records the transitions of one run at a time.
//
// Manager is not safe for concurrent use across runs: BeginRun must be called once per
// run, and EndRun before the next BeginRun. Within a run, its methods are safe for
// concurrent use because the fake API server's reactors that call them run on different
// goroutines (the main goroutine and the scheduler goroutine).
type Manager interface {
	// BeginRun opens the record file for one run and writes its header line.
	BeginRun(ctx context.Context, info RunInfo) error
	// EndRun closes the current run's record file.
	EndRun() error

	RegisterNode(nodeID string, capacity, attribute map[string]string)
	DeregisterNode(nodeID string)

	// CreateTask records a task's first instance, together with the task's immutable state.
	CreateTask(taskID, instanceID string, immutable TaskImmutable)
	// Recreate records a new instance of a task whose previous instance ended in failed or deleted.
	Recreate(taskID, instanceID string)
	// Bind records assign_node and start together, because internal execution mode places and starts
	// an instance in the same binding operation (decisions.md).
	Bind(taskID, instanceID, nodeID string, allocated map[string]string)
	// Complete records an instance's completion. It does not revert assigned_node (research.md 11.1.2).
	Complete(taskID, instanceID string)
	// Delete records an instance's deletion, including reverting assigned_node.
	Delete(taskID, instanceID string)
}

func NewManager(writerFactory WriterFactory, commitProvider CommitProvider, hardwareInfoProvider HardwareInfoProvider) Manager {
	return &manager{
		writerFactory:        writerFactory,
		commitProvider:       commitProvider,
		hardwareInfoProvider: hardwareInfoProvider,
	}
}

type nodeState struct {
	capacity  map[string]string
	attribute map[string]string
}

type instanceState struct {
	taskID       string
	status       string
	assignedNode string // "" means unassigned (⊥)
}

type allocationState struct {
	resources map[string]string
}

type manager struct {
	writerFactory        WriterFactory
	commitProvider       CommitProvider
	hardwareInfoProvider HardwareInfoProvider

	mu          sync.Mutex
	writer      Writer
	seq         uint64
	nodes       map[string]*nodeState
	instances   map[string]*instanceState
	allocations map[string]*allocationState // keyed by instanceID
}

func (m *manager) BeginRun(ctx context.Context, info RunInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	w, err := m.writerFactory(info)
	if err != nil {
		return fmt.Errorf("record.BeginRun: failed to open writer: %w", err)
	}
	evaluatorCommit, err := m.commitProvider()
	if err != nil {
		w.Close()
		return fmt.Errorf("record.BeginRun: failed to get evaluator commit: %w", err)
	}
	hw, err := m.hardwareInfoProvider()
	if err != nil {
		w.Close()
		return fmt.Errorf("record.BeginRun: failed to get hardware info: %w", err)
	}
	h := header{
		EvaluationID: info.EvaluationID,
		ScenarioID:   info.ScenarioID,
		Iteration:    info.Iteration,
		Input: inputInfo{
			TraceName:       info.Identity.TraceName,
			TraceVersion:    info.Identity.TraceVersion,
			ReferenceCommit: info.Identity.ReferenceCommit,
			InputSeed:       info.Identity.InputSeed,
			Rho:             info.Identity.Rho,
		},
		Execution: executionInfo{
			EvaluatorCommit: evaluatorCommit,
			ReferenceCommit: info.Identity.ReferenceCommit,
			Hardware:        hw,
		},
		Scheduler: schedulerInfo{
			Profile: info.Identity.SchedulerProfile,
		},
	}
	if err := w.WriteLine(h); err != nil {
		w.Close()
		return fmt.Errorf("record.BeginRun: failed to write header: %w", err)
	}

	m.writer = w
	m.seq = 0
	m.nodes = make(map[string]*nodeState)
	m.instances = make(map[string]*instanceState)
	m.allocations = make(map[string]*allocationState)
	return nil
}

func (m *manager) EndRun() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writer == nil {
		return nil
	}
	err := m.writer.Close()
	m.writer = nil
	if err != nil {
		return fmt.Errorf("record.EndRun: failed to close writer: %w", err)
	}
	return nil
}

func (m *manager) write(op, componentID, taskID, state, stateKey string, before, after any, immutable *TaskImmutable) {
	m.seq++
	t := Transition{
		Time:          clock.VirtualTime().Format("2006-01-02T15:04:05.000000000Z07:00"),
		Seq:           m.seq,
		Operation:     op,
		ComponentID:   componentID,
		TaskID:        taskID,
		State:         state,
		StateKey:      stateKey,
		Before:        before,
		After:         after,
		TaskImmutable: immutable,
	}
	if m.writer == nil {
		return
	}
	_ = m.writer.WriteLine(t) // Errors here would have to be surfaced through every operation hook; a run-ending flush would be too late to matter. Logged by the caller's writer implementation if needed.
}

func (m *manager) RegisterNode(nodeID string, capacity, attribute map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[nodeID] = &nodeState{capacity: capacity, attribute: attribute}
	m.write(OpRegisterNode, nodeID, "", StateNodeInformation, "", nil, true, nil)
	m.write(OpRegisterNode, nodeID, "", StateAvailable, "", nil, true, nil)
	m.write(OpRegisterNode, nodeID, "", StateResourceCapacity, "", nil, capacity, nil)
	m.write(OpRegisterNode, nodeID, "", StateNodeAttribute, "", nil, attribute, nil)
}

func (m *manager) DeregisterNode(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, nodeID)
	m.write(OpDeregisterNode, nodeID, "", StateNodeInformation, "", true, nil, nil)
}

func (m *manager) CreateTask(taskID, instanceID string, immutable TaskImmutable) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.instances[instanceID] = &instanceState{taskID: taskID, status: "pending"}
	imm := immutable
	m.write(OpCreateTask, instanceID, taskID, StateInstanceStatus, "", nil, "pending", &imm)
	m.write(OpCreateTask, instanceID, taskID, StateAssignedNode, "", nil, nil, nil)
}

func (m *manager) Recreate(taskID, instanceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.instances[instanceID] = &instanceState{taskID: taskID, status: "pending"}
	m.write(OpRecreate, instanceID, taskID, StateInstanceStatus, "", nil, "pending", nil)
	m.write(OpRecreate, instanceID, taskID, StateAssignedNode, "", nil, nil, nil)
}

func (m *manager) Bind(taskID, instanceID, nodeID string, allocated map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inst, ok := m.instances[instanceID]
	if !ok {
		inst = &instanceState{taskID: taskID}
		m.instances[instanceID] = inst
	}

	var beforeNode any
	if inst.assignedNode != "" {
		beforeNode = inst.assignedNode
	}
	inst.assignedNode = nodeID
	m.write(OpAssignNode, instanceID, taskID, StateAssignedNode, "", beforeNode, nodeID, nil)

	m.allocations[instanceID] = &allocationState{resources: allocated}
	m.write(OpAssignNode, instanceID, taskID, StateAllocatedResource, instanceID, nil, allocated, nil)

	var beforeStatus any
	if inst.status != "" {
		beforeStatus = inst.status
	}
	inst.status = "running"
	m.write(OpStart, instanceID, taskID, StateInstanceStatus, "", beforeStatus, "running", nil)
}

func (m *manager) Complete(taskID, instanceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inst := m.instances[instanceID]
	var before any
	if inst != nil && inst.status != "" {
		before = inst.status
	}
	m.write(OpComplete, instanceID, taskID, StateInstanceStatus, "", before, "completed", nil)
	if inst != nil {
		inst.status = "completed"
	}
	m.release(taskID, instanceID)
}

func (m *manager) Delete(taskID, instanceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inst := m.instances[instanceID]
	var beforeStatus any
	var assignedBefore string
	if inst != nil {
		if inst.status != "" {
			beforeStatus = inst.status
		}
		assignedBefore = inst.assignedNode
	}
	m.write(OpDelete, instanceID, taskID, StateInstanceStatus, "", beforeStatus, "deleted", nil)
	if assignedBefore != "" {
		m.write(OpDelete, instanceID, taskID, StateAssignedNode, "", assignedBefore, nil, nil)
	}
	if inst != nil {
		inst.status = "deleted"
		inst.assignedNode = ""
	}
	m.release(taskID, instanceID)
}

// release records the release operation, if the instance has an allocation.
// It is called by both Complete and Delete (research.md 11.5), with the caller holding m.mu.
func (m *manager) release(taskID, instanceID string) {
	alloc, ok := m.allocations[instanceID]
	if !ok {
		return
	}
	delete(m.allocations, instanceID)
	m.write(OpRelease, instanceID, taskID, StateAllocatedResource, instanceID, alloc.resources, nil, nil)
}
