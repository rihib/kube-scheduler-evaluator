package evaluator

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/pkg/definition"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

const testUserID = "tester"

// fakeRecordWriter is the Fake writer used by the unit tests below (units/U01.md:
// "記録の書き出し先...は、evaluator を作るときに外から渡せるようにする...テストで Fake を渡すため").
// It keeps the written lines as parsed JSON objects, in order, instead of writing a file.
//
// failFromLine, when positive, makes WriteLine fail from that line onwards (1 is the
// header line), standing in for a writer that stops accepting lines mid-run, such as a
// full disk.
type fakeRecordWriter struct {
	failFromLine int

	mu    sync.Mutex
	lines []map[string]any
}

var errFakeWriteFailure = errors.New("fake writer: out of space")

func (w *fakeRecordWriter) WriteLine(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failFromLine > 0 && len(w.lines)+1 >= w.failFromLine {
		return errFakeWriteFailure
	}
	w.lines = append(w.lines, m)
	return nil
}

func (w *fakeRecordWriter) Close() error { return nil }

func (w *fakeRecordWriter) linesCopy() []map[string]any {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]map[string]any, len(w.lines))
	copy(out, w.lines)
	return out
}

// fakeRecordOptions returns the config options carrying Fakes for the record's writer,
// the evaluator commit and the hardware info (units/U01.md). Every run of the scenario
// is handed the same writer, so that a scenario with more than one iteration leaves the
// lines of all of its runs in one place, each run starting with its own header line.
func fakeRecordOptions(w *fakeRecordWriter) []ConfigOption {
	return []ConfigOption{
		WithRecordWriterFactory(func(RecordRunInfo) (RecordWriter, error) {
			return w, nil
		}),
		WithCommitProvider(func() (string, error) {
			return "fake-evaluator-commit", nil
		}),
		WithHardwareInfoProvider(func() (string, error) {
			return "fake-hardware", nil
		}),
	}
}

func testNode(cpu, mem string) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "node-",
			Labels:       map[string]string{"zone": "a"},
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpu),
				corev1.ResourceMemory: resource.MustParse(mem),
				corev1.ResourcePods:   resource.MustParse("110"),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpu),
				corev1.ResourceMemory: resource.MustParse(mem),
				corev1.ResourcePods:   resource.MustParse("110"),
			},
		},
	}
	node.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Node"))
	return node
}

// testReplicaSet builds a one-Pod ReplicaSet (one task, units/U01.md), with the
// annotations the evaluator needs to decide when the Pod's execution ends.
func testReplicaSet(namePrefix, podCPU string, priority int32, executionDuration time.Duration) *appsv1.ReplicaSet {
	podLabels := map[string]string{"app": namePrefix}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: namePrefix + "-",
			Namespace:    "default",
			Annotations: map[string]string{
				definition.DeadlineDurationAnnotationKey(testUserID): "1h",
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: ptr.To[int32](1),
			Selector: &metav1.LabelSelector{
				MatchLabels: podLabels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels,
					Annotations: map[string]string{
						definition.ExecutionDurationAnnotationKey(testUserID): executionDuration.String(),
					},
				},
				Spec: corev1.PodSpec{
					SchedulerName: "default-scheduler",
					Priority:      ptr.To(priority),
					Containers: []corev1.Container{
						{
							Name:  "c",
							Image: "example.com/image:1.0",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(podCPU)},
								Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(podCPU)},
							},
						},
					},
				},
			},
		},
	}
	rs.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))
	return rs
}

func testIdentity() definition.RunIdentity {
	return definition.RunIdentity{
		TraceName:        "openb",
		TraceVersion:     "v1",
		ReferenceCommit:  "reference-commit-abc",
		InputSeed:        "seed-1",
		Rho:              1.1,
		SchedulerProfile: "LeastAllocated",
	}
}

// runEvaluatorExitCode runs the scenario the given number of iterations with w as the
// record's writer, and returns the exit code of the run.
func runEvaluatorExitCode(t *testing.T, w *fakeRecordWriter, iteration int, generator func(chan<- definition.Event)) int {
	t.Helper()
	cfg := WithConfig(testUserID, "", "", "", false, slog.LevelError, fakeRecordOptions(w)...)
	eval, err := New(
		cfg,
		WithMetricStores(),
		WithScenario("test", iteration, generator, nil, testIdentity()),
	)
	if err != nil {
		t.Fatalf("evaluator.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return eval.Run(ctx)
}

// runEvaluator runs the scenario once and returns the lines of the record it wrote.
func runEvaluator(t *testing.T, generator func(chan<- definition.Event)) []map[string]any {
	t.Helper()
	w := &fakeRecordWriter{}
	if code := runEvaluatorExitCode(t, w, 1, generator); code != 0 {
		t.Fatalf("evaluator.Run: exit code %d", code)
	}
	return w.linesCopy()
}

// findTransitions returns, in order, the transition lines (excluding the header) whose
// operation and component_id match. componentID may be "" to match any component.
func findTransitions(lines []map[string]any, operation, componentID string) []map[string]any {
	var out []map[string]any
	for _, l := range lines[1:] {
		if l["operation"] != operation {
			continue
		}
		if componentID != "" && l["component_id"] != componentID {
			continue
		}
		out = append(out, l)
	}
	return out
}

// findTransitionsByStateKey returns, in order, the transition lines whose operation
// and state_key match. Used for node-keyed mutable states recorded on behalf of an
// instance (allocated_resource, research.md section 10.5), where component_id is the
// node and state_key is the instance.
func findTransitionsByStateKey(lines []map[string]any, operation, stateKey string) []map[string]any {
	var out []map[string]any
	for _, l := range lines[1:] {
		if l["operation"] != operation {
			continue
		}
		if l["state_key"] != stateKey {
			continue
		}
		out = append(out, l)
	}
	return out
}

func mustFloat(t *testing.T, v any, what string) float64 {
	t.Helper()
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("%s: expected a number, got %T (%v)", what, v, v)
	}
	return f
}

// TestRun_NormalLifecycle covers a Pod that is created, placed, started and completed
// without contention: create_task, assign_node, start, complete and release.
func TestRun_NormalLifecycle(t *testing.T) {
	lines := runEvaluator(t, func(ch chan<- definition.Event) {
		ch <- definition.NewEvent(definition.EventTypeCreate, testNode("4", "8Gi"), 0)
		ch <- definition.NewEvent(definition.EventTypeCreate, testReplicaSet("job", "1", 0, 10*time.Second), 0)
	})
	if len(lines) == 0 {
		t.Fatalf("expected at least the header line, got none")
	}

	t.Run("header identifies the run", func(t *testing.T) {
		header := lines[0]
		input, ok := header["input"].(map[string]any)
		if !ok {
			t.Fatalf("header has no input section: %v", header)
		}
		if got := input["trace_name"]; got != "openb" {
			t.Errorf("input.trace_name = %v, want openb", got)
		}
		if got := mustFloat(t, input["rho"], "input.rho"); got != 1.1 {
			t.Errorf("input.rho = %v, want 1.1", got)
		}
		execution, ok := header["execution"].(map[string]any)
		if !ok {
			t.Fatalf("header has no execution section: %v", header)
		}
		if got := execution["evaluator_commit"]; got != "fake-evaluator-commit" {
			t.Errorf("execution.evaluator_commit = %v, want fake-evaluator-commit", got)
		}
		if got := execution["reference_commit"]; got != "reference-commit-abc" {
			t.Errorf("execution.reference_commit = %v, want reference-commit-abc", got)
		}
		if got := execution["hardware"]; got != "fake-hardware" {
			t.Errorf("execution.hardware = %v, want fake-hardware", got)
		}
		scheduler, ok := header["scheduler"].(map[string]any)
		if !ok {
			t.Fatalf("header has no scheduler section: %v", header)
		}
		if got := scheduler["profile"]; got != "LeastAllocated" {
			t.Errorf("scheduler.profile = %v, want LeastAllocated", got)
		}
		if evalID, _ := header["evaluation_id"].(string); evalID == "" {
			t.Errorf("header has no evaluation_id")
		}
	})

	t.Run("register_node records the node's labels as node_attribute", func(t *testing.T) {
		attributeRecords := findTransitions(lines, "register_node", "")
		found := false
		for _, r := range attributeRecords {
			if r["state"] != "node_attribute" {
				continue
			}
			found = true
			after, ok := r["after"].(map[string]any)
			if !ok {
				t.Fatalf("register_node node_attribute after = %v, want a map", r["after"])
			}
			if after["zone"] != "a" {
				t.Errorf("registered node_attribute zone = %v, want a", after["zone"])
			}
		}
		if !found {
			t.Fatalf("no register_node node_attribute record found in %v", attributeRecords)
		}
	})

	t.Run("register_node records the node's capacity at the start", func(t *testing.T) {
		capacityRecords := findTransitions(lines, "register_node", "")
		found := false
		for _, r := range capacityRecords {
			if r["state"] != "resource_capacity" {
				continue
			}
			found = true
			after, ok := r["after"].(map[string]any)
			if !ok {
				t.Fatalf("register_node resource_capacity after = %v, want a map", r["after"])
			}
			if after["cpu"] != "4" {
				t.Errorf("registered node cpu capacity = %v, want 4", after["cpu"])
			}
			if _, hasBefore := r["before"]; hasBefore {
				t.Errorf("register_node should have no before value, got %v", r["before"])
			}
		}
		if !found {
			t.Fatalf("no register_node resource_capacity record found in %v", capacityRecords)
		}
	})

	t.Run("the Pod's instance goes create_task -> assign_node -> start -> complete -> release in virtual time order", func(t *testing.T) {
		createRecords := findTransitions(lines, "create_task", "")
		if len(createRecords) == 0 {
			t.Fatalf("no create_task record found")
		}
		instanceID, _ := createRecords[0]["component_id"].(string)
		if instanceID == "" {
			t.Fatalf("create_task record has no component_id: %v", createRecords[0])
		}

		assignRecords := findTransitions(lines, "assign_node", instanceID)
		startRecords := findTransitions(lines, "start", instanceID)
		completeRecords := findTransitions(lines, "complete", instanceID)
		// release's component_id is the node the allocation was released from, not the
		// instance (same keying as assign_node's allocated_resource record), so it is
		// found by state_key instead.
		releaseRecords := findTransitionsByStateKey(lines, "release", instanceID)
		for name, recs := range map[string][]map[string]any{
			"assign_node": assignRecords, "start": startRecords,
			"complete": completeRecords, "release": releaseRecords,
		} {
			if len(recs) == 0 {
				t.Fatalf("no %s record found for instance %s", name, instanceID)
			}
		}

		seqOf := func(r map[string]any) float64 { return mustFloat(t, r["seq"], "seq") }
		if !(seqOf(createRecords[0]) < seqOf(assignRecords[0]) &&
			seqOf(assignRecords[0]) < seqOf(startRecords[0]) &&
			seqOf(startRecords[0]) < seqOf(completeRecords[0]) &&
			seqOf(completeRecords[0]) <= seqOf(releaseRecords[0])) {
			t.Errorf("records are not in the expected order: create=%v assign=%v start=%v complete=%v release=%v",
				createRecords[0]["seq"], assignRecords[0]["seq"], startRecords[0]["seq"], completeRecords[0]["seq"], releaseRecords[0]["seq"])
		}

		startTime, _ := startRecords[0]["time"].(string)
		completeTime, _ := completeRecords[0]["time"].(string)
		if startTime == "" || completeTime == "" {
			t.Fatalf("start or complete record has no time: start=%v complete=%v", startRecords[0], completeRecords[0])
		}
		if startTime == completeTime {
			t.Errorf("start and complete happened at the same virtual time, want complete to be 10s after start")
		}

		// assign_node and start record at the same virtual time, because internal
		// execution mode places and starts an instance in the same binding operation
		// (units/U01.md).
		assignTime, _ := assignRecords[0]["time"].(string)
		if assignTime == "" {
			t.Fatalf("assign_node record has no time: %v", assignRecords[0])
		}
		if assignTime != startTime {
			t.Errorf("assign_node happened at %v but start happened at %v, want the same virtual time", assignTime, startTime)
		}

		// assign_node's allocated_resource after value must reflect the Pod's actual
		// resource request, keyed by the node that was assigned (research.md section 10.5).
		allocatedRecords := findTransitionsByStateKey(lines, "assign_node", instanceID)
		allocFound := false
		for _, r := range allocatedRecords {
			if r["state"] != "allocated_resource" {
				continue
			}
			allocFound = true
			after, ok := r["after"].(map[string]any)
			if !ok {
				t.Fatalf("assign_node allocated_resource after = %v, want a map", r["after"])
			}
			if after["cpu"] != "1" {
				t.Errorf("assign_node allocated_resource.cpu = %v, want 1", after["cpu"])
			}
		}
		if !allocFound {
			t.Fatalf("no assign_node allocated_resource record found for instance %s in %v", instanceID, allocatedRecords)
		}

		// complete must not revert assigned_node (research.md section 11.1.2): the
		// Pod has an assigned_node transition only from assign_node, not from complete.
		assignedNodeChanges := findTransitions(lines, "complete", instanceID)
		for _, r := range assignedNodeChanges {
			if r["state"] == "assigned_node" {
				t.Errorf("complete must not record an assigned_node change, but found one: %v", r)
			}
		}
	})

	t.Run("create_task records the task's immutable state", func(t *testing.T) {
		createRecords := findTransitions(lines, "create_task", "")
		var withImmutable map[string]any
		for _, r := range createRecords {
			if imm, ok := r["task_immutable"]; ok && imm != nil {
				withImmutable = r
				break
			}
		}
		if withImmutable == nil {
			t.Fatalf("no create_task record carries task_immutable: %v", createRecords)
		}
		imm, ok := withImmutable["task_immutable"].(map[string]any)
		if !ok {
			t.Fatalf("task_immutable is not an object: %v", withImmutable["task_immutable"])
		}
		if imm["group"] != "default" {
			t.Errorf("task_immutable.group = %v, want default", imm["group"])
		}
		req, ok := imm["resource_request"].(map[string]any)
		if !ok || req["cpu"] != "1" {
			t.Errorf("task_immutable.resource_request.cpu = %v, want 1", imm["resource_request"])
		}
		limit, ok := imm["resource_limit"].(map[string]any)
		if !ok || limit["cpu"] != "1" {
			t.Errorf("task_immutable.resource_limit.cpu = %v, want 1", imm["resource_limit"])
		}
		// One openb row is one job holding exactly one task (units/U01.md), so the job
		// is identified by the task itself.
		if imm["job_id"] != withImmutable["task_id"] {
			t.Errorf("task_immutable.job_id = %v, want the task's own id %v", imm["job_id"], withImmutable["task_id"])
		}
		if got := mustFloat(t, imm["priority"], "task_immutable.priority"); got != 0 {
			t.Errorf("task_immutable.priority = %v, want 0", imm["priority"])
		}
		// This Pod has no node selector, so it carries no placement constraint.
		if constraint, ok := imm["placement_constraint"]; ok {
			t.Errorf("task_immutable.placement_constraint = %v, want it to be absent", constraint)
		}
	})
}

// TestRun_NodeDeregistration covers a Node that the scenario removes: register_node at
// creation, deregister_node at deletion.
func TestRun_NodeDeregistration(t *testing.T) {
	node := testNode("4", "8Gi")
	node.Name = "deregistration-test-node"
	deleted := node.DeepCopy()

	lines := runEvaluator(t, func(ch chan<- definition.Event) {
		ch <- definition.NewEvent(definition.EventTypeCreate, node, 0)
		ch <- definition.NewEvent(definition.EventTypeDelete, deleted, time.Second)
	})

	registerRecords := findTransitions(lines, "register_node", node.Name)
	if len(registerRecords) == 0 {
		t.Fatalf("no register_node record found for node %s", node.Name)
	}
	deregisterRecords := findTransitions(lines, "deregister_node", node.Name)
	if len(deregisterRecords) == 0 {
		t.Fatalf("no deregister_node record found for node %s", node.Name)
	}
	if mustFloat(t, registerRecords[0]["seq"], "seq") >= mustFloat(t, deregisterRecords[0]["seq"], "seq") {
		t.Errorf("register_node (seq %v) should come before deregister_node (seq %v)",
			registerRecords[0]["seq"], deregisterRecords[0]["seq"])
	}
	if before, ok := deregisterRecords[0]["before"]; !ok || before != true {
		t.Errorf("deregister_node's node_information before value = %v, want true", before)
	}
	if _, hasAfter := deregisterRecords[0]["after"]; hasAfter {
		t.Errorf("deregister_node's node_information after value should be absent, got %v", deregisterRecords[0]["after"])
	}
}

// TestRun_PreemptionCausesDeleteAndRecreate covers a Pod preempted by a higher-priority
// Pod: the preempted Pod's instance records "delete" (not "complete", since it has not
// run to the end of its execution duration), and the ReplicaSet's resync creates a new
// instance of the same task, recorded as "recreate".
func TestRun_PreemptionCausesDeleteAndRecreate(t *testing.T) {
	lines := runEvaluator(t, func(ch chan<- definition.Event) {
		ch <- definition.NewEvent(definition.EventTypeCreate, testNode("1", "8Gi"), 0)
		ch <- definition.NewEvent(definition.EventTypeCreate, testReplicaSet("low", "1", 0, time.Hour), 0)
		ch <- definition.NewEvent(definition.EventTypeCreate, testReplicaSet("high", "1", 100, time.Hour), time.Second)
	})

	deleteRecords := findTransitions(lines, "delete", "")
	if len(deleteRecords) == 0 {
		t.Fatalf("no delete record found; expected the low-priority Pod to be preempted")
	}
	preemptedInstanceID, _ := deleteRecords[0]["component_id"].(string)
	if preemptedInstanceID == "" {
		t.Fatalf("delete record has no component_id: %v", deleteRecords[0])
	}

	// complete must not also be recorded for the same instance: it was preempted, not
	// completed by running to the end of its execution duration.
	if completeRecords := findTransitions(lines, "complete", preemptedInstanceID); len(completeRecords) != 0 {
		t.Errorf("preempted instance %s should not also have a complete record: %v", preemptedInstanceID, completeRecords)
	}

	// delete must revert assigned_node (the opposite of complete, units/U01.md): the
	// preempted instance was bound to a node before it was deleted.
	deleteAssignedNodeChanges := findTransitions(lines, "delete", preemptedInstanceID)
	foundAssignedNodeRevert := false
	for _, r := range deleteAssignedNodeChanges {
		if r["state"] != "assigned_node" {
			continue
		}
		foundAssignedNodeRevert = true
		if before, _ := r["before"].(string); before == "" {
			t.Errorf("delete's assigned_node before value = %v, want the node the instance was bound to", r["before"])
		}
		if _, hasAfter := r["after"]; hasAfter {
			t.Errorf("delete's assigned_node after value should be absent, got %v", r["after"])
		}
	}
	if !foundAssignedNodeRevert {
		t.Errorf("no delete record reverting assigned_node found for instance %s: %v", preemptedInstanceID, deleteAssignedNodeChanges)
	}

	taskID, _ := deleteRecords[0]["task_id"].(string)
	if taskID == "" {
		t.Fatalf("delete record has no task_id: %v", deleteRecords[0])
	}
	recreateRecords := findTransitions(lines, "recreate", "")
	found := false
	for _, r := range recreateRecords {
		if r["task_id"] == taskID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("no recreate record found for task %s after it was preempted: %v", taskID, recreateRecords)
	}
}

// TestRun_FailsWhenTheRecordWriterFails covers a writer that stops accepting lines in
// the middle of a run (a full disk, say): the run must report failure, because a record
// missing some of its transitions is not a record of the run that happened, and the
// aggregation that reads it later cannot tell the difference (units/U01.md).
func TestRun_FailsWhenTheRecordWriterFails(t *testing.T) {
	// Fail from the first transition line, keeping the header line written, so the run
	// gets as far as producing transitions before the writer starts refusing them.
	w := &fakeRecordWriter{failFromLine: 2}
	code := runEvaluatorExitCode(t, w, 1, func(ch chan<- definition.Event) {
		ch <- definition.NewEvent(definition.EventTypeCreate, testNode("4", "8Gi"), 0)
		ch <- definition.NewEvent(definition.EventTypeCreate, testReplicaSet("job", "1", 0, 10*time.Second), 0)
	})
	if code == 0 {
		t.Errorf("evaluator.Run: exit code 0, want non-zero when the record writer fails")
	}
	if lines := w.linesCopy(); len(lines) != 1 {
		t.Errorf("the writer accepted %d lines, want only the header line", len(lines))
	}
}

// splitRuns splits the lines written by a scenario with more than one iteration into
// one slice per run, each starting with that run's header line. A header line is the
// only line without an operation.
func splitRuns(lines []map[string]any) [][]map[string]any {
	var runs [][]map[string]any
	for _, l := range lines {
		if _, isTransition := l["operation"]; !isTransition {
			runs = append(runs, []map[string]any{l})
			continue
		}
		if len(runs) == 0 {
			continue // a transition before any header: left to the caller's assertions
		}
		runs[len(runs)-1] = append(runs[len(runs)-1], l)
	}
	return runs
}

// TestRun_EachIterationRecordsItsOwnRun covers a scenario run twice in one process
// (units/U01.md: one record per run). Each run starts from an empty cluster, so each
// run's record carries the whole of the cluster's state at the start of its own
// observation period: the Node is registered again, and the Pod's lifecycle is recorded
// again from create_task to complete.
//
// The Node has a fixed name rather than a GenerateName, so that a Node left behind by
// the previous run is not merely invisible in the record but makes the second run's
// Node creation fail outright with an already-exists error.
//
// What this guards and what it does not: the fixed Node name guards the Node side, where
// state left behind by the previous run is observable from outside. The Pod side is not
// guarded. The Pod recorder keys its per-run maps on the Pod's namespace/name and on the
// owner ReplicaSet's UID, and the fake API server gives every Pod a random name
// (GenerateName + rand.String) and a fresh UID, so entries left over from the first run
// can never be hit by the second one. Leaking those maps across runs is therefore
// unobservable here, and a run's record stays correct either way; guarding it would mean
// injecting the maps or pinning the Pod names, which this test deliberately does not do.
func TestRun_EachIterationRecordsItsOwnRun(t *testing.T) {
	const nodeName = "repeated-run-node"
	w := &fakeRecordWriter{}
	code := runEvaluatorExitCode(t, w, 2, func(ch chan<- definition.Event) {
		node := testNode("4", "8Gi")
		node.Name = nodeName
		ch <- definition.NewEvent(definition.EventTypeCreate, node, 0)
		ch <- definition.NewEvent(definition.EventTypeCreate, testReplicaSet("job", "1", 0, 10*time.Second), 0)
	})
	if code != 0 {
		t.Fatalf("evaluator.Run: exit code %d, want 0", code)
	}

	runs := splitRuns(w.linesCopy())
	if len(runs) != 2 {
		t.Fatalf("got %d runs in the record, want 2 (one header line per run)", len(runs))
	}
	for i, lines := range runs {
		header := lines[0]
		if got := mustFloat(t, header["iteration"], "header.iteration"); got != float64(i) {
			t.Errorf("run %d: header.iteration = %v, want %d", i, header["iteration"], i)
		}
		if got := findTransitions(lines, "register_node", nodeName); len(got) == 0 {
			t.Errorf("run %d: no register_node record for node %s: %v", i, nodeName, lines)
		}
		createRecords := findTransitions(lines, "create_task", "")
		if len(createRecords) == 0 {
			t.Fatalf("run %d: no create_task record: %v", i, lines)
		}
		instanceID, _ := createRecords[0]["component_id"].(string)
		if got := findTransitions(lines, "complete", instanceID); len(got) == 0 {
			t.Errorf("run %d: instance %s has no complete record; the Pod ran to the end of its execution duration: %v",
				i, instanceID, lines)
		}
	}
}

// cpuContainer returns a container requesting and limiting the given amount of CPU.
func cpuContainer(name, cpu string) corev1.Container {
	return corev1.Container{
		Name:  name,
		Image: "example.com/image:1.0",
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
		},
	}
}

// requestAndAllocatedCPUOf returns the CPU in the task's immutable resource_request and
// in the allocated_resource the instance holds on its node, as recorded for the one
// task of the given run.
func requestAndAllocatedCPUOf(t *testing.T, lines []map[string]any) (request, allocated string) {
	t.Helper()
	createRecords := findTransitions(lines, "create_task", "")
	if len(createRecords) == 0 {
		t.Fatalf("no create_task record found")
	}
	instanceID, _ := createRecords[0]["component_id"].(string)
	for _, r := range createRecords {
		imm, ok := r["task_immutable"].(map[string]any)
		if !ok {
			continue
		}
		req, ok := imm["resource_request"].(map[string]any)
		if !ok {
			t.Fatalf("task_immutable.resource_request is not an object: %v", imm["resource_request"])
		}
		request, _ = req["cpu"].(string)
	}
	for _, r := range findTransitionsByStateKey(lines, "assign_node", instanceID) {
		if r["state"] != "allocated_resource" {
			continue
		}
		after, ok := r["after"].(map[string]any)
		if !ok {
			t.Fatalf("assign_node allocated_resource after is not an object: %v", r["after"])
		}
		allocated, _ = after["cpu"].(string)
	}
	if request == "" || allocated == "" {
		t.Fatalf("no resource_request (%q) or allocated_resource (%q) recorded: %v", request, allocated, lines)
	}
	return request, allocated
}

// TestRun_MultiContainerPodRecordsAllOfItsContainers covers a Pod with more than one
// container: the task's resource_request and the instance's allocated_resource must
// cover every container, not just the first one, because that is what the scheduler
// reserves on the node.
func TestRun_MultiContainerPodRecordsAllOfItsContainers(t *testing.T) {
	lines := runEvaluator(t, func(ch chan<- definition.Event) {
		rs := testReplicaSet("multi", "1", 0, 10*time.Second)
		rs.Spec.Template.Spec.Containers = append(rs.Spec.Template.Spec.Containers, cpuContainer("sidecar", "2"))
		ch <- definition.NewEvent(definition.EventTypeCreate, testNode("4", "8Gi"), 0)
		ch <- definition.NewEvent(definition.EventTypeCreate, rs, 0)
	})

	request, allocated := requestAndAllocatedCPUOf(t, lines)
	if request != "3" {
		t.Errorf("task_immutable.resource_request.cpu = %v, want 3 (1 + 2 over both containers)", request)
	}
	if allocated != "3" {
		t.Errorf("assign_node allocated_resource.cpu = %v, want 3 (1 + 2 over both containers)", allocated)
	}
}

// TestRun_InitContainerCountsTowardThePodsRequest covers a Pod whose init container
// requests more than its regular containers. kube-scheduler reserves
// max(sum of the regular containers, the largest init container) on the node, so a
// record that left the init container out would report less than the scheduler actually
// held, and the aggregation would see a node as emptier than it was.
func TestRun_InitContainerCountsTowardThePodsRequest(t *testing.T) {
	lines := runEvaluator(t, func(ch chan<- definition.Event) {
		rs := testReplicaSet("init", "1", 0, 10*time.Second)
		rs.Spec.Template.Spec.InitContainers = []corev1.Container{cpuContainer("setup", "3")}
		ch <- definition.NewEvent(definition.EventTypeCreate, testNode("4", "8Gi"), 0)
		ch <- definition.NewEvent(definition.EventTypeCreate, rs, 0)
	})

	request, allocated := requestAndAllocatedCPUOf(t, lines)
	if request != "3" {
		t.Errorf("task_immutable.resource_request.cpu = %v, want 3 (the init container, larger than the regular container's 1)", request)
	}
	if allocated != "3" {
		t.Errorf("assign_node allocated_resource.cpu = %v, want 3 (the init container, larger than the regular container's 1)", allocated)
	}
}

// TestNew_RequiresAPlaceToWriteTheRecordInInternalMode covers New refusing a config
// that says nothing about where the transition record goes. Internal execution mode
// always writes one (units/U01.md), so the failure belongs at construction, with the
// option to set, rather than at the first write of the first run.
func TestNew_RequiresAPlaceToWriteTheRecordInInternalMode(t *testing.T) {
	cfg := WithConfig(testUserID, "", "", "", false, slog.LevelError)
	eval, err := New(
		cfg,
		WithMetricStores(),
		WithScenario("test", 1, func(chan<- definition.Event) {}, nil, testIdentity()),
	)
	if err == nil {
		t.Fatalf("evaluator.New returned %v, want an error when neither the record output dir nor a writer factory is given", eval)
	}
	if !strings.Contains(err.Error(), "WithRecordOutputDir") {
		t.Errorf("evaluator.New error = %q, want it to name the option to set", err)
	}
}

// TestRun_RecordsThePodsPriorityAndNodeSelector covers the rest of the task's immutable
// state (units/U01.md): the Pod's priority, and its node selector as the task's
// placement_constraint (research.md section 10.2).
func TestRun_RecordsThePodsPriorityAndNodeSelector(t *testing.T) {
	lines := runEvaluator(t, func(ch chan<- definition.Event) {
		rs := testReplicaSet("constrained", "1", 7, 10*time.Second)
		// testNode labels its nodes zone=a, so the Pod still has somewhere to go.
		rs.Spec.Template.Spec.NodeSelector = map[string]string{"zone": "a"}
		ch <- definition.NewEvent(definition.EventTypeCreate, testNode("4", "8Gi"), 0)
		ch <- definition.NewEvent(definition.EventTypeCreate, rs, 0)
	})

	var imm map[string]any
	for _, r := range findTransitions(lines, "create_task", "") {
		if m, ok := r["task_immutable"].(map[string]any); ok {
			imm = m
			break
		}
	}
	if imm == nil {
		t.Fatalf("no create_task record carries task_immutable: %v", lines)
	}
	if got := mustFloat(t, imm["priority"], "task_immutable.priority"); got != 7 {
		t.Errorf("task_immutable.priority = %v, want 7", imm["priority"])
	}
	constraint, ok := imm["placement_constraint"].(map[string]any)
	if !ok {
		t.Fatalf("task_immutable.placement_constraint = %v, want a map", imm["placement_constraint"])
	}
	if constraint["zone"] != "a" {
		t.Errorf("task_immutable.placement_constraint.zone = %v, want a", constraint["zone"])
	}
}
