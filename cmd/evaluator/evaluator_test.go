package evaluator

import (
	"context"
	"encoding/json"
	"log/slog"
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

// fakeRecordWriter is the Fake writer used by the unit tests below (decisions.md:
// "記録の書き出し先...は、evaluator を作るときに外から渡せるようにする...テストで Fake を渡すため").
// It keeps the written lines as parsed JSON objects, in order, instead of writing a file.
type fakeRecordWriter struct {
	mu    sync.Mutex
	lines []map[string]any
}

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

// newFakeRecordSetup returns a config carrying Fakes for the record's writer,
// evaluator commit and hardware info (decisions.md), and the single writer the
// factory will hand back for the one run each test below performs.
func newFakeRecordSetup() (ConfigOption, ConfigOption, ConfigOption, *fakeRecordWriter) {
	w := &fakeRecordWriter{}
	writerOpt := WithRecordWriterFactory(func(RecordRunInfo) (RecordWriter, error) {
		return w, nil
	})
	commitOpt := WithCommitProvider(func() (string, error) {
		return "fake-evaluator-commit", nil
	})
	hwOpt := WithHardwareInfoProvider(func() (string, error) {
		return "fake-hardware", nil
	})
	return writerOpt, commitOpt, hwOpt, w
}

func testNode(cpu, mem string) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "node-",
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

// testReplicaSet builds a one-Pod ReplicaSet (one task, decisions.md), with the
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

func runEvaluator(t *testing.T, opts []ConfigOption, generator func(chan<- definition.Event)) []map[string]any {
	t.Helper()
	writerOpt, commitOpt, hwOpt, w := newFakeRecordSetup()
	allOpts := append([]ConfigOption{writerOpt, commitOpt, hwOpt}, opts...)
	cfg := WithConfig(testUserID, "", "", "", false, slog.LevelError, allOpts...)
	eval, err := New(
		cfg,
		WithMetricStores(),
		WithScenario("test", 1, generator, nil, testIdentity()),
	)
	if err != nil {
		t.Fatalf("evaluator.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if code := eval.Run(ctx); code != 0 {
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
	lines := runEvaluator(t, nil, func(ch chan<- definition.Event) {
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
		releaseRecords := findTransitions(lines, "release", instanceID)
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

		// complete must not revert assigned_node (research.md section 11.1.2): the
		// Pod has an assigned_node transition only from assign_node, not from complete.
		assignedNodeChanges := findTransitions(lines, "delete", instanceID)
		for _, r := range assignedNodeChanges {
			if r["state"] == "assigned_node" {
				t.Errorf("complete must not record an assigned_node change, but found a delete record: %v", r)
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
	})
}

// TestRun_NodeDeregistration covers a Node that the scenario removes: register_node at
// creation, deregister_node at deletion.
func TestRun_NodeDeregistration(t *testing.T) {
	node := testNode("4", "8Gi")
	node.Name = "deregistration-test-node"
	deleted := node.DeepCopy()

	lines := runEvaluator(t, nil, func(ch chan<- definition.Event) {
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
	lines := runEvaluator(t, nil, func(ch chan<- definition.Event) {
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
