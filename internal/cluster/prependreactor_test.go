package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/consts"
	"github.com/pfnet/kube-scheduler-evaluator/internal/metric"
	"github.com/pfnet/kube-scheduler-evaluator/internal/record"
	"github.com/pfnet/kube-scheduler-evaluator/pkg/definition"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// The tests below pin the fake API server's resourceVersion down to the one property
// client-go's shared informer reads it for: an update whose resourceVersion is
// unchanged is taken to be a resync, and a resync notification is delivered only to the
// event handlers that the informer's last resync check happened to mark as resyncing
// (tools/cache's sharedIndexInformer.OnUpdate and sharedProcessor.distribute).
//
// The scheduler registers its handlers after the informer factory has been started, so
// its handlers' first resync deadline falls after the informer's first resync tick, and
// that tick marks them as not resyncing for the whole resync period that follows. While
// a fake API server leaves every resourceVersion empty, every Pod update in that window
// is silently dropped for the scheduler: the binding update that sets spec.nodeName
// never arrives as the "became assigned" transition that calls Cache.AddPod, so the Pod
// stays assumed in the scheduler cache forever and WaitForScheduler never sees the
// scheduler stabilize. Found at openb scale (~5800 Pods), where a run lasts longer than
// one resync period; runs that finish within the first resync period never hit it.

// newTestFakeAPIServer returns the fake API server of one run, with the same reactors
// the evaluator registers on it in internal execution mode.
func newTestFakeAPIServer(t *testing.T) kubernetes.Interface {
	t.Helper()
	c, err := consts.NewConsts(
		"tester",
		[]definition.Scenario{definition.NewScenario("test", 1, func(chan<- definition.Event) {}, nil, definition.RunIdentity{})},
		time.Now(),
		nil,
	)
	if err != nil {
		t.Fatalf("consts.NewConsts: %v", err)
	}
	metricManager, err := metric.NewManager(c)
	if err != nil {
		t.Fatalf("metric.NewManager: %v", err)
	}
	metricManager.SetScenarioID("scenario-test")
	mng, err := NewManager(
		c, metricManager,
		func(record.RunInfo) (record.Writer, error) { return discardRecordWriter{}, nil },
		func() (string, error) { return "evaluator-commit", nil },
		func() (string, error) { return "hardware", nil },
	)
	if err != nil {
		t.Fatalf("cluster.NewManager: %v", err)
	}
	m, ok := mng.(*manager)
	if !ok {
		t.Fatalf("cluster.NewManager returned %T, want *manager", mng)
	}
	if err := m.recordManager.BeginRun(context.Background(), record.RunInfo{EvaluationID: "evaluation", ScenarioID: "scenario-test"}); err != nil {
		t.Fatalf("record.BeginRun: %v", err)
	}
	t.Cleanup(func() {
		if err := m.recordManager.EndRun(); err != nil {
			t.Errorf("record.EndRun: %v", err)
		}
	})
	m.resetFakeCluster()
	return m.client
}

type discardRecordWriter struct{}

func (discardRecordWriter) WriteLine(any) error { return nil }
func (discardRecordWriter) Close() error        { return nil }

// testPod is a Pod the scheduler would be responsible for, with the annotation the
// Pod recorder needs to record its creation.
func testPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   "default",
			Annotations: map[string]string{definition.ExecutionDurationAnnotationKey("tester"): "1s"},
		},
		Spec: corev1.PodSpec{
			SchedulerName: "default-scheduler",
			Containers:    []corev1.Container{{Name: "c", Image: "example.com/image:1.0"}},
		},
	}
}

func TestFakeAPIServer_creatingAPodStampsAResourceVersion(t *testing.T) {
	client := newTestFakeAPIServer(t)

	created, err := client.CoreV1().Pods("default").Create(context.Background(), testPod("pod-1"), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create the pod: %v", err)
	}

	if created.ResourceVersion == "" {
		t.Errorf("the created pod has no resourceVersion")
	}
}

func TestFakeAPIServer_bindingAPodChangesItsResourceVersion(t *testing.T) {
	client := newTestFakeAPIServer(t)
	ctx := context.Background()
	created, err := client.CoreV1().Pods("default").Create(ctx, testPod("pod-1"), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create the pod: %v", err)
	}

	if err := client.CoreV1().Pods("default").Bind(ctx, &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Name: created.Name, Namespace: created.Namespace},
		Target:     corev1.ObjectReference{Kind: "Node", Name: "node-1"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to bind the pod: %v", err)
	}

	bound, err := client.CoreV1().Pods("default").Get(ctx, created.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to read the bound pod back: %v", err)
	}
	if bound.Spec.NodeName != "node-1" {
		t.Fatalf("the pod was not bound: nodeName is %q", bound.Spec.NodeName)
	}
	if bound.ResourceVersion == created.ResourceVersion {
		t.Errorf("binding left the pod's resourceVersion at %q", bound.ResourceVersion)
	}
}

func TestFakeAPIServer_patchingAPodChangesItsResourceVersion(t *testing.T) {
	client := newTestFakeAPIServer(t)
	ctx := context.Background()
	created, err := client.CoreV1().Pods("default").Create(ctx, testPod("pod-1"), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create the pod: %v", err)
	}

	patched, err := client.CoreV1().Pods("default").Patch(
		ctx, created.Name, types.StrategicMergePatchType,
		[]byte(`{"status":{"conditions":[{"type":"PodScheduled","status":"False","reason":"Unschedulable"}]}}`),
		metav1.PatchOptions{}, "status",
	)
	if err != nil {
		t.Fatalf("failed to patch the pod: %v", err)
	}

	if len(patched.Status.Conditions) == 0 {
		t.Fatalf("the patch was not applied: the pod has no condition")
	}
	if patched.ResourceVersion == created.ResourceVersion {
		t.Errorf("the patch left the pod's resourceVersion at %q", patched.ResourceVersion)
	}
}
