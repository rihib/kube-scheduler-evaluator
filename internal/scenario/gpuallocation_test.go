package scenario

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/clock"
	"github.com/pfnet/kube-scheduler-evaluator/internal/cluster"
	"github.com/pfnet/kube-scheduler-evaluator/internal/consts"
	"github.com/pfnet/kube-scheduler-evaluator/internal/event"
	"github.com/pfnet/kube-scheduler-evaluator/internal/metric"
	metricpoint "github.com/pfnet/kube-scheduler-evaluator/internal/metric/point"
	"github.com/pfnet/kube-scheduler-evaluator/pkg/definition"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEventRequestsGPUFromReplicaSetTemplate(t *testing.T) {
	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-job", Namespace: "default"},
		Spec: appsv1.ReplicaSetSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name: "job",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								gpuResourceName: resource.MustParse("1"),
							},
						},
					}},
				},
			},
		},
	}
	replicaSet.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))
	scenarioEvent, err := event.NewEvent(
		definition.EventTypeCreate,
		replicaSet,
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}

	got, err := eventRequestsGPU(scenarioEvent)
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Fatal("ReplicaSet Pod template GPU request was not detected")
	}
}

func TestManagerRecordsGPUAllocationForReplicaSet(t *testing.T) {
	const userID = "example.com"
	startedAt := time.Now()
	scenarioDefinition := definition.NewScenario("gpu", 1, func(ch chan<- definition.Event) {
		ch <- definition.NewEvent(definition.EventTypeCreate, gpuNode(), time.Second)
		ch <- definition.NewEvent(definition.EventTypeCreate, gpuReplicaSet(userID), time.Second)
	}, nil)
	c, err := consts.NewConsts(userID, []definition.Scenario{scenarioDefinition}, startedAt, nil)
	if err != nil {
		t.Fatal(err)
	}
	metrics := &recordingMetricManager{scenarioID: scenarioDefinition.ScenarioID()}
	var _ metric.Manager = metrics
	resources, err := cluster.NewManager(c, metrics)
	if err != nil {
		t.Fatal(err)
	}
	clock.SetVirtualTime(startedAt)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := NewManager(ctx, scenarioDefinition, c, resources, metrics).Run(ctx); err != nil {
		t.Fatal(err)
	}

	if !metrics.contains("gpu_allocation_percent") {
		t.Fatal("GPU allocation metric was not recorded")
	}
	if !metrics.contains("gpu_average_allocation_percent") {
		t.Fatal("average GPU allocation metric was not recorded")
	}
}

func gpuNode() *corev1.Node {
	gpus := resource.MustParse("8")
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-node"},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("32"),
				corev1.ResourceMemory: resource.MustParse("256Gi"),
				corev1.ResourcePods:   resource.MustParse("110"),
				gpuResourceName:       gpus,
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("32"),
				corev1.ResourceMemory: resource.MustParse("256Gi"),
				corev1.ResourcePods:   resource.MustParse("110"),
				gpuResourceName:       gpus,
			},
		},
	}
	node.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Node"))
	return node
}

func gpuReplicaSet(userID string) *appsv1.ReplicaSet {
	replicas := int32(1)
	labels := map[string]string{"app": "gpu-job"}
	replicaSet := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gpu-job",
			Namespace: "default",
			Annotations: map[string]string{
				definition.DeadlineDurationAnnotationKey(userID): (2 * time.Minute).String(),
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						definition.ExecutionDurationAnnotationKey(userID): time.Minute.String(),
					},
				},
				Spec: corev1.PodSpec{
					SchedulerName: "default-scheduler",
					Containers: []corev1.Container{{
						Name:  "job",
						Image: "registry.example.com/job:1",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU: resource.MustParse("1"),
								gpuResourceName:    resource.MustParse("1"),
							},
						},
					}},
				},
			},
		},
	}
	replicaSet.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))
	return replicaSet
}

type recordingMetricManager struct {
	mu         sync.Mutex
	scenarioID string
	iteration  int
	lines      []string
}

func (m *recordingMetricManager) ScenarioID() string {
	return m.scenarioID
}

func (m *recordingMetricManager) SetScenarioID(id string) {
	m.scenarioID = id
}

func (m *recordingMetricManager) Iteration() int {
	return m.iteration
}

func (m *recordingMetricManager) SetIteration(iteration int) {
	m.iteration = iteration
}

func (m *recordingMetricManager) Start(context.Context) {}
func (m *recordingMetricManager) Close()                {}
func (m *recordingMetricManager) Wait()                 {}

func (m *recordingMetricManager) Record(point metricpoint.MetricPoint) {
	line, err := point.ToJSONLine()
	if err != nil {
		panic(err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lines = append(m.lines, line)
}

func (m *recordingMetricManager) contains(metricName string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, line := range m.lines {
		var data struct {
			Metric map[string]string `json:"metric"`
		}
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			continue
		}
		if strings.EqualFold(data.Metric["__name__"], metricName) {
			return true
		}
	}
	return false
}
