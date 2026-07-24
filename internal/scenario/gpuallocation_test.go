package scenario

import (
	"testing"
	"time"

	"github.com/pfnet/kube-scheduler-evaluator/internal/event"
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
