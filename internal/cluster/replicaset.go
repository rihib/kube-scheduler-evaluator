package cluster

import (
	"context"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (m *manager) syncReplicaSet(ctx context.Context) error {
	replicasets, err := m.client.AppsV1().ReplicaSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	pods, err := m.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, rs := range replicasets.Items {
		var ownedPods []corev1.Pod
		for _, pod := range pods.Items {
			for _, owner := range pod.OwnerReferences {
				if owner.UID == rs.UID {
					ownedPods = append(ownedPods, pod)
				}
			}
		}
		diff := int(*rs.Spec.Replicas) - len(ownedPods)
		if diff > 0 {
			for range diff {
				tmpl := rs.Spec.Template.DeepCopy()
				newPod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						GenerateName: rs.Name + "-",
						Namespace:    rs.Namespace,
						Labels:       tmpl.Labels,
						Annotations:  tmpl.Annotations,
					},
					Spec: tmpl.Spec,
				}
				newPod.OwnerReferences = []metav1.OwnerReference{
					*metav1.NewControllerRef(&rs, rs.GroupVersionKind()),
				}
				created, err := m.client.CoreV1().Pods(rs.Namespace).Create(ctx, newPod, metav1.CreateOptions{})
				if err != nil {
					return err
				}
				slog.DebugContext(ctx, "create missing replicaset child pod", "pod", created.Name)
			}
		}
		if diff < 0 {
			for i := 0; i < -diff && i < len(ownedPods); i++ {
				err := m.client.CoreV1().Pods(ownedPods[i].Namespace).Delete(ctx, ownedPods[i].Name, metav1.DeleteOptions{})
				if err != nil {
					return err
				}
				slog.DebugContext(ctx, "delete excess replicaset child pod", "pod", ownedPods[i].Name)
			}
		}
	}
	return nil
}
