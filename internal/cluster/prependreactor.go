package cluster

import (
	"encoding/json"
	"fmt"
	"log/slog"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pfnet/kube-scheduler-evaluator/internal/cluster/pod"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/uuid"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func prependDebugReactor(client *fakeclientset.Clientset) {
	client.PrependReactor("*", "*", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		slog.Debug("API action detected",
			"verb", action.GetVerb(),
			"resource", action.GetResource().Resource,
			"subresource", action.GetSubresource(),
			"namespace", action.GetNamespace(),
		)
		return false, nil, nil
	})
}

func prependObjectCreationReactor(client *fakeclientset.Clientset, resource string) {
	// This reactor is called in main goroutine when client.Create is called
	client.Fake.PrependReactor("create", resource, func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		createAction, ok := action.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		obj := createAction.GetObject()
		accessor, err := meta.Accessor(obj)
		if err != nil {
			return false, nil, nil
		}
		// GenerateName
		if accessor.GetName() == "" && accessor.GetGenerateName() != "" {
			accessor.SetName(accessor.GetGenerateName() + rand.String(5))
		}
		// UID
		if accessor.GetUID() == "" {
			accessor.SetUID(types.UID(uuid.NewUUID()))
		}
		// Pod status and timestamps
		if pod, ok := obj.(*corev1.Pod); ok {
			if pod.Status.Phase == "" {
				pod.Status.Phase = corev1.PodPending
			}
			if pod.CreationTimestamp.IsZero() {
				pod.CreationTimestamp = metav1.Now()
			}
		}
		return false, nil, nil
	})
}

// prependPodRecordReactor records the creation and the deletion of Pods in recorder.
// It lets the default reactor of the fake client store the change, and records it only if the change succeeds.
// It must run after the creation reactor for "pods", which sets the name, the UID and the status of a new Pod.
func prependPodRecordReactor(client *fakeclientset.Clientset, recorder *pod.Recorder) {
	objectReaction := k8stesting.ObjectReaction(client.Tracker())
	gvr := corev1.SchemeGroupVersion.WithResource("pods")
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		if action.GetSubresource() != "" {
			return false, nil, nil
		}
		handled, ret, err = objectReaction(action)
		if err != nil {
			return handled, ret, err
		}
		if created, ok := ret.(*corev1.Pod); ok {
			recorder.PodCreated(created)
		}
		return handled, ret, err
	})
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		if action.GetSubresource() != "" {
			return false, nil, nil
		}
		deleteAction, ok := action.(k8stesting.DeleteAction)
		if !ok {
			return false, nil, nil
		}
		obj, getErr := client.Tracker().Get(gvr, deleteAction.GetNamespace(), deleteAction.GetName())
		handled, ret, err = objectReaction(action)
		if err != nil || getErr != nil {
			return handled, ret, err
		}
		if deleted, ok := obj.(*corev1.Pod); ok {
			recorder.PodDeleted(deleted)
		}
		return handled, ret, err
	})
}

func prependPodBindingReactor(client *fakeclientset.Clientset, recorder *pod.Recorder) {
	// This reactor is called in scheduler goroutine when scheduler tries to bind a Pod
	client.PrependReactor("create", "pods/binding", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		createAction, ok := action.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		binding, ok := createAction.GetObject().(*corev1.Binding)
		if !ok {
			return false, nil, nil
		}
		gvr := corev1.SchemeGroupVersion.WithResource("pods")
		obj, err := client.Tracker().Get(gvr, binding.Namespace, binding.Name)
		if err != nil {
			return true, nil, err
		}
		// Update Pod's spec.nodeName and status to simulate binding
		pod := obj.(*corev1.Pod).DeepCopy()
		pod.Spec.NodeName = binding.Target.Name
		pod.Status.Phase = corev1.PodRunning

		// Remove existing PodScheduled condition
		var conds []corev1.PodCondition
		for _, c := range pod.Status.Conditions {
			if c.Type == corev1.PodScheduled {
				continue
			}
			conds = append(conds, c)
		}
		pod.Status.Conditions = conds
		// Add PodScheduled=True condition
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               corev1.PodScheduled,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
		})
		// Record scheduledAt before storing the bound Pod,
		// so that scheduledAt is set whenever the informer cache shows the Pod as Running.
		// The virtual clock does not advance until WaitForScheduler sees the binding in the scheduler cache,
		// so the virtual time here is that of the event which made the Pod schedulable.
		recorder.PodScheduled(pod)
		if err = client.Tracker().Update(gvr, pod, binding.Namespace); err != nil {
			return true, nil, err
		}
		slog.Debug("pod binding succeeded", "pod", binding.Name, "node", binding.Target.Name)
		return true, nil, nil
	})
}

func prependPodPatchReactor(client *fakeclientset.Clientset) {
	// This reactor is called in scheduler goroutine when scheduler patches a Pod (e.g., marking Unschedulable)
	reactorPatch := func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		pa, ok := action.(k8stesting.PatchAction)
		if !ok {
			return false, nil, nil
		}
		gvr := corev1.SchemeGroupVersion.WithResource("pods")
		obj, getErr := client.Tracker().Get(gvr, pa.GetNamespace(), pa.GetName())
		if getErr != nil {
			return true, nil, getErr
		}
		// Apply the patch since fake client does not apply it automatically
		patchedObj, applyErr := applyPodPatch(obj, pa.GetPatchType(), pa.GetPatch())
		if applyErr != nil {
			return true, nil, applyErr
		}
		slog.Debug("pod patched", "pod", pa.GetName())
		pod := patchedObj
		if err := client.Tracker().Update(gvr, pod, pa.GetNamespace()); err != nil {
			return true, nil, err
		}
		return true, pod, nil
	}
	client.PrependReactor("patch", "pods", reactorPatch)
	client.PrependReactor("patch", "pods/status", reactorPatch)
}

func applyPodPatch(oldObj runtime.Object, pt types.PatchType, patch []byte) (*corev1.Pod, error) {
	oldPod, ok := oldObj.(*corev1.Pod)
	if !ok {
		return nil, fmt.Errorf("unexpected object type after patch")
	}
	oldBytes, err := json.Marshal(oldPod)
	if err != nil {
		return nil, err
	}
	var patchedBytes []byte
	switch pt {
	case types.MergePatchType:
		patchedBytes, err = jsonpatch.MergePatch(oldBytes, patch)
	case types.StrategicMergePatchType:
		patchedBytes, err = strategicpatch.StrategicMergePatch(oldBytes, patch, &corev1.Pod{})
	case types.JSONPatchType:
		var jp jsonpatch.Patch
		jp, err = jsonpatch.DecodePatch(patch)
		if err == nil {
			patchedBytes, err = jp.Apply(oldBytes)
		}
	default:
		return nil, fmt.Errorf("unsupported patch type %s", pt)
	}
	if err != nil {
		return nil, err
	}
	var pod corev1.Pod
	if err := json.Unmarshal(patchedBytes, &pod); err != nil {
		return nil, err
	}
	return &pod, nil
}
