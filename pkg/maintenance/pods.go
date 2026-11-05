package maintenance

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// PodSource reads the pods the run kernel runs on one node and evicts them
// through the kernel's own eviction interface (C-65). A drain never deletes a
// pod directly: the kernel judges the disruption and its answer is final.
type PodSource interface {
	// PodsOn lists the pods the kernel scheduled to one node. A pod whose owner
	// cannot be read fails the list: an owner nobody resolved cannot be judged
	// node-local, and the drain must not evict on a guess (C-65).
	PodsOn(ctx context.Context, node string) ([]Pod, error)
	// Evict asks the kernel to evict one pod.
	Evict(ctx context.Context, namespace, name string) error
}

// Pod is one workload instance the run kernel runs on a node.
type Pod struct {
	// Namespace is where the pod lives.
	Namespace string
	// Name is the pod's name.
	Name string
	// WorkloadKind and WorkloadName name the object that created the pod; both
	// are empty for a pod nothing owns.
	WorkloadKind string
	WorkloadName string
	// IsUnresolved marks a pod whose owner the kernel no longer holds. The drain
	// cannot judge whether such a pod belongs to a node-local application, so it
	// leaves it on the node instead of guessing (C-65).
	IsUnresolved bool
}

// Cluster reads pods over the run kernel's own API.
type Cluster struct {
	// Kube is the run kernel client; required.
	Kube kubernetes.Interface
}

// PodsOn implements PodSource.
func (c Cluster) PodsOn(ctx context.Context, node string) ([]Pod, error) {
	pods, err := c.Kube.CoreV1().Pods("").List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + node})
	if err != nil {
		return nil, err
	}
	replicaSets := map[string]workload{}
	found := make([]Pod, 0, len(pods.Items))
	for index := range pods.Items {
		pod := &pods.Items[index]
		if pod.DeletionTimestamp != nil {
			continue
		}
		owner, err := workloadOf(ctx, c.Kube, replicaSets, pod)
		if err != nil {
			return nil, err
		}
		found = append(found, Pod{
			Namespace:    pod.Namespace,
			Name:         pod.Name,
			WorkloadKind: owner.kind,
			WorkloadName: owner.name,
			IsUnresolved: owner.unresolved,
		})
	}
	return found, nil
}

// Evict implements PodSource.
func (c Cluster) Evict(ctx context.Context, namespace, name string) error {
	return c.Kube.CoreV1().Pods(namespace).EvictV1(ctx, &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
	})
}

// workload names the object a pod belongs to.
type workload struct {
	kind string
	name string
	// unresolved marks a pod whose owner chain the kernel cannot complete.
	unresolved bool
}

// workloadOf resolves the workload a pod belongs to. A pod a Deployment created
// is owned by that Deployment's ReplicaSet, so the answer a drain reasons about
// is the ReplicaSet's own owner; a pod something else created names its owner
// directly, and a bare pod names nothing.
//
// A ReplicaSet the kernel cannot answer for fails the resolution: without its
// owner the pod cannot be judged node-local, and guessing would hand it to the
// eviction path (C-65).
func workloadOf(ctx context.Context, kube kubernetes.Interface, known map[string]workload, pod *corev1.Pod) (workload, error) {
	for _, owner := range pod.OwnerReferences {
		if owner.Kind != "ReplicaSet" {
			return workload{kind: owner.Kind, name: owner.Name}, nil
		}
		key := pod.Namespace + "/" + owner.Name
		if resolved, cached := known[key]; cached {
			return resolved, nil
		}
		replicaSet, err := kube.AppsV1().ReplicaSets(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return workload{kind: owner.Kind, name: owner.Name, unresolved: true}, nil
		}
		if err != nil {
			return workload{}, fmt.Errorf("read ReplicaSet %s for pod %s/%s: %w", owner.Name, pod.Namespace, pod.Name, err)
		}
		resolved := workload{kind: owner.Kind, name: owner.Name}
		for _, parent := range replicaSet.OwnerReferences {
			resolved = workload{kind: parent.Kind, name: parent.Name}
			break
		}
		known[key] = resolved
		return resolved, nil
	}
	return workload{}, nil
}
