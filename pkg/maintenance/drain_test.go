package maintenance

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/tsic404/kosd/pkg/controlplane"
)

// TestReplicasReadsTheKernelsDefault covers C-65's stop gate: a workload that
// names no replica count runs one replica, so the drain must not read the
// absence as a stop.
func TestReplicasReadsTheKernelsDefault(t *testing.T) {
	cases := []struct {
		name   string
		object map[string]any
		want   int64
	}{
		{"no spec.replicas", map[string]any{"spec": map[string]any{}}, 1},
		{"stopped", map[string]any{"spec": map[string]any{"replicas": int64(0)}}, 0},
		{"running", map[string]any{"spec": map[string]any{"replicas": int64(3)}}, 3},
		{"unreadable", map[string]any{"spec": map[string]any{"replicas": "three"}}, 1},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := replicas(testCase.object); got != testCase.want {
				t.Fatalf("replicas() = %d, want %d", got, testCase.want)
			}
		})
	}
}

// TestRetainedKeepsWhatItCannotJudge covers the drain's protected set: a
// node-local application's workload and a daemon set stay, and a pod whose owner
// the kernel no longer holds stays too, because nothing proves it is movable
// (C-65).
func TestRetainedKeepsWhatItCannotJudge(t *testing.T) {
	protected := map[string]bool{workloadKey("Deployment", "apps", "shop"): true}
	cases := []struct {
		name string
		pod  Pod
		keep bool
	}{
		{"node-local workload", Pod{Namespace: "apps", Name: "shop-1", WorkloadKind: "Deployment", WorkloadName: "shop"}, true},
		{"daemon set", Pod{Namespace: "apps", Name: "agent-1", WorkloadKind: "DaemonSet", WorkloadName: "agent"}, true},
		{"unresolved owner", Pod{Namespace: "apps", Name: "shop-2", WorkloadKind: "ReplicaSet", WorkloadName: "shop-abc", IsUnresolved: true}, true},
		{"movable workload", Pod{Namespace: "apps", Name: "cache-1", WorkloadKind: "Deployment", WorkloadName: "cache"}, false},
		{"bare pod", Pod{Namespace: "apps", Name: "batch-1"}, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			reason, keep := retained(testCase.pod, protected)
			if keep != testCase.keep {
				t.Fatalf("retained() = %v (%q), want %v", keep, reason, testCase.keep)
			}
			if keep && reason == "" {
				t.Fatal("a retained pod carries no reason")
			}
		})
	}
}

// TestPodsOnFailsWhenTheOwnerCannotBeRead covers the read path a drain reasons
// from: a pod whose ReplicaSet the kernel cannot answer for fails the list,
// because the alternative is to guess an owner and hand the pod to the eviction
// path (C-65).
func TestPodsOnFailsWhenTheOwnerCannotBeRead(t *testing.T) {
	client := fake.NewSimpleClientset(ownedPod("shop-1", "shop-abc"))
	client.PrependReactor("get", "replicasets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("the API server is unreachable")
	})

	if _, err := (Cluster{Kube: client}).PodsOn(context.Background(), "node-a"); err == nil {
		t.Fatal("a pod whose owner could not be read was listed anyway")
	} else if !strings.Contains(err.Error(), "shop-abc") {
		t.Fatalf("failure %v does not name the owner it could not read", err)
	}
}

// TestPodsOnMarksAPodWhoseOwnerIsGone covers the other side: an owner the kernel
// no longer holds leaves the pod unjudged, which the drain keeps on the node
// rather than evicting.
func TestPodsOnMarksAPodWhoseOwnerIsGone(t *testing.T) {
	client := fake.NewSimpleClientset(ownedPod("shop-1", "shop-abc"))
	client.PrependReactor("get", "replicasets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "replicasets"}, "shop-abc")
	})

	pods, err := (Cluster{Kube: client}).PodsOn(context.Background(), "node-a")
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods) != 1 || !pods[0].IsUnresolved {
		t.Fatalf("pods = %+v, want one unresolved pod", pods)
	}
}

// TestPodsOnResolvesTheDeploymentBehindAReplicaSet covers the owner chain: the
// workload a drain judges is the Deployment the ReplicaSet belongs to.
func TestPodsOnResolvesTheDeploymentBehindAReplicaSet(t *testing.T) {
	client := fake.NewSimpleClientset(ownedPod("shop-1", "shop-abc"), replicaSetObject("apps", "shop-abc", "shop"))

	pods, err := (Cluster{Kube: client}).PodsOn(context.Background(), "node-a")
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods) != 1 || pods[0].WorkloadKind != "Deployment" || pods[0].WorkloadName != "shop" {
		t.Fatalf("pods = %+v, want the pod resolved to Deployment/shop", pods)
	}
}

// replicaSetObject writes the ReplicaSet a Deployment owns.
func replicaSetObject(namespace, name, deployment string) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "Deployment", Name: deployment, Controller: new(true),
			}},
		},
	}
}

// ownedPod writes one pod a ReplicaSet owns.
func ownedPod(name, replicaSet string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "apps",
			Name:      name,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: replicaSet, Controller: new(true),
			}},
		},
		Spec: corev1.PodSpec{NodeName: "node-a"},
	}
}

// failingPods answers a fixed pod list and refuses every eviction.
type failingPods struct {
	pods []Pod
}

// PodsOn implements the pod source.
func (p failingPods) PodsOn(context.Context, string) ([]Pod, error) {
	return p.pods, nil
}

// Evict implements the pod source.
func (p failingPods) Evict(context.Context, string, string) error {
	return errors.New("the kernel refused the eviction")
}

// recordingAuditor keeps the records a flow writes.
type recordingAuditor struct {
	records []controlplane.AuditRecord
}

// Record implements the auditor.
func (a *recordingAuditor) Record(_ context.Context, record controlplane.AuditRecord) error {
	a.records = append(a.records, record)
	return nil
}

// TestDrainRecordsAFailedEviction covers C-14's rule over the drain: an
// eviction the kernel refuses is a write attempt, so it leaves a record even
// though the step did not complete.
func TestDrainRecordsAFailedEviction(t *testing.T) {
	node := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata":   map[string]any{"name": "node-a"},
		"spec":       map[string]any{"unschedulable": true},
	}}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			volumeResource:      "VolumeList",
			applicationResource: "ApplicationList",
		}, node)
	auditor := &recordingAuditor{}
	service := &Service{
		Dynamic: client,
		Pods:    failingPods{pods: []Pod{{Namespace: "apps", Name: "batch-1"}}},
		Auditor: auditor,
	}

	if _, failure := service.Drain(context.Background(), "node-a", "owner"); failure == nil {
		t.Fatal("a refused eviction was reported as a drained node")
	}
	if len(auditor.records) != 1 {
		t.Fatalf("records = %+v, want the failed drain", auditor.records)
	}
	record := auditor.records[0]
	if record.Verb != controlplane.VerbDrain || record.Result != controlplane.ResultFailed || record.Object.Name != "node-a" {
		t.Fatalf("record = %+v, want a failed drain of node-a", record)
	}
	if record.Actor != "owner" || record.Source != controlplane.SourceAPI {
		t.Fatalf("record actor/source = %q/%q, want the caller and the API", record.Actor, record.Source)
	}
}
