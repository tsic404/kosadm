package harness

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/tsic404/kosd/pkg/controlplane"
)

// DefaultKubeletVersion is the version a case's node reports before a cluster
// upgrade moves it.
const DefaultKubeletVersion = "v1.37.0"

// CreateNode writes one ready node that reports version, which is the fact the
// upgrade preflight reads and the maintenance flow ends on (C-55, C-65).
func (c *Cluster) CreateNode(t *testing.T, name, version string) {
	t.Helper()
	c.CreateClusterObject(t, "", "v1", "nodes", "", map[string]any{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata":   map[string]any{"name": name},
	})
	c.SetKubeletVersion(t, name, version)
}

// SetKubeletVersion writes the version a node reports; a node ends its
// maintenance when it reports the upgrade's target version (C-55).
func (c *Cluster) SetKubeletVersion(t *testing.T, name, version string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	nodes := c.Direct(t, "", "v1", "Node", "")
	node, err := nodes.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read node %q: %v", name, err)
	}
	node.Object["status"] = map[string]any{
		"conditions":  []any{map[string]any{"type": "Ready", "status": "True"}},
		"allocatable": map[string]any{"cpu": "4", "memory": "8Gi"},
		"nodeInfo":    map[string]any{"kubeletVersion": version},
	}
	if _, err := nodes.UpdateStatus(ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("write the status of node %q: %v", name, err)
	}
}

// NodeUnschedulable reports whether one node is out of scheduling, which is the
// state the maintenance flow moves (C-65).
func (c *Cluster) NodeUnschedulable(t *testing.T, name string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	node, err := c.Direct(t, "", "v1", "Node", "").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read node %q: %v", name, err)
	}
	cordoned, _, _ := unstructured.NestedBool(node.Object, "spec", "unschedulable")
	return cordoned
}

// CreatePod writes one pod scheduled on node. A pod that names a workload is
// owned by that workload's ReplicaSet, so a drain resolves the application the
// workload belongs to; an empty workload leaves the pod unowned.
func (c *Cluster) CreatePod(t *testing.T, namespace, name, node, workload string) string {
	t.Helper()
	metadata := map[string]any{"name": name, "namespace": namespace}
	if workload != "" {
		metadata["ownerReferences"] = []any{map[string]any{
			"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": workload + "-abc",
			"uid": "uid-" + workload, "controller": true,
		}}
		c.CreateReplicaSet(t, namespace, workload)
	}
	c.CreateClusterObject(t, "", "v1", "pods", namespace, map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   metadata,
		"spec": map[string]any{
			"nodeName":                      node,
			"terminationGracePeriodSeconds": int64(0),
			"containers":                    []any{map[string]any{"name": "app", "image": "nginx:1.27"}},
		},
	})
	return name
}

// CreateReplicaSet writes the ReplicaSet of one workload, owned by the workload,
// so the owner chain a drain resolves ends at the application's workload.
func (c *Cluster) CreateReplicaSet(t *testing.T, namespace, workload string) {
	t.Helper()
	c.CreateClusterObject(t, "apps", "v1", "replicasets", namespace, map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "ReplicaSet",
		"metadata": map[string]any{
			"name":      workload + "-abc",
			"namespace": namespace,
			"ownerReferences": []any{map[string]any{
				"apiVersion": "apps/v1", "kind": "Deployment", "name": workload,
				"uid": "uid-" + workload, "controller": true,
			}},
		},
		"spec": map[string]any{
			"replicas": int64(2),
			"selector": map[string]any{"matchLabels": map[string]any{"app": workload}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": workload}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "app", "image": "nginx:1.27"}}},
			},
		},
	})
}

// PodExists reports whether the cluster still holds one pod.
func (c *Cluster) PodExists(t *testing.T, namespace, name string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	_, err := c.Direct(t, "", "v1", "Pod", namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("read pod %s/%s: %v", namespace, name, err)
	}
	return err == nil
}

// RetainedPods reads the pods one drain left on the node.
func RetainedPods(drain controlplane.MaintenanceDrain) map[string]bool {
	retained := make(map[string]bool, len(drain.Retained))
	for _, entry := range drain.Retained {
		retained[entry.Pod] = true
	}
	return retained
}
