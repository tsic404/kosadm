package harness

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// nativeWorkloads resolves the native kinds an application renders. They are
// not kernel kinds, so discovery does not list them under the kernel group.
var nativeWorkloads = map[string]schema.GroupVersionResource{
	"Deployment":  {Group: "apps", Version: "v1", Resource: "deployments"},
	"StatefulSet": {Group: "apps", Version: "v1", Resource: "statefulsets"},
	"ReplicaSet":  {Group: "apps", Version: "v1", Resource: "replicasets"},
	"DaemonSet":   {Group: "apps", Version: "v1", Resource: "daemonsets"},
}

// Workload reads one native workload an application rendered.
func (c *Cluster) Workload(t *testing.T, kind, namespace, name string) *unstructured.Unstructured {
	t.Helper()
	resource, ok := nativeWorkloads[kind]
	if !ok {
		t.Fatalf("no native workload resource for kind %q", kind)
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	object, err := c.Dynamic.Resource(resource).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s %s/%s: %v", kind, namespace, name, err)
	}
	return object
}

// WorkloadExists reports whether a rendered native workload exists.
func (c *Cluster) WorkloadExists(kind, namespace, name string) bool {
	resource, ok := nativeWorkloads[kind]
	if !ok {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	_, err := c.Dynamic.Resource(resource).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	return err == nil
}

// WorkloadReplicas reads the replica count the workload carries.
func (c *Cluster) WorkloadReplicas(t *testing.T, kind, namespace, name string) int64 {
	t.Helper()
	replicas, found, err := unstructured.NestedInt64(c.Workload(t, kind, namespace, name).Object, "spec", "replicas")
	if err != nil || !found {
		t.Fatalf("%s %s/%s carries no spec.replicas", kind, namespace, name)
	}
	return replicas
}

// SetWorkloadStatus writes the observed replica counts, so application health
// has an observed workload to read (C-08).
func (c *Cluster) SetWorkloadStatus(t *testing.T, kind, namespace, name string, desired, ready int64) {
	t.Helper()
	resource, ok := nativeWorkloads[kind]
	if !ok {
		t.Fatalf("no native workload resource for kind %q", kind)
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	client := c.Dynamic.Resource(resource).Namespace(namespace)
	object, err := client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s %s/%s: %v", kind, namespace, name, err)
	}
	object.Object["status"] = map[string]any{
		"replicas":           desired,
		"readyReplicas":      ready,
		"observedGeneration": object.GetGeneration(),
	}
	if _, err := client.UpdateStatus(ctx, object, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("write status of %s %s/%s: %v", kind, namespace, name, err)
	}
}
