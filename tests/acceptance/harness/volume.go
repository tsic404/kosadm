package harness

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/tsic404/kosd/pkg/markers"
	"github.com/tsic404/kosd/pkg/write"
)

// Volume returns a namespaced volume with the given spec and no status, so the
// case's volume reconciler is its only status writer.
func Volume(namespace, name string, spec map[string]any) *unstructured.Unstructured {
	manifest := Manifest("Volume", namespace, name, map[string]string{markers.ManagedBy: write.ManagedByValue}, nil)
	manifest["spec"] = spec
	return &unstructured.Unstructured{Object: manifest}
}

// VolumeNode reads where a volume's data sits (C-38).
func (c *Cluster) VolumeNode(t *testing.T, namespace, name string) string {
	t.Helper()
	volume := c.GetIn(t, namespace, "v1beta1", "Volume", name)
	node, _, _ := unstructured.NestedString(volume.Object, "status", "node")
	return node
}

// StorageClass writes a storage class. envtest runs no provisioner, so a case
// supplies the backend a volume resolves against (C-38).
func (c *Cluster) StorageClass(t *testing.T, name, provisioner string, allowExpansion bool, reclaimPolicy string, parameters map[string]string) {
	t.Helper()
	object := map[string]any{
		"apiVersion":    "storage.k8s.io/v1",
		"kind":          "StorageClass",
		"metadata":      map[string]any{"name": name},
		"provisioner":   provisioner,
		"reclaimPolicy": reclaimPolicy,
	}
	if allowExpansion {
		object["allowVolumeExpansion"] = true
	}
	if len(parameters) > 0 {
		values := make(map[string]any, len(parameters))
		for key, value := range parameters {
			values[key] = value
		}
		object["parameters"] = values
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	resource := c.ExtensionResource("storage.k8s.io", "v1", "storageclasses", "")
	if _, err := resource.Create(ctx, &unstructured.Unstructured{Object: object}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create storage class %s: %v", name, err)
	}
}

// CreateClaim creates one bound claim for a volume, through the write row the
// volume reconciler owns (C-04, C-38).
func (c *Cluster) CreateClaim(t *testing.T, namespace, name, storageClass string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	claim := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"labels": map[string]any{
				markers.ManagedBy: write.ManagedByValue,
				markers.OwnerUID:  "volume-uid",
			},
		},
		"spec": map[string]any{
			"accessModes":      []any{"ReadWriteOnce"},
			"storageClassName": storageClass,
			"resources":        map[string]any{"requests": map[string]any{"storage": "1Gi"}},
		},
	}}
	if _, err := c.Writer().CreateClaim(ctx, write.SubjectVolumeReconciler, write.Session{}, write.ClaimBound, claim); err != nil {
		t.Fatalf("create claim %s/%s: %v", namespace, name, err)
	}
}

// Claim reads one claim.
func (c *Cluster) Claim(t *testing.T, namespace, name string) *corev1.PersistentVolumeClaim {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	claim, err := c.Kube.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read claim %s/%s: %v", namespace, name, err)
	}
	return claim
}

// ClaimExists reports whether a claim is still there, so a case can observe the
// data staying in place.
func (c *Cluster) ClaimExists(t *testing.T, namespace, name string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	_, err := c.Direct(t, "", "v1", "PersistentVolumeClaim", namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("read claim %s/%s: %v", namespace, name, err)
	}
	return err == nil
}
