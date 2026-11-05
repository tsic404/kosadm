package install

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/api/v1beta1"
)

// TestStatusReportsTheInstallationAndItsComponents pins the read-only status
// report: the namespace, the control-plane rollout, the kernel definitions and
// each component's Ready condition.
func TestStatusReportsTheInstallationAndItsComponents(t *testing.T) {
	replicas := int32(1)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: ControlPlaneName, Namespace: DefaultNamespace},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1},
	}
	kube := kubernetesfake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: DefaultNamespace}},
		deployment,
	)
	extensions := apiextensionsfake.NewSimpleClientset(
		&apiextensionsv1.CustomResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: "applications." + api.Group},
			Spec:       apiextensionsv1.CustomResourceDefinitionSpec{Group: api.Group},
		},
		&apiextensionsv1.CustomResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: "widgets.example.test"},
			Spec:       apiextensionsv1.CustomResourceDefinitionSpec{Group: "widgets.example.test"},
		},
	)
	resource, ok := api.ResourceName("SystemComponent")
	if !ok {
		t.Fatal("the kernel model names no SystemComponent resource")
	}
	component := func(name string, conditions ...map[string]any) *unstructured.Unstructured {
		entries := make([]any, 0, len(conditions))
		for _, condition := range conditions {
			entries = append(entries, condition)
		}
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": api.Group + "/" + v1beta1.Version,
			"kind":       "SystemComponent",
			"metadata":   map[string]any{"name": name},
			"status":     map[string]any{"conditions": entries},
		}}
	}
	dynamic := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			{Group: api.Group, Version: v1beta1.Version, Resource: resource}: "SystemComponentList",
		},
		component("demo-widgets", map[string]any{
			"type": "Ready", "status": "False", "reason": "DependencyMissing",
		}),
		component("demo-pages", map[string]any{
			"type": "Ready", "status": "True",
		}),
	)

	reader, err := NewStatusReader(kube, extensions, dynamic)
	if err != nil {
		t.Fatalf("build status reader: %v", err)
	}
	status, err := reader.Read(context.Background())
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	if !status.Installed || status.Namespace != DefaultNamespace {
		t.Fatalf("installation = %q installed=%t, want %s installed", status.Namespace, status.Installed, DefaultNamespace)
	}
	if !status.ControlPlane.Present || status.ControlPlane.Desired != 1 || status.ControlPlane.Ready != 1 {
		t.Fatalf("control plane = %+v, want one of one ready", status.ControlPlane)
	}
	if len(status.Definitions) != 1 || status.Definitions[0] != "applications."+api.Group {
		t.Fatalf("definitions = %v, want only the kernel definitions", status.Definitions)
	}
	if len(status.Components) != 2 {
		t.Fatalf("components = %+v, want two", status.Components)
	}
	if status.Components[0].Name != "demo-pages" || !status.Components[0].Ready {
		t.Fatalf("component = %+v, want the ready one first", status.Components[0])
	}
	if status.Components[1].Name != "demo-widgets" || status.Components[1].Ready ||
		status.Components[1].Reason != "DependencyMissing" {
		t.Fatalf("component = %+v, want the blocked one with its reason", status.Components[1])
	}
}

// TestStatusReportsAMissingInstallation covers a cluster the installation has
// not reached: the status read reports it instead of failing.
func TestStatusReportsAMissingInstallation(t *testing.T) {
	resource, ok := api.ResourceName("SystemComponent")
	if !ok {
		t.Fatal("the kernel model names no SystemComponent resource")
	}
	lists := map[schema.GroupVersionResource]string{
		{Group: api.Group, Version: v1beta1.Version, Resource: resource}: "SystemComponentList",
	}
	reader, err := NewStatusReader(kubernetesfake.NewSimpleClientset(), apiextensionsfake.NewSimpleClientset(),
		dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists))
	if err != nil {
		t.Fatalf("build status reader: %v", err)
	}
	status, err := reader.Read(context.Background())
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status.Installed || status.ControlPlane.Present {
		t.Fatalf("status = %+v, want a missing installation", status)
	}
}
