package install

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/pkg/extension"
	"github.com/tsic404/kosd/pkg/markers"
)

// componentClient serves one attributed SystemComponent naming reference.
func componentClient(t *testing.T, name, reference string) *dynamicfake.FakeDynamicClient {
	t.Helper()
	resource := schema.GroupVersionResource{Group: api.Group, Version: "v1beta1", Resource: "systemcomponents"}
	component := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": api.Group + "/v1beta1",
		"kind":       "SystemComponent",
		"metadata": map[string]any{
			"name":   name,
			"labels": map[string]any{markers.ManagedBy: "kosd"},
		},
		"spec": map[string]any{
			"package": map[string]any{"reference": reference},
		},
	}}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{resource: "SystemComponentList"},
		component,
	)
}

// TestComponentClassesReadsPackedArtifacts pins C-15 and C-17: the classes an
// installed extension whitelists come from the package artifact an operator
// delivered, which is an archive carrying the declaration, not bare JSON.
func TestComponentClassesReadsPackedArtifacts(t *testing.T) {
	reference := "test://demo-widgets"
	declaration := extension.Declaration{
		Identifier: "demo.widgets",
		Version:    "1.0.0",
		APIGroup:   "widgets.example.test",
		Contributions: extension.Contributions{
			ObjectClasses: []extension.ObjectClass{{
				Group: "widgets.example.test", Kind: "Widget", Resource: "widgets",
				Versions: []string{"v1"}, Namespaced: true,
			}},
		},
	}
	content, _, err := extension.Pack(declaration, nil)
	if err != nil {
		t.Fatalf("pack the extension: %v", err)
	}
	client := componentClient(t, "demo-widgets", reference)
	classes, err := ComponentClasses(context.Background(), client, extension.MemoryArtifacts{reference: content})
	if err != nil {
		t.Fatalf("read the installed component classes: %v", err)
	}
	if len(classes) != 1 || classes[0].Group != "widgets.example.test" || classes[0].Kind != "Widget" {
		t.Fatalf("classes = %v, want the declared Widget class", classes)
	}
}

// TestComponentClassesRefusesAnUnreadablePackage keeps a broken artifact from
// silently dropping the classes of an installed component.
func TestComponentClassesRefusesAnUnreadablePackage(t *testing.T) {
	reference := "test://broken-widgets"
	client := componentClient(t, "broken-widgets", reference)
	_, err := ComponentClasses(context.Background(), client, extension.MemoryArtifacts{reference: []byte("not a package")})
	if err == nil {
		t.Fatal("a component whose package cannot be read contributed no error")
	}
}
