package install

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/pkg/extension"
	"github.com/tsic404/kosd/pkg/markers"
)

// ComponentClasses lists the object classes the installed components declare.
// The install window whitelists classes an installed extension contributes
// (C-15), so the judgment has to read the same declarations the installer read
// rather than a list a caller assembled by hand.
func ComponentClasses(ctx context.Context, client dynamic.Interface, artifacts extension.ArtifactSource) ([]schema.GroupKind, error) {
	if client == nil || artifacts == nil {
		return nil, nil
	}
	plural, ok := api.ResourceName("SystemComponent")
	if !ok {
		return nil, fmt.Errorf("no resource name for SystemComponent")
	}
	list, err := client.Resource(schema.GroupVersionResource{
		Group: api.Group, Version: "v1beta1", Resource: plural,
	}).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list system components: %w", err)
	}
	classes := make([]schema.GroupKind, 0, len(list.Items))
	for index := range list.Items {
		component := &list.Items[index]
		attribution, err := markers.AttributionOf(component.GetLabels())
		if err != nil || attribution == markers.AttributionUnmanaged {
			continue
		}
		reference, found, err := unstructured.NestedString(component.Object, "spec", "package", "reference")
		if err != nil || !found || reference == "" {
			// An adopted component installs no package and contributes no class.
			continue
		}
		content, err := artifacts.Fetch(ctx, reference)
		if err != nil {
			return nil, fmt.Errorf("read the package of component %s: %w", component.GetName(), err)
		}
		declaration, err := extension.Parse(content)
		if err != nil {
			return nil, fmt.Errorf("read the declaration of component %s: %w", component.GetName(), err)
		}
		loaded, err := declaration.Load()
		if err != nil {
			return nil, fmt.Errorf("load the declaration of component %s: %w", component.GetName(), err)
		}
		for _, class := range loaded.Contributions.ObjectClasses {
			classes = append(classes, schema.GroupKind{Group: class.Group, Kind: class.Kind})
		}
	}
	return classes, nil
}
