package harness

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/api/v1beta1"
	"github.com/tsic404/kosd/pkg/app"
	"github.com/tsic404/kosd/pkg/markers"
	"github.com/tsic404/kosd/pkg/reconcile"
	"github.com/tsic404/kosd/pkg/write"
)

// InstallDefinition publishes one application definition as the catalogue does
// (C-03). Publishing the same name again with new content is the definition
// update of C-32: the entry keeps its name and the applications that name it
// keep their installed objects.
func (c *Cluster) InstallDefinition(t *testing.T, definition *v1beta1.ApplicationDefinition) {
	t.Helper()
	document, err := runtime.DefaultUnstructuredConverter.ToUnstructured(definition)
	if err != nil {
		t.Fatalf("encode definition %q: %v", definition.Name, err)
	}
	object := &unstructured.Unstructured{Object: document}
	object.SetGroupVersionKind(schema.GroupVersionKind{Group: api.Group, Version: "v1beta1", Kind: "ApplicationDefinition"})
	object.SetLabels(map[string]string{markers.ManagedBy: write.ManagedByValue})
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	if _, err := c.Writer().Apply(ctx, write.SubjectCatalogueSync, write.Session{}, write.PartSpec, object); err != nil {
		t.Fatalf("install definition %q: %v", definition.Name, err)
	}
}

// ApplicationManifest builds the body that creates one application through the
// control plane (C-12, C-32).
func ApplicationManifest(name, namespace, definitionRef, targetNamespace string, config map[string]any, volumes []string) map[string]any {
	spec := map[string]any{
		"definitionRef": definitionRef,
		"namespace":     targetNamespace,
	}
	if len(config) > 0 {
		spec["config"] = config
	}
	if len(volumes) > 0 {
		spec["volumes"] = volumes
	}
	body := Manifest("Application", namespace, name, map[string]string{markers.ManagedBy: write.ManagedByValue}, nil)
	body["spec"] = spec
	return body
}

// CreateApplication creates one application the controller installs (C-32).
func (c *Cluster) CreateApplication(t *testing.T, name, definitionRef, targetNamespace string) *unstructured.Unstructured {
	t.Helper()
	body := ApplicationManifest(name, Namespace, definitionRef, targetNamespace, nil, nil)
	return c.Create(t, &unstructured.Unstructured{Object: body})
}

// ApplicationController builds the real application controller over the
// cluster. A case drives it round by round, so a lifecycle step is observed
// before the next one runs (C-06).
func (c *Cluster) ApplicationController(t *testing.T, options app.Options) *app.Controller {
	t.Helper()
	return app.NewController(app.NewWriterStore(c.Writer()), options)
}

// ApplicationRound runs one application round (C-06).
func (c *Cluster) ApplicationRound(t *testing.T, controller *app.Controller, name string) {
	t.Helper()
	if _, err := controller.Reconcile(context.Background(), reconcile.Request{
		Kind: "Application", Namespace: Namespace, Name: name,
	}); err != nil {
		t.Fatalf("reconcile application %s: %v", name, err)
	}
}

// SettleApplication runs rounds until the application leaves the install
// sequence or the budget ends, and returns the last phase.
func (c *Cluster) SettleApplication(t *testing.T, controller *app.Controller, name string, rounds int) string {
	t.Helper()
	phase := ""
	for range rounds {
		c.ApplicationRound(t, controller, name)
		phase = c.ApplicationPhase(t, name)
		if phase == app.PhaseReady || phase == app.PhaseFailed {
			break
		}
	}
	return phase
}

// ApplicationPhase reads the stored phase (C-33).
func (c *Cluster) ApplicationPhase(t *testing.T, name string) string {
	t.Helper()
	phase, _, err := unstructured.NestedString(c.Get(t, "v1beta1", "Application", name).Object, "status", "phase")
	if err != nil {
		t.Fatalf("read application %s phase: %v", name, err)
	}
	return phase
}

// ApplicationStatusOf decodes the stored application status (C-07, C-33).
func (c *Cluster) ApplicationStatusOf(t *testing.T, name string) v1beta1.ApplicationStatus {
	t.Helper()
	raw, found, err := unstructured.NestedMap(c.Get(t, "v1beta1", "Application", name).Object, "status")
	if err != nil {
		t.Fatalf("read application %s status: %v", name, err)
	}
	if !found {
		t.Fatalf("application %s has no status", name)
	}
	var status v1beta1.ApplicationStatus
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &status); err != nil {
		t.Fatalf("decode application %s status: %v", name, err)
	}
	return status
}

// configTemplate renders one ConfigMap in the declared namespace. Lifecycle
// cases use it where the package content itself is not under test.
const configTemplate = `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Name }}
  namespace: {{ .Namespace }}
data:
  mode: "acceptance"
`

// DefinitionContent builds a definition from explicit package content: the
// template it renders, the hooks it runs and the data format it declares. Its
// digest is computed from the content, as the catalogue does (C-32).
func DefinitionContent(t *testing.T, name, dataFormat, template string, hooks []v1beta1.Hook, fields []v1beta1.ConfigField) *v1beta1.ApplicationDefinition {
	t.Helper()
	definition := &v1beta1.ApplicationDefinition{
		TypeMeta:   metav1.TypeMeta{APIVersion: api.Group + "/v1beta1", Kind: "ApplicationDefinition"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.ApplicationDefinitionSpec{
			Package:  v1beta1.PackageIdentity{Version: "1.0.0", DataFormat: dataFormat},
			Config:   fields,
			Template: template,
			Hooks:    hooks,
		},
	}
	digest, err := app.DefinitionDigest(definition.Spec)
	if err != nil {
		t.Fatalf("digest definition %q: %v", name, err)
	}
	definition.Spec.Package.Digest = digest
	if err := app.ValidateDefinition(definition.Spec); err != nil {
		t.Fatalf("definition %q is invalid: %v", name, err)
	}
	return definition
}

// Definition builds a definition that renders one ConfigMap and carries the
// given capability requirements. Its package digest is computed from the
// content, as the catalogue does (C-32).
func Definition(t *testing.T, name string, requires []v1beta1.CapabilityRequirement) *v1beta1.ApplicationDefinition {
	t.Helper()
	return TemplateDefinition(t, name, configTemplate, nil, requires...)
}

// DefinitionWithTemplate is Definition with a translation template the case
// supplies, so a package can be shaped for what the case measures.
func DefinitionWithTemplate(t *testing.T, name, template string, requires []v1beta1.CapabilityRequirement) *v1beta1.ApplicationDefinition {
	t.Helper()
	return TemplateDefinition(t, name, template, nil, requires...)
}

// TemplateDefinition builds a definition with the given translation template and
// package resource defaults. Its package digest is computed from the content.
func TemplateDefinition(t *testing.T, name, template string, resources *v1beta1.ResourceRequirements, requires ...v1beta1.CapabilityRequirement) *v1beta1.ApplicationDefinition {
	t.Helper()
	definition := &v1beta1.ApplicationDefinition{
		TypeMeta: metav1.TypeMeta{APIVersion: api.Group + "/v1beta1", Kind: "ApplicationDefinition"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1beta1.ApplicationDefinitionSpec{
			Package:   v1beta1.PackageIdentity{Version: "1.0.0"},
			Template:  template,
			Requires:  requires,
			Resources: resources,
		},
	}
	digest, err := app.DefinitionDigest(definition.Spec)
	if err != nil {
		t.Fatalf("digest definition %q: %v", name, err)
	}
	definition.Spec.Package.Digest = digest
	if err := app.ValidateDefinition(definition.Spec); err != nil {
		t.Fatalf("definition %q is invalid: %v", name, err)
	}
	return definition
}
