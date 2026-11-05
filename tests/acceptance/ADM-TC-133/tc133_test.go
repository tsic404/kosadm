// Package admtc133 verifies the KOS self-upgrade order: the release's object
// definitions are replaced before its control plane, both control-plane
// versions read and write the same object in between, and the version the
// upgraded control plane requires is served when the upgrade ends
// (ADM-AC-71, ADM-TC-133).
package admtc133

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/tsic404/kosd/pkg/extension"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
)

const (
	// definitionName is the definition whose shape moves with the release.
	definitionName = "applications.kos.tsic.top"
	// releaseDescription is what the release ships; the previous release shipped
	// another description, which is what makes the definition move (C-16).
	releaseDescription = "Application is a namespaced application instance."
	// previousDescription is the shape an older installation left behind.
	previousDescription = "Application is a namespaced application instance (previous release)."
	// objectName is the object both control-plane versions read and write.
	objectName = "window"
	// previousImage and targetImage are the two control-plane releases.
	previousImage = "ghcr.io/tsic404/kos-control-plane:v0.1.0"
	targetImage   = "ghcr.io/tsic404/kos-control-plane:v0.2.0"
	// oldVersion and newVersion are the served versions the two control planes
	// were built against (C-16).
	oldVersion = "v1alpha1"
	newVersion = "v1beta1"
	// dependent is the installed extension whose framework range only admits the
	// version the release stops serving.
	dependent = "old-only"
	// installedFramework is the framework version the previous release serves.
	installedFramework = "1.0"
	// poll and wait bound one upgrade run in the acceptance harness.
	poll = 20 * time.Millisecond
	wait = 60 * time.Second
)

// TestTC133DefinitionsBeforeControlPlane upgrades an installed KOS and observes
// the window between the two halves of the release move.
func TestTC133DefinitionsBeforeControlPlane(t *testing.T) {
	cluster := harness.Start(t)
	cluster.EnsureNamespace(t, install.DefaultNamespace)
	cluster.Create(t, harness.Application(objectName, nil))

	// The cluster starts at the previous release: that release's definitions and
	// the control plane it runs (C-55).
	if _, err := install.Upgrade(context.Background(), install.UpgradeConfig{
		Plan:          upgradePlan(previousImage),
		APIExtensions: cluster.APIExtensions,
		Definitions:   previousDefinitions(t),
		Dynamic:       cluster.Dynamic,
		Poll:          poll,
		Wait:          wait,
	}); err != nil {
		t.Fatalf("install the previous release: %v", err)
	}
	if image := controlPlaneImage(t, cluster); image != previousImage {
		t.Fatalf("the previous release runs image %q, want %q", image, previousImage)
	}
	if description := servedDescription(t, cluster); description != previousDescription {
		t.Fatalf("the served definition carries %q, want the previous release's shape", description)
	}

	// An extension that only runs on the framework version this release stops
	// serving strands the upgrade before any definition is replaced (C-16).
	artifacts := extension.MemoryArtifacts{}
	cluster.InstallPackage(t, dependent, extension.Declaration{
		Identifier:       dependent,
		Version:          "1.0.0",
		FrameworkVersion: ">=" + installedFramework + " <" + extension.FrameworkVersion,
		Contributions: extension.Contributions{Settings: []extension.Setting{{
			ID: "old-only-setting", Scope: extension.SettingScopeCluster, Writer: extension.SettingWriterUser,
		}}},
	}, artifacts)
	frameworks := &extension.Installer{
		Dynamic:    cluster.Dynamic,
		Resources:  cluster.Resources(),
		Artifacts:  artifacts,
		Frameworks: []string{installedFramework},
	}
	refused, err := install.Upgrade(context.Background(), install.UpgradeConfig{
		Plan:          upgradePlan(targetImage),
		APIExtensions: cluster.APIExtensions,
		Definitions:   harness.Definitions(t),
		Dynamic:       cluster.Dynamic,
		Frameworks:    frameworks,
		Poll:          poll,
		Wait:          wait,
	})
	var blocked install.FrameworkBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("the upgrade with a stranded extension = %v, want a framework refusal", err)
	}
	if len(blocked.Preflight.Dependents) != 1 || blocked.Preflight.Dependents[0].Component != dependent {
		t.Fatalf("dependents = %+v, want %s", blocked.Preflight.Dependents, dependent)
	}
	if len(refused.Definitions) != 0 || len(refused.Objects) != 0 {
		t.Fatalf("the refused upgrade wrote %v, %v", refused.Definitions, refused.Objects)
	}
	if image := controlPlaneImage(t, cluster); image != previousImage {
		t.Fatalf("the refused upgrade replaced the control plane with %q", image)
	}
	if description := servedDescription(t, cluster); description != previousDescription {
		t.Fatalf("the refused upgrade replaced the definitions with %q", description)
	}

	// Releasing the dependency lets the same upgrade pass (C-16).
	cluster.DeleteComponent(t, dependent)
	cluster.ExtensionState(t, artifacts)

	// The upgrade replaces the definitions first; the window observation runs
	// when the control-plane workload is written, which is the changeover.
	observed := &window{cluster: cluster}
	result, err := install.Upgrade(context.Background(), install.UpgradeConfig{
		Plan:          upgradePlan(targetImage),
		APIExtensions: cluster.APIExtensions,
		Definitions:   harness.Definitions(t),
		Dynamic:       observed.client(cluster.Dynamic),
		Frameworks:    frameworks,
		Poll:          poll,
		Wait:          wait,
	})
	if err != nil {
		t.Fatalf("upgrade the release: %v", err)
	}
	if !observed.reached {
		t.Fatal("the upgrade never replaced the control-plane workload")
	}
	if !observed.definitionApplied {
		t.Fatal("the control plane was replaced before its definition carried the release's shape")
	}
	if !observed.fieldsMatch {
		t.Fatalf("the two served versions read different field sets in the window: %v", observed.problems)
	}
	if !observed.firstWriteAccepted || !observed.secondWriteAccepted {
		t.Fatalf("a control-plane version could not write in the window: %v", observed.problems)
	}
	if !containsString(result.Definitions, definitionName) {
		t.Fatalf("the upgrade applied %v, want the definition %s", result.Definitions, definitionName)
	}
	if result.Frameworks == nil || result.Frameworks.IsBlocked {
		t.Fatalf("framework preflight = %+v, want an unblocked judgement", result.Frameworks)
	}

	// After the upgrade both served versions stay up, and the version the new
	// control plane requires is served: it does not refuse to start (C-16).
	if image := controlPlaneImage(t, cluster); image != targetImage {
		t.Fatalf("the upgraded control plane runs image %q, want %q", image, targetImage)
	}
	if description := servedDescription(t, cluster); description != releaseDescription {
		t.Fatalf("the served definition carries %q, want the release's shape", description)
	}
	for _, version := range []string{oldVersion, newVersion} {
		if !servesVersion(t, cluster, version) {
			t.Fatalf("the upgraded definition does not serve %s", version)
		}
	}
}

// upgradePlan is the existing-cluster plan of one control-plane release.
func upgradePlan(image string) install.Plan {
	return install.Plan{
		Mode:      install.ModeExisting,
		Namespace: install.DefaultNamespace,
		Owner:     "owner",
		Image:     image,
	}
}

// previousDefinitions writes the release's manifests with the applications
// definition carrying the previous release's shape, which is what an older
// installation left in the cluster.
func previousDefinitions(t *testing.T) install.CRDDirectory {
	t.Helper()
	release := harness.CRDDirectory(t)
	directory := t.TempDir()
	entries, err := os.ReadDir(release)
	if err != nil {
		t.Fatalf("read the release's definitions: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		content, err := os.ReadFile(filepath.Join(release, entry.Name()))
		if err != nil {
			t.Fatalf("read definition %s: %v", entry.Name(), err)
		}
		if strings.Contains(string(content), definitionName) {
			content = bytes.ReplaceAll(content, []byte(releaseDescription), []byte(previousDescription))
		}
		if err := os.WriteFile(filepath.Join(directory, entry.Name()), content, 0o600); err != nil {
			t.Fatalf("write definition %s: %v", entry.Name(), err)
		}
	}
	return install.CRDDirectory{FS: os.DirFS(directory)}
}

// window observes the changeover: it reads and writes the same object through
// both served versions at the moment the control-plane workload is replaced.
type window struct {
	once    sync.Once
	cluster *harness.Cluster
	reached bool
	// definitionApplied reports whether the definition already carried the
	// release's shape when the control plane was written.
	definitionApplied   bool
	fieldsMatch         bool
	firstWriteAccepted  bool
	secondWriteAccepted bool
	problems            []string
}

// client wraps one dynamic client so the window observation runs before the
// control-plane workload changes.
func (w *window) client(inner dynamic.Interface) dynamic.Interface {
	return windowClient{Interface: inner, window: w}
}

// observe records the window once, however many writes the upgrade makes.
func (w *window) observe() {
	w.once.Do(func() {
		w.reached = true
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		defer cancel()
		w.definitionApplied = servedDescriptionOf(ctx, w.cluster) == releaseDescription
		oldObject, oldErr := w.cluster.GetObject(ctx, oldVersion, "Application", harness.Namespace, objectName)
		newObject, newErr := w.cluster.GetObject(ctx, newVersion, "Application", harness.Namespace, objectName)
		if oldErr != nil || newErr != nil {
			w.problems = append(w.problems, "read: "+errorText(oldErr)+errorText(newErr))
			return
		}
		w.fieldsMatch = reflect.DeepEqual(harness.Flatten(oldObject.Object), harness.Flatten(newObject.Object))
		if !w.fieldsMatch {
			w.problems = append(w.problems, "the field sets differ")
		}
		w.firstWriteAccepted = writeAnnotation(ctx, w.cluster, oldVersion, "window/old") == nil
		w.secondWriteAccepted = writeAnnotation(ctx, w.cluster, newVersion, "window/new") == nil
		if !w.firstWriteAccepted || !w.secondWriteAccepted {
			w.problems = append(w.problems, "a version refused a write")
		}
	})
}

// writeAnnotation writes one object through one served version, the way a
// control plane built against that version does.
func writeAnnotation(ctx context.Context, cluster *harness.Cluster, version, key string) error {
	resource, err := cluster.Resource(version, "Application", harness.Namespace)
	if err != nil {
		return err
	}
	object, err := resource.Get(ctx, objectName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	annotations := object.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[key] = "window"
	object.SetAnnotations(annotations)
	_, err = resource.Update(ctx, object, metav1.UpdateOptions{})
	return err
}

// windowClient routes deployment writes through the window observation.
type windowClient struct {
	dynamic.Interface
	window *window
}

// Resource implements dynamic.Interface: a write to the control-plane workload
// is the changeover, so the observation runs before it lands.
func (c windowClient) Resource(resource schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	inner := c.Interface.Resource(resource)
	if resource.Resource != "deployments" {
		return inner
	}
	return windowResource{NamespaceableResourceInterface: inner, window: c.window}
}

// windowResource keeps the observation on the namespaced client the upgrade
// writes a workload through.
type windowResource struct {
	dynamic.NamespaceableResourceInterface
	window *window
}

// Namespace implements dynamic.NamespaceableResourceInterface.
func (r windowResource) Namespace(namespace string) dynamic.ResourceInterface {
	return windowInterface{ResourceInterface: r.NamespaceableResourceInterface.Namespace(namespace), window: r.window}
}

// windowInterface runs the observation before one workload write.
type windowInterface struct {
	dynamic.ResourceInterface
	window *window
}

// Create implements dynamic.ResourceInterface.
func (i windowInterface) Create(ctx context.Context, object *unstructured.Unstructured, options metav1.CreateOptions, subresources ...string) (*unstructured.Unstructured, error) {
	i.window.observe()
	return i.ResourceInterface.Create(ctx, object, options, subresources...)
}

// Update implements dynamic.ResourceInterface.
func (i windowInterface) Update(ctx context.Context, object *unstructured.Unstructured, options metav1.UpdateOptions, subresources ...string) (*unstructured.Unstructured, error) {
	i.window.observe()
	return i.ResourceInterface.Update(ctx, object, options, subresources...)
}

// errorText renders one error for the failure message.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error() + "; "
}

// containsString reports whether values carries want.
func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// controlPlaneImage reads the image the control-plane workload runs.
func controlPlaneImage(t *testing.T, cluster *harness.Cluster) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	deployment, err := cluster.Direct(t, "apps", "v1", "Deployment", install.DefaultNamespace).
		Get(ctx, install.ControlPlaneName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return ""
	}
	if err != nil {
		t.Fatalf("read the control-plane workload: %v", err)
	}
	containers, _, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	if err != nil && len(containers) == 0 {
		t.Fatalf("read the control-plane workload's containers: %v", err)
	}
	for _, container := range containers {
		entry, ok := container.(map[string]any)
		if !ok {
			continue
		}
		if image, found, _ := unstructured.NestedString(entry, "image"); found {
			return image
		}
	}
	return ""
}

// servedDefinition reads the applications definition the cluster serves.
func servedDefinition(t *testing.T, cluster *harness.Cluster) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	definition, err := cluster.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().
		Get(ctx, definitionName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the served definition %s: %v", definitionName, err)
	}
	return definition
}

// servedDescription reads the storage version's schema description.
func servedDescription(t *testing.T, cluster *harness.Cluster) string {
	t.Helper()
	return descriptionOf(servedDefinition(t, cluster))
}

// servedDescriptionOf is servedDescription for the window observation, which
// runs off the test goroutine's helpers.
func servedDescriptionOf(ctx context.Context, cluster *harness.Cluster) string {
	definition, err := cluster.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().
		Get(ctx, definitionName, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	return descriptionOf(definition)
}

// descriptionOf reads the schema description of the storage version.
func descriptionOf(definition *apiextensionsv1.CustomResourceDefinition) string {
	for _, version := range definition.Spec.Versions {
		if !version.Storage || version.Schema == nil || version.Schema.OpenAPIV3Schema == nil {
			continue
		}
		return version.Schema.OpenAPIV3Schema.Description
	}
	return ""
}

// servesVersion reports whether the definition serves one version.
func servesVersion(t *testing.T, cluster *harness.Cluster, version string) bool {
	t.Helper()
	for _, served := range servedDefinition(t, cluster).Spec.Versions {
		if served.Name == version && served.Served {
			return true
		}
	}
	return false
}
