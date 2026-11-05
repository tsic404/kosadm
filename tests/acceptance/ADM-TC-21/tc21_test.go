// Package admtc21 verifies the install window of a cluster that receives an
// application with a volume and an endpoint plus an extension: objects a
// cluster controller creates for the declaration pass, KOS subjects may write
// only attributed objects, extension classes are whitelisted, and an installer
// write to an unlabelled object fails (ADM-AC-9, ADM-TC-21).
package admtc21

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/api/v1beta1"
	"github.com/tsic404/kosd/pkg/extension"
	"github.com/tsic404/kosd/pkg/markers"
)

const (
	widgetsGroup = "widgets.example.test"
	// quietWindow shortens the C-15 quiet period; the contract value is pinned
	// by the package unit test.
	quietWindow = 200 * time.Millisecond
)

// declaration is the extension the window installs.
func declaration() extension.Declaration {
	return extension.Declaration{
		Identifier: "demo.widgets",
		Version:    "1.0.0",
		APIGroup:   widgetsGroup,
		Contributions: extension.Contributions{
			ObjectClasses: []extension.ObjectClass{{
				Group: widgetsGroup, Kind: "Widget", Resource: "widgets",
				Versions: []string{"v1"}, Namespaced: true,
			}},
			Controllers: []extension.Controller{{
				ID: "widget", Manages: []extension.GroupKind{{Group: widgetsGroup, Kind: "Widget"}},
			}},
		},
	}
}

func TestTC21InstallWindowWithVolumeAndExtension(t *testing.T) {
	cluster := harness.StartAudited(t)
	installerIdentity := harness.InstallerIdentity()
	controlPlaneIdentity := harness.ControlPlaneIdentity()
	cluster.GrantClusterAdmin(t, installerIdentity)
	cluster.GrantClusterAdmin(t, controlPlaneIdentity)
	artifacts := extension.MemoryArtifacts{}
	identity := harness.OpenIdentity(t, nil, nil)
	clients := cluster.InstallClients(t, installerIdentity)
	rendered := cluster.DynamicAs(t, controlPlaneIdentity)

	// The extension is installed inside the window: its class definition is
	// written by the control-plane identity, which is the C-15 whitelist branch
	// for extension-contributed definitions.
	setup := func(ctx context.Context) error {
		cluster.InstallCRDAs(t, controlPlaneIdentity, widgetsGroup, "v1", "Widget", "widgets", true)
		cluster.InstallPackage(t, "demo-widgets", declaration(), artifacts)
		return declareObjects(ctx, cluster, rendered)
	}
	var once sync.Once
	hook := func(ctx context.Context, step string) error {
		if step != install.StepNamespace {
			return nil
		}
		var err error
		once.Do(func() { err = setup(ctx) })
		return err
	}

	newInstaller := func(t *testing.T, after func(context.Context, string) error, settle time.Duration) *install.Installer {
		t.Helper()
		runner, err := install.New(install.Config{
			NoNodeAgentController: true,
			Plan:                  harness.InstallerPlan(nil),
			Dynamic:               clients.Dynamic,
			Kube:                  clients.Kube,
			APIExtensions:         clients.APIExtensions,
			Definitions:           harness.Definitions(t),
			Accounts:              identity.Service,
			Audit:                 cluster.AuditSource(t),
			Artifacts:             artifacts,
			QuietWindow:           quietWindow,
			SettleTimeout:         settle,
			AfterStep:             after,
		})
		if err != nil {
			t.Fatalf("build installer: %v", err)
		}
		return runner
	}

	t.Run("volume and extension window passes", func(t *testing.T) {
		result, err := newInstaller(t, hook, 30*time.Second).Install(context.Background())
		if err != nil {
			t.Fatalf("install: %v", err)
		}
		if !result.Window.Measured || !result.Window.Passed {
			t.Fatalf("window = %+v, want a measured pass", result.Window)
		}
	})

	for _, verb := range []string{"create", "modify", "delete"} {
		t.Run("installer "+verb+" outside the whitelist fails", func(t *testing.T) {
			name := "kos-window-" + verb
			if verb != "create" {
				createObject(t, clients.Dynamic, applicationConfigMap(name))
			}
			after := func(ctx context.Context, step string) error {
				if step != install.StepNamespace {
					return nil
				}
				return writeInApplicationNamespace(ctx, clients.Dynamic, verb, name)
			}
			result, err := newInstaller(t, after, 30*time.Second).Install(context.Background())
			if err != nil {
				t.Fatalf("install: %v", err)
			}
			if !result.Window.Measured || result.Window.Passed {
				t.Fatalf("window = %+v, want a measured failure", result.Window)
			}
			if !namesViolation(result.Window.Violations, name, installerIdentity) {
				t.Fatalf("violations = %+v, want kube-system/../%s by %s",
					result.Window.Violations, name, installerIdentity)
			}
		})
	}

	t.Run("an unreconciled extension instance blocks the window", func(t *testing.T) {
		widgets := rendered.Resource(schema.GroupVersionResource{Group: widgetsGroup, Version: "v1", Resource: "widgets"}).Namespace(harness.Namespace)
		broken := unreconciledWidget("broken")
		var once sync.Once
		after := func(ctx context.Context, step string) error {
			if step != install.StepNamespace {
				return nil
			}
			var err error
			once.Do(func() { _, err = widgets.Create(ctx, broken, metav1.CreateOptions{}) })
			return err
		}
		t.Cleanup(func() { _ = widgets.Delete(context.Background(), "broken", metav1.DeleteOptions{}) })
		_, err := newInstaller(t, after, time.Second).Install(context.Background())
		if err == nil {
			t.Fatal("install passed with an unreconciled extension instance")
		}
		if !strings.Contains(err.Error(), "Widget") || !strings.Contains(err.Error(), "Reconciled=False") {
			t.Fatalf("install error = %v, want the unreconciled Widget named", err)
		}
	})
}

// unreconciledWidget is an extension instance whose controller has not
// reconciled it; the install window must wait for it (C-15).
func unreconciledWidget(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": widgetsGroup + "/v1",
		"kind":       "Widget",
		"metadata": map[string]any{
			"name": name, "namespace": harness.Namespace,
			"labels": map[string]any{markers.ManagedBy: "kosd"},
		},
		"status": map[string]any{"conditions": []any{map[string]any{
			"type": "Reconciled", "status": "False", "reason": "Pending", "message": "controller has not reconciled",
		}}},
	}}
}

// declareObjects shapes what an install window of this cluster contains: the
// declared application objects, the rendered downstream objects with their
// attribution, a controller-created object without attribution, and an
// extension instance.
func declareObjects(ctx context.Context, cluster *harness.Cluster, rendered dynamic.Interface) error {
	application := harness.Application("shop", nil)
	application, err := cluster.Dynamic.Resource(kernelResource("Application")).Namespace(harness.Namespace).
		Create(ctx, application, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create application: %w", err)
	}
	volume, err := cluster.Dynamic.Resource(kernelResource("Volume")).Namespace(harness.Namespace).
		Create(ctx, kernelObject("Volume", "data"), metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create volume: %w", err)
	}
	if _, err := cluster.Dynamic.Resource(kernelResource("Endpoint")).Namespace(harness.Namespace).
		Create(ctx, endpointObject(), metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create endpoint: %w", err)
	}

	deployment := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name": "shop", "namespace": harness.Namespace,
			"labels": map[string]any{
				markers.ManagedBy: "kosd", markers.OwnerUID: string(application.GetUID()),
			},
		},
		"spec": map[string]any{
			"replicas": int64(1),
			"selector": map[string]any{"matchLabels": map[string]any{"app": "shop"}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": "shop"}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "shop", "image": "busybox"}}},
			},
		},
	}}
	if _, err := rendered.Resource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}).
		Namespace(harness.Namespace).Create(ctx, deployment, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create rendered deployment: %w", err)
	}
	claim := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata": map[string]any{
			"name": "data", "namespace": harness.Namespace,
			"labels": map[string]any{
				markers.ManagedBy: "kosd", markers.OwnerUID: string(volume.GetUID()),
			},
		},
		"spec": map[string]any{
			"accessModes": []any{"ReadWriteOnce"},
			"resources":   map[string]any{"requests": map[string]any{"storage": "1Gi"}},
		},
	}}
	claimResource := rendered.Resource(schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}).Namespace(harness.Namespace)
	createdClaim, err := claimResource.Create(ctx, claim, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create bound claim: %w", err)
	}
	// The volume reconciler records the binding; the install window waits for
	// no pending claim (C-15).
	createdClaim.Object["status"] = map[string]any{"phase": "Bound"}
	if _, err := claimResource.UpdateStatus(ctx, createdClaim, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("bind the claim: %w", err)
	}
	widget := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": widgetsGroup + "/v1",
		"kind":       "Widget",
		"metadata": map[string]any{
			"name": "gauge", "namespace": harness.Namespace,
			"labels": map[string]any{markers.ManagedBy: "kosd"},
		},
	}}
	if _, err := rendered.Resource(schema.GroupVersionResource{Group: widgetsGroup, Version: "v1", Resource: "widgets"}).
		Namespace(harness.Namespace).Create(ctx, widget, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create extension instance: %w", err)
	}
	// The cluster controller creates the Service the declaration asks for; it
	// is not a KOS subject and must not fail the judgment (C-15).
	service := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   map[string]any{"name": "shop", "namespace": harness.Namespace},
		"spec":       map[string]any{"ports": []any{map[string]any{"port": int64(80)}}},
	}}
	if _, err := cluster.Dynamic.Resource(schema.GroupVersionResource{Version: "v1", Resource: "services"}).
		Namespace(harness.Namespace).Create(ctx, service, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create controller service: %w", err)
	}
	return nil
}

// kernelResource resolves one kernel kind to its resource name.
func kernelResource(kind string) schema.GroupVersionResource {
	plural, _ := api.ResourceName(kind)
	return schema.GroupVersionResource{Group: api.Group, Version: v1beta1.GroupVersion.Version, Resource: plural}
}

// kernelObject builds an empty-spec kernel object.
func kernelObject(kind, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": api.Group + "/" + v1beta1.GroupVersion.Version,
		"kind":       kind,
		"metadata":   map[string]any{"name": name, "namespace": harness.Namespace},
		"spec":       map[string]any{},
	}}
}

// endpointObject builds an endpoint the kernel accepts (C-43).
func endpointObject() *unstructured.Unstructured {
	object := kernelObject("Endpoint", "shop")
	object.Object["spec"] = map[string]any{
		"kind": string(v1beta1.EndpointKindHTTPHost), "targetApp": "shop", "host": "shop.test",
	}
	return object
}

// applicationConfigMap is the unlabelled object the violating run writes.
func applicationConfigMap(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": name, "namespace": harness.Namespace},
		"data":       map[string]any{"origin": "installer"},
	}}
}

// createObject writes one object through the admin client, before the window.
func createObject(t *testing.T, client dynamic.Interface, object *unstructured.Unstructured) {
	t.Helper()
	resource := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).Namespace(harness.Namespace)
	if _, err := resource.Create(context.Background(), object, metav1.CreateOptions{}); err != nil {
		t.Fatalf("place %s: %v", object.GetName(), err)
	}
}

// writeInApplicationNamespace performs one installer write inside the window.
func writeInApplicationNamespace(ctx context.Context, client dynamic.Interface, verb, name string) error {
	resource := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).Namespace(harness.Namespace)
	switch verb {
	case "create":
		_, err := resource.Create(ctx, applicationConfigMap(name), metav1.CreateOptions{})
		return err
	case "modify":
		current, err := resource.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		current.Object["data"] = map[string]any{"origin": "modified"}
		_, err = resource.Update(ctx, current, metav1.UpdateOptions{})
		return err
	case "delete":
		return resource.Delete(ctx, name, metav1.DeleteOptions{})
	default:
		return nil
	}
}

// namesViolation reports whether the judgment names the object and requester.
func namesViolation(violations []install.Violation, name, requester string) bool {
	for _, violation := range violations {
		if violation.Object.Namespace == harness.Namespace && violation.Object.Name == name &&
			violation.Object.Kind == "ConfigMap" && violation.Requester == requester {
			return true
		}
	}
	return false
}
