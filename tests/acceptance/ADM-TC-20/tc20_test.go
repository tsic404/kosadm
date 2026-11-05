// Package admtc20 verifies the install invariant on a clean existing cluster: the
// install window passes when only KOS subjects write whitelisted objects, and
// each installer write to an unlabelled object outside the whitelist fails the
// judgment naming the object and the requester (ADM-AC-9, ADM-TC-20).
package admtc20

import (
	"context"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
)

// quietWindow shortens the C-15 quiet period. The contract value is pinned by
// the package's unit test; the case exercises the mechanism, not the duration.
const quietWindow = 200 * time.Millisecond

// windowObject is the object the violating run writes in kube-system.
func windowObject(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": name, "namespace": "kube-system"},
		"data":       map[string]any{"origin": "installer"},
	}}
}

func TestTC20CleanClusterInstallWindow(t *testing.T) {
	cluster := harness.StartAudited(t)
	cluster.EnsureNamespace(t, "kube-system")
	installerIdentity := harness.InstallerIdentity()
	cluster.GrantClusterAdmin(t, installerIdentity)
	identity := harness.OpenIdentity(t, nil, nil)
	clients := cluster.InstallClients(t, installerIdentity)

	newInstaller := func(t *testing.T, clients harness.InstallClients, hook func(context.Context, string) error) *install.Installer {
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
			QuietWindow:           quietWindow,
			SettleTimeout:         30 * time.Second,
			AfterStep:             hook,
		})
		if err != nil {
			t.Fatalf("build installer: %v", err)
		}
		return runner
	}

	t.Run("clean install passes", func(t *testing.T) {
		result, err := newInstaller(t, clients, nil).Install(context.Background())
		if err != nil {
			t.Fatalf("install: %v", err)
		}
		if !result.Window.Measured {
			t.Fatalf("window = %+v, want a measured window", result.Window)
		}
		if !result.Window.Passed {
			t.Fatalf("window = %+v, want a passed judgment", result.Window)
		}
		if result.StoppedAt != "" || len(result.Steps) == 0 {
			t.Fatalf("run stopped at %q after steps %v", result.StoppedAt, result.Steps)
		}
		if result.OneTimePassword == "" {
			t.Fatalf("install created no one-time password for the first Owner")
		}
	})

	t.Run("rendered-name permissions outside the granted namespace fail", func(t *testing.T) {
		hook := func(ctx context.Context, step string) error {
			if step != install.StepNamespace {
				return nil
			}
			for _, object := range windowPermissions() {
				if _, err := clients.Dynamic.Resource(object.resource).Namespace("kube-system").
					Create(ctx, object.object, metav1.CreateOptions{}); err != nil {
					return err
				}
			}
			return nil
		}
		result, err := newInstaller(t, clients, hook).Install(context.Background())
		if err != nil {
			t.Fatalf("install: %v", err)
		}
		if result.Window.Passed {
			t.Fatalf("the rendered-name permissions in kube-system passed the window")
		}
		for _, kind := range []string{"Role", "RoleBinding"} {
			if !namesPermissionViolation(result.Window.Violations, kind, installerIdentity) {
				t.Fatalf("violations = %+v, want the %s in kube-system by %s", result.Window.Violations, kind, installerIdentity)
			}
		}
	})

	t.Run("a kubeconfig identity is judged", func(t *testing.T) {
		const identity = "kubeconfig-user"
		cluster.GrantClusterAdmin(t, identity)
		operator := cluster.InstallClients(t, identity)
		hook := func(ctx context.Context, step string) error {
			if step != install.StepNamespace {
				return nil
			}
			return writeInWindow(ctx, operator.Dynamic, "create", "kos-window-kubeconfig")
		}
		result, err := newInstaller(t, operator, hook).Install(context.Background())
		if err != nil {
			t.Fatalf("install: %v", err)
		}
		if result.Window.Passed {
			t.Fatalf("the kubeconfig identity's write passed the window")
		}
		if !namesViolationFor(result.Window.Violations, "ConfigMap", "kos-window-kubeconfig", identity) {
			t.Fatalf("violations = %+v, want the write by %s", result.Window.Violations, identity)
		}
	})

	// The whitelist admits a rendered cluster permission only while it still
	// authorizes the KOS identity the installation rendered; a binding that
	// grants the same role to someone else is a violation (C-15).
	t.Run("a rendered cluster permission with another subject fails", func(t *testing.T) {
		ctx := context.Background()
		hook := func(ctx context.Context, step string) error {
			if step != install.StepControlPlane {
				return nil
			}
			binding, err := clients.Kube.RbacV1().ClusterRoleBindings().Get(ctx, install.NodeAgentName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			binding.Subjects = []rbacv1.Subject{{
				Kind: "ServiceAccount", Name: "not-kos", Namespace: harness.Namespace,
			}}
			_, err = clients.Kube.RbacV1().ClusterRoleBindings().Update(ctx, binding, metav1.UpdateOptions{})
			return err
		}
		t.Cleanup(func() {
			_ = clients.Kube.RbacV1().ClusterRoleBindings().Delete(context.Background(), install.NodeAgentName, metav1.DeleteOptions{})
		})
		result, err := newInstaller(t, clients, hook).Install(ctx)
		if err != nil {
			t.Fatalf("install: %v", err)
		}
		if result.Window.Passed {
			t.Fatal("a rendered cluster role binding that grants another subject passed the window")
		}
		if !namesClusterPermissionViolation(result.Window.Violations, "ClusterRoleBinding", install.NodeAgentName, installerIdentity) {
			t.Fatalf("violations = %+v, want the node-agent binding by %s", result.Window.Violations, installerIdentity)
		}
	})

	for _, verb := range []string{"create", "modify", "delete"} {
		t.Run("installer "+verb+" fails the window", func(t *testing.T) {
			name := "kos-window-" + verb
			if verb != "create" {
				if _, err := clients.Dynamic.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).
					Namespace("kube-system").Create(context.Background(), windowObject(name), metav1.CreateOptions{}); err != nil {
					t.Fatalf("place the target object before the window: %v", err)
				}
			}
			write := func(ctx context.Context, step string) error {
				if step != install.StepNamespace {
					return nil
				}
				return writeInWindow(ctx, clients.Dynamic, verb, name)
			}
			result, err := newInstaller(t, clients, write).Install(context.Background())
			if err != nil {
				t.Fatalf("install: %v", err)
			}
			if !result.Window.Measured {
				t.Fatalf("window = %+v, want a measured window", result.Window)
			}
			if result.Window.Passed {
				t.Fatalf("the installer %s of %s passed the window", verb, name)
			}
			if !namesViolation(result.Window.Violations, verb, installerIdentity) {
				t.Fatalf("violations = %+v, want %s of kube-system/%s by %s",
					result.Window.Violations, verb, name, installerIdentity)
			}
		})
	}
}

// writeInWindow performs one installer write inside the install window.
func writeInWindow(ctx context.Context, client dynamic.Interface, verb, name string) error {
	resource := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).Namespace("kube-system")
	switch verb {
	case "create":
		_, err := resource.Create(ctx, windowObject(name), metav1.CreateOptions{})
		return err
	case "modify":
		current, err := resource.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		current.Object["data"] = map[string]any{"origin": "installer-modified"}
		_, err = resource.Update(ctx, current, metav1.UpdateOptions{})
		return err
	case "delete":
		return resource.Delete(ctx, name, metav1.DeleteOptions{})
	default:
		return nil
	}
}

// windowPermissions is the rendered permission pair a KOS subject must not
// write outside the granted namespaces.
func windowPermissions() []struct {
	resource schema.GroupVersionResource
	object   *unstructured.Unstructured
} {
	return []struct {
		resource schema.GroupVersionResource
		object   *unstructured.Unstructured
	}{
		{
			resource: schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"},
			object: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "rbac.authorization.k8s.io/v1",
				"kind":       "Role",
				"metadata":   map[string]any{"name": install.ControlPlaneName, "namespace": "kube-system"},
				"rules":      []any{map[string]any{"apiGroups": []any{"*"}, "resources": []any{"*"}, "verbs": []any{"*"}}},
			}},
		},
		{
			resource: schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"},
			object: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "rbac.authorization.k8s.io/v1",
				"kind":       "RoleBinding",
				"metadata":   map[string]any{"name": install.ControlPlaneName, "namespace": "kube-system"},
				"roleRef": map[string]any{
					"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "cluster-admin",
				},
				"subjects": []any{map[string]any{
					"kind": "User", "apiGroup": "rbac.authorization.k8s.io", "name": "kubeconfig-user",
				}},
			}},
		},
	}
}

// namesPermissionViolation reports one rendered-name permission in kube-system.
func namesPermissionViolation(violations []install.Violation, kind, requester string) bool {
	for _, violation := range violations {
		if violation.Object.Namespace == "kube-system" && violation.Object.Name == install.ControlPlaneName &&
			violation.Object.Kind == kind && violation.Requester == requester {
			return true
		}
	}
	return false
}

// namesViolationFor reports one object by kind, namespace and requester.
func namesViolationFor(violations []install.Violation, kind, name, requester string) bool {
	for _, violation := range violations {
		if violation.Object.Namespace == "kube-system" && violation.Object.Name == name &&
			violation.Object.Kind == kind && violation.Requester == requester {
			return true
		}
	}
	return false
}

// namesViolation reports whether the judgment names the object and requester.
func namesViolation(violations []install.Violation, verb, requester string) bool {
	for _, violation := range violations {
		if violation.Object.Namespace == "kube-system" &&
			violation.Object.Name == "kos-window-"+verb &&
			violation.Object.Kind == "ConfigMap" && violation.Requester == requester {
			return true
		}
	}
	return false
}

// namesClusterPermissionViolation reports one cluster-scoped object by kind,
// name and requester.
func namesClusterPermissionViolation(violations []install.Violation, kind, name, requester string) bool {
	for _, violation := range violations {
		if violation.Object.Kind == kind && violation.Object.Name == name &&
			violation.Object.Namespace == "" && violation.Requester == requester {
			return true
		}
	}
	return false
}
