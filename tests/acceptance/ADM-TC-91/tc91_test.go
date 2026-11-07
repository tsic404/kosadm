// Package admtc91 verifies the uninstall leftover manifest: all six categories are
// reported, the control plane and the object definitions are removed, and the
// data follows the retention option (ADM-AC-46, ADM-TC-91).
package admtc91

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/api/v1beta1"
	"github.com/tsic404/kosd/pkg/adoption"
	"github.com/tsic404/kosd/pkg/extension"
	"github.com/tsic404/kosd/pkg/markers"
)

func TestTC91UninstallLeftoverManifest(t *testing.T) {
	cluster := harness.Start(t)
	installerIdentity := harness.InstallerIdentity()
	cluster.GrantClusterAdmin(t, installerIdentity)
	identity := harness.OpenIdentity(t, nil, nil)
	clients := cluster.InstallClients(t, installerIdentity)
	ctx := context.Background()

	runner, err := install.New(install.Config{
		Plan:                  harness.InstallerPlan(nil),
		Dynamic:               clients.Dynamic,
		Kube:                  clients.Kube,
		APIExtensions:         clients.APIExtensions,
		Definitions:           harness.Definitions(t),
		Accounts:              identity.Service,
		QuietWindow:           200 * time.Millisecond,
		SettleTimeout:         30 * time.Second,
		NoNodeAgentController: true,
	})
	if err != nil {
		t.Fatalf("build installer: %v", err)
	}
	if _, err := runner.Install(ctx); err != nil {
		t.Fatalf("install: %v", err)
	}
	artifacts := extension.MemoryArtifacts{}
	seedLeftovers(t, cluster)
	cluster.InstallPackage(t, "demo-widgets", declaration(), artifacts)

	uninstaller, err := install.NewUninstaller(install.UninstallConfig{
		RetainData:    true,
		Dynamic:       clients.Dynamic,
		Kube:          clients.Kube,
		APIExtensions: clients.APIExtensions,
		Discovery:     clients.Kube.Discovery(),
		Artifacts:     artifacts,
	})
	if err != nil {
		t.Fatalf("build uninstaller: %v", err)
	}
	databaseBefore := fileDigest(t, identity.DatabasePath)
	manifest, err := uninstaller.Uninstall(ctx)
	if err != nil {
		t.Fatalf("uninstall: %v", err)
	}
	// The uninstall retains the control-plane database: the operator's next
	// install reuses the Owner it holds, so nothing may rewrite it here.
	if digest := fileDigest(t, identity.DatabasePath); digest != databaseBefore {
		t.Fatal("the uninstall changed the control-plane database")
	}

	for _, category := range install.Categories() {
		if len(manifest.ByCategory(category)) == 0 {
			t.Fatalf("manifest category %q is empty: %+v", category, manifest.Entries)
		}
	}
	if !hasLeftover(manifest.ByCategory(install.LeftoverExtensionData), extension.ResidualRetained) {
		t.Fatalf("extension data = %+v, want the retained entry", manifest.ByCategory(install.LeftoverExtensionData))
	}
	// A managed claim without the release marker still names data the operator
	// has to find, and its volume carries no marker of its own: the manifest
	// must pair them instead of reporting the claim alone.
	if !namesInstance(manifest.ByCategory(install.LeftoverReleasedClaim), "PersistentVolumeClaim", "cache") {
		t.Fatalf("claims = %+v, want the managed claim without the release marker", manifest.ByCategory(install.LeftoverReleasedClaim))
	}
	if !namesInstance(manifest.ByCategory(install.LeftoverPersistentVolume), "PersistentVolume", "kos-pv-cache") {
		t.Fatalf("volumes = %+v, want the volume of the listed claim", manifest.ByCategory(install.LeftoverPersistentVolume))
	}

	assertRemoved(t, cluster)
	assertNothingDeleting(t, cluster)
	assertForeignNamesUntouched(t, cluster)
	if _, err := cluster.Kube.CoreV1().PersistentVolumeClaims(harness.Namespace).Get(ctx, "data", metav1.GetOptions{}); err != nil {
		t.Fatalf("the retained claim was removed: %v", err)
	}
	if _, err := cluster.Kube.CoreV1().PersistentVolumes().Get(ctx, "kos-pv-data", metav1.GetOptions{}); err != nil {
		t.Fatalf("the retained volume was removed: %v", err)
	}
	// The extension class definition survives the kernel uninstall, so its
	// adopted instance still exists and is reported.
	widgets := cluster.Dynamic.Resource(schema.GroupVersionResource{Group: widgetsGroup, Version: "v1", Resource: "widgets"}).Namespace(harness.Namespace)
	if _, err := widgets.Get(ctx, "legacy-widget", metav1.GetOptions{}); err != nil {
		t.Fatalf("the retained extension instance was removed: %v", err)
	}
	if instances := manifest.ByCategory(install.LeftoverInstance); !namesInstance(instances, "Widget", "legacy-widget") {
		t.Fatalf("instances = %+v, want the extension instance", instances)
	}
	for _, entry := range manifest.ByCategory(install.LeftoverInstance) {
		if entry.Object.Kind == "Application" && entry.Object.Name == "legacy" {
			t.Fatalf("the kernel instance the definition deletion removed is still reported: %+v", entry)
		}
	}
	// The outside binding keeps its grant: the role it names is retained and
	// both objects appear in the manifest.
	if _, err := cluster.Kube.RbacV1().ClusterRoles().Get(ctx, install.ControlPlaneName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the role the outside binding names was deleted: %v", err)
	}
	permissions := manifest.ByCategory(install.LeftoverClusterPermission)
	if !hasPermission(permissions, "ClusterRoleBinding", "alice-kos-view") ||
		!hasPermission(permissions, "ClusterRole", install.ControlPlaneName) {
		t.Fatalf("cluster permissions = %+v, want the binding and the role it keeps", permissions)
	}
}

// TestTC91UninstallRemovesData covers the other side of the retention option:
// with a disposer the claims and volumes are gone and the manifest reports
// none.
func TestTC91UninstallRemovesData(t *testing.T) {
	cluster := harness.Start(t)
	installerIdentity := harness.InstallerIdentity()
	cluster.GrantClusterAdmin(t, installerIdentity)
	identity := harness.OpenIdentity(t, nil, nil)
	clients := cluster.InstallClients(t, installerIdentity)
	ctx := context.Background()

	runner, err := install.New(install.Config{
		Plan:                  harness.InstallerPlan(nil),
		Dynamic:               clients.Dynamic,
		Kube:                  clients.Kube,
		APIExtensions:         clients.APIExtensions,
		Definitions:           harness.Definitions(t),
		Accounts:              identity.Service,
		QuietWindow:           200 * time.Millisecond,
		SettleTimeout:         30 * time.Second,
		NoNodeAgentController: true,
	})
	if err != nil {
		t.Fatalf("build installer: %v", err)
	}
	if _, err := runner.Install(ctx); err != nil {
		t.Fatalf("install: %v", err)
	}
	seedManagedClaim(t, cluster)

	uninstaller, err := install.NewUninstaller(install.UninstallConfig{
		RetainData:    false,
		Dispose:       disposer{kube: cluster.Kube},
		Dynamic:       clients.Dynamic,
		Kube:          clients.Kube,
		APIExtensions: clients.APIExtensions,
		Discovery:     clients.Kube.Discovery(),
	})
	if err != nil {
		t.Fatalf("build uninstaller: %v", err)
	}
	manifest, err := uninstaller.Uninstall(ctx)
	if err != nil {
		t.Fatalf("uninstall: %v", err)
	}
	if entries := manifest.ByCategory(install.LeftoverReleasedClaim); len(entries) != 0 {
		t.Fatalf("claims = %+v, want none after disposal", entries)
	}
	if entries := manifest.ByCategory(install.LeftoverPersistentVolume); len(entries) != 0 {
		t.Fatalf("volumes = %+v, want none after disposal", entries)
	}
	if _, err := cluster.Kube.CoreV1().PersistentVolumeClaims(harness.Namespace).Get(ctx, "data", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the disposed claim still exists: %v", err)
	}
	if _, err := cluster.Kube.CoreV1().PersistentVolumes().Get(ctx, "kos-pv-data", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the disposed volume still exists: %v", err)
	}
}

// disposer removes the installation data the way the volume path would.
type disposer struct {
	kube kubernetes.Interface
}

// Dispose deletes the seeded claim and volume. The test cluster runs no
// protection controller, so the disposer releases the admission-added
// finalizers the way the volume path does (C-39).
func (d disposer) Dispose(ctx context.Context) error {
	claim, err := d.kube.CoreV1().PersistentVolumeClaims(harness.Namespace).Get(ctx, "data", metav1.GetOptions{})
	if err == nil {
		claim.Finalizers = nil
		if _, err := d.kube.CoreV1().PersistentVolumeClaims(harness.Namespace).Update(ctx, claim, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	if err := d.kube.CoreV1().PersistentVolumeClaims(harness.Namespace).Delete(ctx, "data", metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	volume, err := d.kube.CoreV1().PersistentVolumes().Get(ctx, "kos-pv-data", metav1.GetOptions{})
	if err == nil {
		volume.Finalizers = nil
		if _, err := d.kube.CoreV1().PersistentVolumes().Update(ctx, volume, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	if err := d.kube.CoreV1().PersistentVolumes().Delete(ctx, "kos-pv-data", metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// namesInstance reports one object in the instance category.
func namesInstance(entries []install.Leftover, kind, name string) bool {
	for _, entry := range entries {
		if entry.Object.Kind == kind && entry.Object.Name == name {
			return true
		}
	}
	return false
}

// hasPermission reports one object in the cluster-permission category.
func hasPermission(entries []install.Leftover, kind, name string) bool {
	return namesInstance(entries, kind, name)
}

// seedLeftovers shapes what a cluster with history holds before the uninstall.
func seedLeftovers(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	ctx := context.Background()
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data", Namespace: harness.Namespace,
			Labels: map[string]string{markers.Released: "true"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:  "kos-pv-data",
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		},
	}
	if _, err := cluster.Kube.CoreV1().PersistentVolumeClaims(harness.Namespace).Create(ctx, claim, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the released claim: %v", err)
	}
	volume := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "kos-pv-data",
			Labels: map[string]string{markers.ManagedBy: "kosd"},
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/data"},
			},
		},
	}
	if _, err := cluster.Kube.CoreV1().PersistentVolumes().Create(ctx, volume, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the volume: %v", err)
	}

	adopted := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": api.Group + "/" + v1beta1.GroupVersion.Version,
		"kind":       "Application",
		"metadata": map[string]any{
			"name": "legacy", "namespace": harness.Namespace,
			"labels": map[string]any{
				markers.AdoptedBy:      "owner",
				markers.AdoptionReason: "pre-existing",
			},
			"annotations": map[string]any{markers.AdoptedAt: "2026-10-01T00:00:00Z"},
		},
		"spec": map[string]any{},
	}}
	cluster.Create(t, adopted)

	// An adopted extension instance outlives the kernel definitions, because
	// its own definition is not the kernel's to delete.
	cluster.InstallCRD(t, widgetsGroup, "v1", "Widget", "widgets", true)
	widget := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": widgetsGroup + "/v1",
		"kind":       "Widget",
		"metadata": map[string]any{
			"name": "legacy-widget", "namespace": harness.Namespace,
			"labels": map[string]any{
				markers.AdoptedBy:      "owner",
				markers.AdoptionReason: "pre-existing",
			},
			"annotations": map[string]any{markers.AdoptedAt: "2026-10-01T00:00:00Z"},
		},
	}}
	if _, err := cluster.Dynamic.Resource(schema.GroupVersionResource{Group: widgetsGroup, Version: "v1", Resource: "widgets"}).
		Namespace(harness.Namespace).Create(ctx, widget, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the adopted extension instance: %v", err)
	}

	snapshot := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "adoption-snapshot-legacy", Namespace: api.SystemNamespace,
			Labels: map[string]string{adoption.RecordLabel: adoption.RecordValue},
		},
		Data: map[string]string{"snapshot": `{"object":{"kind":"Application","name":"legacy"},"spec":{}}`},
	}
	if _, err := cluster.Kube.CoreV1().ConfigMaps(api.SystemNamespace).Create(ctx, snapshot, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the adoption snapshot: %v", err)
	}

	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "alice-kos-view"},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: install.ControlPlaneName},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "alice"}},
	}
	if _, err := cluster.Kube.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the user permission: %v", err)
	}

	encoded, err := extension.EncodeResiduals([]extension.Residual{{
		Object:      v1beta1.ObjectRef{Group: "widgets.example.test", Version: "v1", Kind: "Widget", Name: "kept"},
		Disposition: extension.ResidualRetained,
	}})
	if err != nil {
		t.Fatalf("encode residual manifest: %v", err)
	}
	residual := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: extension.ResidualName(types.UID("component-1")), Namespace: api.SystemNamespace,
			Labels: map[string]string{extension.ResidualLabel: "component-1"},
		},
		Data: map[string]string{extension.ResidualKey: encoded},
	}
	if _, err := cluster.Kube.CoreV1().ConfigMaps(api.SystemNamespace).Create(ctx, residual, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the extension residual manifest: %v", err)
	}

	// The volume provisioner created this volume, so it carries no marker; its
	// claim is the installation's. The pair must still be reported together.
	cache := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cache", Namespace: harness.Namespace,
			Labels: map[string]string{markers.ManagedBy: "kosd"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:  "kos-pv-cache",
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		},
	}
	if _, err := cluster.Kube.CoreV1().PersistentVolumeClaims(harness.Namespace).Create(ctx, cache, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the managed claim: %v", err)
	}
	cacheVolume := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "kos-pv-cache"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/cache"},
			},
		},
	}
	if _, err := cluster.Kube.CoreV1().PersistentVolumes().Create(ctx, cacheVolume, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the unlabelled volume: %v", err)
	}

	// Another owner's role and binding happen to carry the installation's name
	// and no marker: the uninstall must leave them alone.
	if _, err := cluster.Kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: foreignNamespace},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the other owner's namespace: %v", err)
	}
	foreignRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: install.ControlPlaneName, Namespace: foreignNamespace},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get"}}},
	}
	if _, err := cluster.Kube.RbacV1().Roles(foreignNamespace).Create(ctx, foreignRole, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the other owner's role: %v", err)
	}
	foreignBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: install.ControlPlaneName, Namespace: foreignNamespace},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: install.ControlPlaneName},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "bob"}},
	}
	if _, err := cluster.Kube.RbacV1().RoleBindings(foreignNamespace).Create(ctx, foreignBinding, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the other owner's role binding: %v", err)
	}
}

// assertRemoved checks the workloads, identities and object definitions the
// installation delivered are gone.
func assertRemoved(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	ctx := context.Background()
	if _, err := cluster.Kube.AppsV1().Deployments(api.SystemNamespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the control-plane deployment still exists: %v", err)
	}
	if _, err := cluster.Kube.CoreV1().Services(api.SystemNamespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the control-plane service still exists: %v", err)
	}
	for _, identity := range []string{install.ControlPlaneServiceAccount, install.NodeAgentName} {
		if _, err := cluster.Kube.CoreV1().ServiceAccounts(api.SystemNamespace).Get(ctx, identity, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("service account %s still exists: %v", identity, err)
		}
	}
	// The node agent is an installation workload: it must not keep running with
	// its grant and its object definitions gone.
	if _, err := cluster.Kube.AppsV1().DaemonSets(api.SystemNamespace).Get(ctx, install.NodeAgentName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the node-agent daemonset still exists: %v", err)
	}
	if _, err := cluster.Kube.RbacV1().ClusterRoles().Get(ctx, install.NodeAgentName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the node-agent cluster role still exists: %v", err)
	}
	if _, err := cluster.Kube.RbacV1().ClusterRoleBindings().Get(ctx, install.NodeAgentName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the node-agent cluster role binding still exists: %v", err)
	}
	// The application namespace outlives the uninstall, so the authorization
	// objects the installation wrote there are checked separately.
	if _, err := cluster.Kube.RbacV1().Roles(harness.Namespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the role in the application namespace still exists: %v", err)
	}
	if _, err := cluster.Kube.RbacV1().RoleBindings(harness.Namespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the role binding in the application namespace still exists: %v", err)
	}
	definitions, err := cluster.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list object definitions: %v", err)
	}
	for _, definition := range definitions.Items {
		if definition.Spec.Group == api.Group {
			t.Fatalf("object definition %s survived the uninstall", definition.Name)
		}
	}
}

// assertNothingDeleting pins the end state of C-54: no KOS object is left
// waiting for its finalizer. The check lists the objects an uninstall removes
// and fails on a deletion timestamp that did not finish.
func assertNothingDeleting(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	ctx := context.Background()
	deleting := func(kind, namespace, name string, timestamp *metav1.Time) {
		if timestamp != nil {
			t.Fatalf("%s %s/%s is stuck in deletion since %s", kind, namespace, name, timestamp)
		}
	}
	deployments, err := cluster.Kube.AppsV1().Deployments("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list deployments: %v", err)
	}
	for index := range deployments.Items {
		object := &deployments.Items[index]
		deleting("Deployment", object.Namespace, object.Name, object.DeletionTimestamp)
	}
	agents, err := cluster.Kube.AppsV1().DaemonSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list daemonsets: %v", err)
	}
	for index := range agents.Items {
		object := &agents.Items[index]
		deleting("DaemonSet", object.Namespace, object.Name, object.DeletionTimestamp)
	}
	accounts, err := cluster.Kube.CoreV1().ServiceAccounts("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list service accounts: %v", err)
	}
	for index := range accounts.Items {
		object := &accounts.Items[index]
		deleting("ServiceAccount", object.Namespace, object.Name, object.DeletionTimestamp)
	}
	definitions, err := cluster.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list object definitions: %v", err)
	}
	for index := range definitions.Items {
		definition := &definitions.Items[index]
		deleting("CustomResourceDefinition", "", definition.Name, definition.DeletionTimestamp)
	}
}

// fileDigest is the sha256 of one file, used to prove the database is untouched.
func fileDigest(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

// assertForeignNamesUntouched pins the ownership rule: an object the
// installation did not label is not the installation's, even when it carries
// the installation's name.
func assertForeignNamesUntouched(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	ctx := context.Background()
	if _, err := cluster.Kube.RbacV1().Roles(foreignNamespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the unlabelled role of another owner was deleted: %v", err)
	}
	if _, err := cluster.Kube.RbacV1().RoleBindings(foreignNamespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the unlabelled role binding of another owner was deleted: %v", err)
	}
}

const widgetsGroup = "widgets.example.test"

// foreignNamespace holds another owner's objects, some of them named like the
// installation's.
const foreignNamespace = "outside-kos"

// declaration is the installed extension whose adopted instance the leftover
// list must find; the classes are derived from it, not configured by hand.
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
		},
	}
}

// seedManagedClaim places one KOS-managed claim and its volume, so the removal
// path has data to dispose.
func seedManagedClaim(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	ctx := context.Background()
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data", Namespace: harness.Namespace,
			Labels: map[string]string{markers.ManagedBy: "kosd"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:  "kos-pv-data",
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		},
	}
	if _, err := cluster.Kube.CoreV1().PersistentVolumeClaims(harness.Namespace).Create(ctx, claim, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the managed claim: %v", err)
	}
	volume := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "kos-pv-data",
			Labels: map[string]string{markers.ManagedBy: "kosd"},
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/data"},
			},
		},
	}
	if _, err := cluster.Kube.CoreV1().PersistentVolumes().Create(ctx, volume, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the managed volume: %v", err)
	}
}

// hasLeftover reports one disposition among a category's entries.
func hasLeftover(entries []install.Leftover, disposition string) bool {
	for _, entry := range entries {
		if entry.Disposition == disposition {
			return true
		}
	}
	return false
}
