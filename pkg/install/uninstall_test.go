package install

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/pkg/adoption"
	"github.com/tsic404/kosd/pkg/extension"
	"github.com/tsic404/kosd/pkg/markers"
)

var (
	daemonSetGVR  = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"}
	deploymentGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	serviceGVR    = schema.GroupVersionResource{Version: "v1", Resource: "services"}
	secretGVR     = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}

	// persistentGVRs are the lists the data collection reads.
	persistentGVRs = map[schema.GroupVersionResource]string{
		claimResource:  "PersistentVolumeClaimList",
		volumeResource: "PersistentVolumeList",
	}
	// workloadGVRs are the lists the control-plane stop reads and deletes.
	workloadGVRs = map[schema.GroupVersionResource]string{
		daemonSetGVR:           "DaemonSetList",
		deploymentGVR:          "DeploymentList",
		serviceGVR:             "ServiceList",
		secretGVR:              "SecretList",
		serviceAccountResource: "ServiceAccountList",
	}
)

// TestUninstallRefusesUnreadableExtensionClasses pins C-54: a leftover list
// that cannot know the installed extension classes fails instead of silently
// dropping the adopted extension instances; an explicitly configured list is
// the fallback.
func TestUninstallRefusesUnreadableExtensionClasses(t *testing.T) {
	resource := schema.GroupVersionResource{Group: api.Group, Version: "v1beta1", Resource: "systemcomponents"}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{resource: "SystemComponentList"},
	)
	client.PrependReactor("list", "systemcomponents", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("declaration store unavailable")
	})
	uninstaller := &Uninstaller{config: UninstallConfig{
		Dynamic:   client,
		Artifacts: extension.MemoryArtifacts{},
	}}
	if _, err := uninstaller.extensionClasses(context.Background()); err == nil {
		t.Fatal("an unreadable declaration store was accepted without a fallback")
	}

	uninstaller.config.ExtensionClasses = []schema.GroupKind{{Group: "widgets.example.test", Kind: "Widget"}}
	classes, err := uninstaller.extensionClasses(context.Background())
	if err != nil {
		t.Fatalf("configured classes were not used as the fallback: %v", err)
	}
	if len(classes) != 1 || classes[0].Kind != "Widget" {
		t.Fatalf("classes = %v, want the configured fallback", classes)
	}
}

// TestUninstallStampsTheFinishTime pins C-54's manifest time: the manifest
// names when the uninstall stopped, not when it began. The disposal moves the
// clock the uninstaller reads, so a value taken before the work is visible.
func TestUninstallStampsTheFinishTime(t *testing.T) {
	start := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	clock := start
	uninstaller := newUninstallerUnderTest(t, UninstallConfig{
		RetainData: false,
		Dispose: disposerFunc(func(context.Context) error {
			clock = clock.Add(time.Hour)
			return nil
		}),
		Now: func() time.Time { return clock },
	})
	manifest, err := uninstaller.Uninstall(context.Background())
	if err != nil {
		t.Fatalf("uninstall: %v", err)
	}
	if want := start.Add(time.Hour); !manifest.GeneratedAt.Equal(want) {
		t.Fatalf("GeneratedAt = %s, want the finish time %s", manifest.GeneratedAt, want)
	}
}

// TestUninstallKeepsCollectedEntriesOnFailure pins the failed-run contract: the
// manifest reports what the run had found before it stopped and still carries a
// time, so an operator reads a partial record instead of an empty one.
func TestUninstallKeepsCollectedEntriesOnFailure(t *testing.T) {
	kube := kubernetesfake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "adoption-snapshot-legacy", Namespace: DefaultNamespace,
			Labels: map[string]string{adoption.RecordLabel: adoption.RecordValue},
		},
	})
	extensions := apiextensionsfake.NewSimpleClientset()
	extensions.PrependReactor("list", "customresourcedefinitions", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("definition store unavailable")
	})
	uninstaller := newUninstallerUnderTest(t, UninstallConfig{
		RetainData:    true,
		Kube:          kube,
		APIExtensions: extensions,
	})
	manifest, err := uninstaller.Uninstall(context.Background())
	if err == nil {
		t.Fatal("an unreadable definition store was reported as a finished uninstall")
	}
	if !namesLeftover(manifest.Entries, LeftoverAdoptionSnapshot, "adoption-snapshot-legacy") {
		t.Fatalf("entries = %+v, want the snapshot collected before the failure", manifest.Entries)
	}
	if manifest.GeneratedAt.IsZero() {
		t.Fatal("a failed run carries no time")
	}
}

// TestUninstallKeepsCollectedEntriesWhenThePruneReadFails pins the last step of
// the manifest: an unreadable leftover while pruning instances must not wipe
// what the collection phase already found, or the CLI would print an empty
// "incomplete" manifest exactly when an operator needs the record.
func TestUninstallKeepsCollectedEntriesWhenThePruneReadFails(t *testing.T) {
	application := schema.GroupVersionResource{Group: api.Group, Version: "v1beta1", Resource: "applications"}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			application:    "ApplicationList",
			claimResource:  "PersistentVolumeClaimList",
			volumeResource: "PersistentVolumeList",
		},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": api.Group + "/v1beta1",
			"kind":       "Application",
			"metadata": map[string]any{
				"name": "legacy", "namespace": DefaultNamespace,
				"labels": map[string]any{
					markers.AdoptedBy:      "owner",
					markers.AdoptionReason: "pre-existing",
				},
			},
		}},
	)
	client.PrependReactor("get", "applications", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: api.Group, Resource: "applications"}, "legacy", errors.New("denied"))
	})
	uninstaller := newUninstallerUnderTest(t, UninstallConfig{
		RetainData: true,
		Dynamic:    client,
		Discovery: stubDiscovery{lists: []*metav1.APIResourceList{{
			GroupVersion: api.Group + "/v1beta1",
			APIResources: []metav1.APIResource{
				{Name: "applications", Kind: "Application", Namespaced: true, Verbs: []string{"list"}},
			},
		}}},
	})
	manifest, err := uninstaller.Uninstall(context.Background())
	if err == nil {
		t.Fatal("an unreadable leftover was reported as a finished uninstall")
	}
	if !namesLeftover(manifest.Entries, LeftoverInstance, "legacy") {
		t.Fatalf("entries = %+v, want the instance collected before the prune failed", manifest.Entries)
	}
	if manifest.GeneratedAt.IsZero() {
		t.Fatal("a failed run carries no time")
	}
}

// TestUninstallRemovesEveryIdentityOfTheExternalForm pins the ownership of the
// two clusters: the control-plane workload, its identity and its credential go
// from the control-plane cluster; the node agent and its identity go from the
// managed cluster; and the control-plane identity Render writes into the
// managed cluster goes too.
func TestUninstallRemovesEveryIdentityOfTheExternalForm(t *testing.T) {
	managed := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), workloadGVRs,
		namespacedObject("v1", "ServiceAccount", ControlPlaneServiceAccount),
		namespacedObject("v1", "ServiceAccount", NodeAgentName),
		namespacedObject("apps/v1", "DaemonSet", NodeAgentName),
	)
	controlPlane := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), workloadGVRs,
		namespacedObject("v1", "ServiceAccount", ControlPlaneServiceAccount),
		namespacedObject("apps/v1", "Deployment", ControlPlaneName),
		namespacedObject("v1", "Service", ControlPlaneName),
		namespacedObject("v1", "Secret", ManagedCredentialSecret),
	)
	uninstaller := newUninstallerUnderTest(t, UninstallConfig{
		Mode:       ModeExternal,
		RetainData: true,
		Dynamic:    managed,
		ControlPlaneCluster: &Cluster{
			Dynamic:       controlPlane,
			Kube:          kubernetesfake.NewSimpleClientset(),
			APIExtensions: apiextensionsfake.NewSimpleClientset(),
		},
	})
	ctx := context.Background()
	if err := uninstaller.stopControlPlane(ctx); err != nil {
		t.Fatalf("stop the control plane: %v", err)
	}
	managedObjects := []struct {
		resource schema.GroupVersionResource
		name     string
	}{
		{serviceAccountResource, ControlPlaneServiceAccount},
		{serviceAccountResource, NodeAgentName},
		{daemonSetGVR, NodeAgentName},
	}
	for _, object := range managedObjects {
		_, err := managed.Resource(object.resource).Namespace(DefaultNamespace).Get(ctx, object.name, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			t.Fatalf("the managed cluster still holds %s %s: %v", object.resource.Resource, object.name, err)
		}
	}
	controlPlaneObjects := []struct {
		resource schema.GroupVersionResource
		name     string
	}{
		{serviceAccountResource, ControlPlaneServiceAccount},
		{deploymentGVR, ControlPlaneName},
		{serviceGVR, ControlPlaneName},
		{secretGVR, ManagedCredentialSecret},
	}
	for _, object := range controlPlaneObjects {
		_, err := controlPlane.Resource(object.resource).Namespace(DefaultNamespace).Get(ctx, object.name, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			t.Fatalf("the control-plane cluster still holds %s %s: %v", object.resource.Resource, object.name, err)
		}
	}
}

// TestDeletePermissionsKeepsUnlabelledNamesakes pins the ownership rule: the
// installer labels what it writes, so a same-named object without the marker
// belongs to someone else and must survive the uninstall.
func TestDeletePermissionsKeepsUnlabelledNamesakes(t *testing.T) {
	managed := map[string]string{markers.ManagedBy: "kosd"}
	kube := kubernetesfake.NewSimpleClientset(
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: ControlPlaneName}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: ControlPlaneName, Namespace: "other"}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: ControlPlaneName, Namespace: "other"}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: NodeAgentName, Labels: managed}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: NodeAgentName, Labels: managed}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "kos-reader", Namespace: "other", Labels: managed}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "kos-reader", Namespace: "other", Labels: managed}},
	)
	uninstaller := newUninstallerUnderTest(t, UninstallConfig{RetainData: true, Kube: kube})
	ctx := context.Background()
	if err := uninstaller.deletePermissions(ctx); err != nil {
		t.Fatalf("delete permissions: %v", err)
	}
	if _, err := kube.RbacV1().ClusterRoles().Get(ctx, ControlPlaneName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the unlabelled cluster role was deleted: %v", err)
	}
	if _, err := kube.RbacV1().Roles("other").Get(ctx, ControlPlaneName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the unlabelled role was deleted: %v", err)
	}
	if _, err := kube.RbacV1().RoleBindings("other").Get(ctx, ControlPlaneName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the unlabelled role binding was deleted: %v", err)
	}
	for _, missing := range []struct {
		resource, namespace, name string
	}{
		{"clusterroles", "", NodeAgentName},
		{"clusterrolebindings", "", NodeAgentName},
		{"roles", "other", "kos-reader"},
		{"rolebindings", "other", "kos-reader"},
	} {
		if err := deleted(kube, ctx, missing.resource, missing.namespace, missing.name); err != nil {
			t.Fatal(err)
		}
	}
}

// deleted reports whether the named RBAC object is gone.
func deleted(kube *kubernetesfake.Clientset, ctx context.Context, resource, namespace, name string) error {
	var err error
	switch resource {
	case "clusterroles":
		_, err = kube.RbacV1().ClusterRoles().Get(ctx, name, metav1.GetOptions{})
	case "clusterrolebindings":
		_, err = kube.RbacV1().ClusterRoleBindings().Get(ctx, name, metav1.GetOptions{})
	case "roles":
		_, err = kube.RbacV1().Roles(namespace).Get(ctx, name, metav1.GetOptions{})
	case "rolebindings":
		_, err = kube.RbacV1().RoleBindings(namespace).Get(ctx, name, metav1.GetOptions{})
	}
	if !apierrors.IsNotFound(err) {
		return errors.New(resource + " " + name + " survived the labelled cleanup")
	}
	return nil
}

// disposerFunc adapts a function to DataDisposer.
type disposerFunc func(context.Context) error

// Dispose implements DataDisposer.
func (f disposerFunc) Dispose(ctx context.Context) error { return f(ctx) }

// newUninstallerUnderTest builds an uninstaller over the given piece and fills
// every client the case leaves out with a fake that holds nothing.
func newUninstallerUnderTest(t *testing.T, config UninstallConfig) *Uninstaller {
	t.Helper()
	if config.Dynamic == nil {
		config.Dynamic = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), persistentGVRs)
	}
	if config.Kube == nil {
		config.Kube = kubernetesfake.NewSimpleClientset()
	}
	if config.APIExtensions == nil {
		config.APIExtensions = apiextensionsfake.NewSimpleClientset()
	}
	if config.Discovery == nil {
		config.Discovery = &discoveryfake.FakeDiscovery{Fake: &k8stesting.Fake{}}
	}
	uninstaller, err := NewUninstaller(config)
	if err != nil {
		t.Fatalf("build uninstaller: %v", err)
	}
	return uninstaller
}

// namespacedObject is one installation object as the cluster stores it.
func namespacedObject(apiVersion, kind, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata":   map[string]any{"name": name, "namespace": DefaultNamespace},
	}}
}

// namesLeftover reports one entry of one category.
func namesLeftover(entries []Leftover, category, name string) bool {
	for _, entry := range entries {
		if entry.Category == category && entry.Object.Name == name {
			return true
		}
	}
	return false
}
