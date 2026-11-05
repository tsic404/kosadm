package install

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

var (
	persistentVolumeGVR = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}
	configMapGVR        = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
)

// TestRescueListReadsTheScopeDiscoveryGives pins C-54's query path: a
// cluster-scoped resource ignores the namespace instead of asking for a path
// the server answers with 404, and a resource the server does not serve fails
// instead of reading as an empty result.
func TestRescueListReadsTheScopeDiscoveryGives(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			persistentVolumeGVR: "PersistentVolumeList",
			configMapGVR:        "ConfigMapList",
		},
		rescueObject("v1", "PersistentVolume", "", "data"),
		rescueObject("v1", "ConfigMap", "apps", "settings"),
	)
	finder := &discoveryfake.FakeDiscovery{
		Fake: &k8stesting.Fake{Resources: []*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "persistentvolumes", Kind: "PersistentVolume", Namespaced: false, Verbs: []string{"list"}},
				{Name: "configmaps", Kind: "ConfigMap", Namespaced: true, Verbs: []string{"list"}},
			},
		}}},
	}
	rescue, err := NewRescue(client, finder)
	if err != nil {
		t.Fatalf("build rescue reader: %v", err)
	}
	ctx := context.Background()

	volumes, err := rescue.List(ctx, persistentVolumeGVR, "default")
	if err != nil {
		t.Fatalf("read a cluster-scoped resource: %v", err)
	}
	if len(volumes) != 1 || volumes[0].GetName() != "data" {
		t.Fatalf("cluster-scoped read = %d object(s), want the volume", len(volumes))
	}
	// The namespace narrows a namespaced resource.
	settings, err := rescue.List(ctx, configMapGVR, "apps")
	if err != nil {
		t.Fatalf("read a namespaced resource: %v", err)
	}
	if len(settings) != 1 || settings[0].GetName() != "settings" {
		t.Fatalf("namespaced read = %d object(s), want the config map", len(settings))
	}
	missing, err := rescue.List(ctx, configMapGVR, "other")
	if err != nil {
		t.Fatalf("read an empty namespace: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("read of another namespace = %d object(s), want none", len(missing))
	}
	if _, err := rescue.List(ctx, schema.GroupVersionResource{Version: "v1", Resource: "widgets"}, ""); err == nil {
		t.Fatal("a resource the server does not serve read as an empty result")
	}
}

// TestRescueNativeReportsWhatItCouldNotRead pins C-54's rescue behaviour on a
// damaged cluster: one unreadable resource and one unreachable group do not
// hide the resources that answered; both gaps are reported.
func TestRescueNativeReportsWhatItCouldNotRead(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			persistentVolumeGVR: "PersistentVolumeList",
			configMapGVR:        "ConfigMapList",
		},
		rescueObject("v1", "PersistentVolume", "", "data"),
	)
	client.PrependReactor("list", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "", errors.New("denied"))
	})
	finder := stubDiscovery{
		lists: []*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "configmaps", Kind: "ConfigMap", Namespaced: true, Verbs: []string{"list"}},
				{Name: "persistentvolumes", Kind: "PersistentVolume", Namespaced: false, Verbs: []string{"list"}},
			},
		}},
		err: &discovery.ErrGroupDiscoveryFailed{Groups: map[schema.GroupVersion]error{
			{Group: "metrics.k8s.io", Version: "v1beta1"}: errors.New("backend down"),
		}},
	}
	rescue, err := NewRescue(client, finder)
	if err != nil {
		t.Fatalf("build rescue reader: %v", err)
	}
	report, err := rescue.Native(context.Background())
	if err != nil {
		t.Fatalf("a partial discovery failed the whole read: %v", err)
	}
	if len(report.Views) != 1 || report.Views[0].Resource != persistentVolumeGVR || len(report.Views[0].Objects) != 1 {
		t.Fatalf("views = %+v, want the readable volume", report.Views)
	}
	if len(report.Failures) != 2 {
		t.Fatalf("failures = %+v, want the refused resource and the unreachable group", report.Failures)
	}
	var refused, unreachable bool
	for _, failure := range report.Failures {
		if failure.Resource == configMapGVR {
			refused = true
		}
		if failure.Resource.Group == "metrics.k8s.io" && failure.Resource.Resource == "" {
			unreachable = true
		}
	}
	if !refused || !unreachable {
		t.Fatalf("failures = %+v, want both the refused resource and the unreachable group", report.Failures)
	}
}

// TestRescueNativeFailsWhenDiscoveryIsUnreadable keeps the one case rescue
// cannot work around an error: without any discovery answer there is nothing to
// read.
func TestRescueNativeFailsWhenDiscoveryIsUnreadable(t *testing.T) {
	rescue, err := NewRescue(dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()), stubDiscovery{
		err: errors.New("apiserver unreachable"),
	})
	if err != nil {
		t.Fatalf("build rescue reader: %v", err)
	}
	if _, err := rescue.Native(context.Background()); err == nil {
		t.Fatal("an unreadable discovery was reported as a successful read")
	}
}

// stubDiscovery answers the one discovery call the rescue path makes.
type stubDiscovery struct {
	discovery.DiscoveryInterface
	lists []*metav1.APIResourceList
	err   error
}

// ServerPreferredResources returns the case's fixed answer.
func (d stubDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return d.lists, d.err
}

// rescueObject is one native object as the API server stores it.
func rescueObject(apiVersion, kind, namespace, name string) *unstructured.Unstructured {
	metadata := map[string]any{"name": name}
	if namespace != "" {
		metadata["namespace"] = namespace
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata":   metadata,
	}}
}
