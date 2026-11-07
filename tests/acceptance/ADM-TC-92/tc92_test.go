// Package admtc92 verifies the rescue mode: after the control plane stops, rescue
// reads native resources directly and writes nothing — not to the cluster and
// not to the control-plane database (ADM-AC-46, ADM-TC-92).
package admtc92

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
	"github.com/tsic404/kosd/api"
)

func TestTC92RescueIsReadOnly(t *testing.T) {
	cluster := harness.StartAudited(t)
	installerIdentity := harness.InstallerIdentity()
	cluster.GrantClusterAdmin(t, installerIdentity)
	identityStore := harness.OpenIdentity(t, nil, nil)
	clients := cluster.InstallClients(t, installerIdentity)
	ctx := context.Background()

	runner, err := install.New(install.Config{
		Plan:                  harness.InstallerPlan(nil),
		Dynamic:               clients.Dynamic,
		Kube:                  clients.Kube,
		APIExtensions:         clients.APIExtensions,
		Definitions:           harness.Definitions(t),
		Accounts:              identityStore.Service,
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

	// Stop the control plane: the rescue reader takes over from a dead process.
	if err := cluster.Kube.AppsV1().Deployments(api.SystemNamespace).Delete(ctx, install.ControlPlaneName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("stop the control plane: %v", err)
	}
	harness.Eventually(t, 30*time.Second, "the control-plane deployment to stop", func() bool {
		_, err := cluster.Kube.AppsV1().Deployments(api.SystemNamespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	})

	// The query path must ask for a cluster-scoped resource at its own scope: a
	// namespace in front of it answers 404, which used to read as zero objects.
	volume := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "rescue-volume"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/rescue"},
			},
		},
	}
	if _, err := cluster.Kube.CoreV1().PersistentVolumes().Create(ctx, volume, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the cluster-scoped object: %v", err)
	}

	snapshotter := &install.Snapshotter{Dynamic: cluster.Dynamic, Discovery: cluster.Kube.Discovery()}
	before, err := snapshotter.Take(ctx)
	if err != nil {
		t.Fatalf("snapshot before rescue: %v", err)
	}
	databaseBefore := fileDigest(t, identityStore.DatabasePath)
	rescueStarted := time.Now()

	rescue, err := install.NewRescue(cluster.Dynamic, cluster.Kube.Discovery())
	if err != nil {
		t.Fatalf("build rescue reader: %v", err)
	}
	report, err := rescue.Native(ctx)
	if err != nil {
		t.Fatalf("rescue reads native resources: %v", err)
	}
	if len(report.Failures) != 0 {
		t.Fatalf("rescue could not read %v", report.Failures)
	}
	if !fetchedSomething(report.Views) {
		t.Fatal("rescue returned no native objects")
	}
	volumes, err := rescue.List(ctx, schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}, harness.Namespace)
	if err != nil {
		t.Fatalf("rescue reads a cluster-scoped resource: %v", err)
	}
	if len(volumes) != 1 || volumes[0].GetName() != "rescue-volume" {
		t.Fatalf("cluster-scoped read = %d object(s), want the persistent volume", len(volumes))
	}
	// A resource name the server does not serve is an error, not an empty
	// cluster: silently reporting zero objects would hide a typo from an operator
	// who is already in trouble.
	if _, err := rescue.List(ctx, schema.GroupVersionResource{Version: "v1", Resource: "widgets"}, ""); err == nil {
		t.Fatal("an unserved resource read as a successful empty list")
	}

	after, err := snapshotter.Take(ctx)
	if err != nil {
		t.Fatalf("snapshot after rescue: %v", err)
	}
	if !reflect.DeepEqual(before.Objects, after.Objects) {
		t.Fatal("rescue changed a cluster object")
	}
	if digest := fileDigest(t, identityStore.DatabasePath); digest != databaseBefore {
		t.Fatal("rescue changed the control-plane database")
	}
	events, err := cluster.AuditSource(t).Events(ctx, rescueStarted, time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("read the audit log: %v", err)
	}
	for _, event := range events {
		if event.User == installerIdentity || event.User == harness.ControlPlaneIdentity() {
			t.Fatalf("rescue produced the write %s %s by %q", event.Verb, event.Object.Name, event.User)
		}
	}
}

// fetchedSomething reports whether rescue read at least one native object.
func fetchedSomething(views []install.NativeView) bool {
	for _, view := range views {
		if len(view.Objects) > 0 {
			return true
		}
	}
	return false
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
