package install

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

// The controller chain the fixtures carry: the workload's UID, and the UID of
// the ReplicaSet the workload's pods name as their controller.
const (
	workloadUID      = types.UID("uid-" + ControlPlaneName)
	workloadSetUID   = types.UID("uid-" + ControlPlaneName + "-rs")
	canaryWorkload   = ControlPlaneName + "-canary"
	canarySetUID     = types.UID("uid-" + canaryWorkload + "-rs")
	canarySetWorkUID = types.UID("uid-" + canaryWorkload)
)

// TestNodeForHostFindsTheNodeThatIsThisHost pins the identification the install
// run needs when the operator names no node: the file was written on this host,
// so the workload has to land on the node that is this host. A node reports
// itself by its name or by its hostname label, and either may carry the domain
// the host does not, so both forms have to match.
func TestNodeForHostFindsTheNodeThatIsThisHost(t *testing.T) {
	cases := map[string]struct {
		node     *corev1.Node
		hostname string
	}{
		"node name": {
			node:     &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-a"}},
			hostname: "kos-a",
		},
		"hostname label": {
			node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name:   "kos-a",
				Labels: map[string]string{corev1.LabelHostname: "kos-a"},
			}},
			hostname: "kos-a",
		},
		"domain on the node": {
			node:     &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-a.example.test"}},
			hostname: "kos-a",
		},
		"domain on the host": {
			node:     &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-a"}},
			hostname: "kos-a.example.test",
		},
		"both qualified": {
			node:     &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-a.example.test"}},
			hostname: "kos-a.example.test",
		},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			kube := fake.NewSimpleClientset(testCase.node)
			node, found, err := NodeForHost(context.Background(), kube, testCase.hostname)
			if err != nil {
				t.Fatalf("identify this host: %v", err)
			}
			if !found || node != testCase.node.Name {
				t.Fatalf("node = %q found = %t, want %q", node, found, testCase.node.Name)
			}
		})
	}
}

// TestNodeForHostRefusesANodeInAnotherDomain pins the other side of the name
// rule: two fully qualified names that differ are two machines even when their
// short names match. Dropping the domain would make worker.site-a.example and
// worker.site-b.example one host, and the workload would read the other
// machine's filesystem at the database path. The hostname label is the name of
// the machine, so a node name that happens to share the short name cannot
// override it.
func TestNodeForHostRefusesANodeInAnotherDomain(t *testing.T) {
	cases := map[string]*corev1.Node{
		"node name": {
			ObjectMeta: metav1.ObjectMeta{Name: "worker.site-b.example"},
		},
		"hostname label": {
			ObjectMeta: metav1.ObjectMeta{
				Name:   "worker",
				Labels: map[string]string{corev1.LabelHostname: "worker.site-b.example"},
			},
		},
	}
	for name, node := range cases {
		t.Run(name, func(t *testing.T) {
			kube := fake.NewSimpleClientset(node)
			resolved, found, err := NodeForHost(context.Background(), kube, "worker.site-a.example")
			if err != nil {
				t.Fatalf("identify this host: %v", err)
			}
			if found || resolved != "" {
				t.Fatalf("node = %q found = %t, want no node that names another machine", resolved, found)
			}
		})
	}
}

// TestNodeForHostReportsWhenNoNodeIsThisHost pins the other outcome: a host the
// cluster does not know has no node to pin, and the install run has to refuse
// rather than render a workload that reads an empty directory on whatever node
// it lands on.
func TestNodeForHostReportsWhenNoNodeIsThisHost(t *testing.T) {
	kube := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-b"}},
	)
	node, found, err := NodeForHost(context.Background(), kube, "kos-c")
	if err != nil {
		t.Fatalf("identify this host: %v", err)
	}
	if found || node != "" {
		t.Fatalf("node = %q found = %t, want no node", node, found)
	}
}

// TestNodeForHostRefusesAnAmbiguousHost pins the third outcome: two nodes that
// both report the host name are two different filesystems, and picking one
// would be a guess about which holds the file. The run refuses instead.
func TestNodeForHostRefusesAnAmbiguousHost(t *testing.T) {
	kube := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name:   "kos-b",
			Labels: map[string]string{corev1.LabelHostname: "kos-a"},
		}},
	)
	if _, _, err := NodeForHost(context.Background(), kube, "kos-a"); err == nil {
		t.Fatal("two nodes reporting one host name were resolved to one of them")
	}
}

// TestNodeForHostRefusesAnEmptyHostName pins that a host which reports no name
// cannot be matched against a node by accident.
func TestNodeForHostRefusesAnEmptyHostName(t *testing.T) {
	kube := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-a"}})
	if _, _, err := NodeForHost(context.Background(), kube, "  "); err == nil {
		t.Fatal("an empty host name was accepted")
	}
}

// TestSeededDatabaseResolvesTheFileTheWorkloadServes pins the delivery the
// install run hands the plan: the absolute path of the database it opened and
// the local account that owns it. A relative path resolves against the working
// directory, and anything that is not a regular file is refused, because the
// workload would otherwise create a fresh database there and serve accounts the
// installation never wrote.
func TestSeededDatabaseResolvesTheFileTheWorkloadServes(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "kos.db")
	if err := os.WriteFile(path, []byte("seeded"), 0o600); err != nil {
		t.Fatalf("write the database: %v", err)
	}
	delivery, err := SeededDatabase(path, "node-a")
	if err != nil {
		t.Fatalf("resolve the database: %v", err)
	}
	if delivery.Path != path {
		t.Fatalf("path = %q, want %q", delivery.Path, path)
	}
	if delivery.Node != "node-a" {
		t.Fatalf("node = %q, want node-a", delivery.Node)
	}
	if delivery.Owner.UID != int64(os.Getuid()) || delivery.Owner.GID != int64(os.Getgid()) {
		t.Fatalf("owner = %+v, want uid/gid %d/%d", delivery.Owner, os.Getuid(), os.Getgid())
	}

	working, err := os.Getwd()
	if err != nil {
		t.Fatalf("read the working directory: %v", err)
	}
	relative, err := filepath.Rel(working, path)
	if err != nil {
		t.Fatalf("make the path relative: %v", err)
	}
	resolved, err := SeededDatabase(relative, "node-a")
	if err != nil {
		t.Fatalf("resolve a relative database path: %v", err)
	}
	if resolved.Path != path {
		t.Fatalf("relative path resolved to %q, want %q", resolved.Path, path)
	}
}

// TestSeededDatabaseResolvesALinkToItsTarget pins the link boundary: the
// workload mounts the directory of the path it is given, so a link delivered as
// itself would leave the target missing inside the container and the control
// plane could not open the file the installation wrote. The delivery must name
// the resolved target, and the target's own account.
func TestSeededDatabaseResolvesALinkToItsTarget(t *testing.T) {
	target := t.TempDir()
	path := filepath.Join(target, "kos.db")
	if err := os.WriteFile(path, []byte("seeded"), 0o600); err != nil {
		t.Fatalf("write the database: %v", err)
	}
	// The link lives in a different directory from its target, which is the
	// shape that breaks a delivery mounting the link's own parent.
	link := filepath.Join(t.TempDir(), "kos.db")
	if err := os.Symlink(path, link); err != nil {
		t.Fatalf("link the database: %v", err)
	}
	delivery, err := SeededDatabase(link, "node-a")
	if err != nil {
		t.Fatalf("resolve a linked database: %v", err)
	}
	if delivery.Path != path {
		t.Fatalf("path = %q, want the link's target %q", delivery.Path, path)
	}
}

// TestSeededDatabaseRefusesALinkIntoThePackageDirectory pins the other link
// boundary: the string prefix check only sees the link's own path, so a link
// whose target sits in the read-only package directory would pass it and be
// mounted read-only, where the control plane cannot write its journal. The
// resolved target has to be judged, not the link.
func TestSeededDatabaseRefusesALinkIntoThePackageDirectory(t *testing.T) {
	packages := t.TempDir()
	target := filepath.Join(packages, "kos.db")
	if err := os.WriteFile(target, []byte("seeded"), 0o600); err != nil {
		t.Fatalf("write the database: %v", err)
	}
	link := filepath.Join(t.TempDir(), "kos.db")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("link the database: %v", err)
	}
	if _, err := resolveSeededDatabase(link, "node-a", packages); err == nil {
		t.Fatal("a link into the read-only package directory was accepted")
	}
}

// TestSeededDatabaseRefusesWhatTheWorkloadCannotServe pins the refusal: a path
// that names no file would leave the control plane on a database it creates
// itself, which is the credential that cannot sign in.
func TestSeededDatabaseRefusesWhatTheWorkloadCannotServe(t *testing.T) {
	directory := t.TempDir()
	for name, path := range map[string]string{
		"missing file": filepath.Join(directory, "absent.db"),
		"directory":    directory,
	} {
		if _, err := SeededDatabase(path, "node-a"); err == nil {
			t.Fatalf("%s: %q was accepted as a database delivery", name, path)
		}
	}
}

// TestSeededDatabaseAllowsADeliveryWithoutANode pins the compatible input: a
// delivery that names no node renders an unpinned workload, which is the
// operator's choice on a single node where every pod lands on the same
// filesystem.
func TestSeededDatabaseAllowsADeliveryWithoutANode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kos.db")
	if err := os.WriteFile(path, []byte("seeded"), 0o600); err != nil {
		t.Fatalf("write the database: %v", err)
	}
	delivery, err := SeededDatabase(path, "")
	if err != nil {
		t.Fatalf("a database delivery without a node was refused: %v", err)
	}
	if delivery.Node != "" {
		t.Fatalf("node = %q, want empty", delivery.Node)
	}
}

// TestInheritDatabaseDeliveryKeepsTheServedFile pins the upgrade side of the
// delivery: an upgrade names a release, not the installation's inputs, so the
// plan it builds carries no database. Rendering that plan would point the new
// pod at the image's own empty database and lose every account, the first Owner
// included. The served delivery is what the rewrite has to keep (C-54, C-63).
func TestInheritDatabaseDeliveryKeepsTheServedFile(t *testing.T) {
	served := DatabaseDelivery{
		Path:  "/var/lib/kosd/kos.db",
		Node:  "node-a",
		Owner: DatabaseIdentity{UID: 1000, GID: 1000},
	}
	inherited := InheritDatabaseDelivery(Plan{Mode: ModeExisting, Owner: "owner"}, served, true)
	if inherited.Database == nil {
		t.Fatal("an upgrade that named no database lost the served one")
	}
	if *inherited.Database != served {
		t.Fatalf("inherited delivery = %+v, want the served %+v", *inherited.Database, served)
	}
}

// TestInheritDatabaseDeliveryKeepsTheNodeOfANamedFile pins the mixed case: an
// operator who names the database but not the node still gets the node the file
// is on. The hostPath reads one node's filesystem, so dropping the pin would
// let the replacement start where the file is not.
func TestInheritDatabaseDeliveryKeepsTheNodeOfANamedFile(t *testing.T) {
	named := DatabaseDelivery{Path: "/var/lib/kosd/kos.db", Owner: DatabaseIdentity{UID: 1000, GID: 1000}}
	served := DatabaseDelivery{Path: "/var/lib/kosd/kos.db", Node: "node-a", Owner: DatabaseIdentity{UID: 1000, GID: 1000}}
	inherited := InheritDatabaseDelivery(Plan{Mode: ModeExisting, Owner: "owner", Database: &named}, served, true)
	if inherited.Database.Node != "node-a" {
		t.Fatalf("inherited node = %q, want node-a: the file is only reachable from that node", inherited.Database.Node)
	}
	if inherited.Database.Path != named.Path {
		t.Fatalf("inherited path = %q, want the named %q", inherited.Database.Path, named.Path)
	}
}

// TestInheritDatabaseDeliveryLeavesAnExplicitDeliveryAlone pins that the
// inheritance never overrides what the operator named: an upgrade that moves the
// installation to another database has to land on the one it named.
func TestInheritDatabaseDeliveryLeavesAnExplicitDeliveryAlone(t *testing.T) {
	named := DatabaseDelivery{Path: "/srv/kos/kos.db", Node: "node-b", Owner: DatabaseIdentity{UID: 2000, GID: 2000}}
	served := DatabaseDelivery{Path: "/var/lib/kosd/kos.db", Node: "node-a", Owner: DatabaseIdentity{UID: 1000, GID: 1000}}
	inherited := InheritDatabaseDelivery(Plan{Mode: ModeExisting, Owner: "owner", Database: &named}, served, true)
	if *inherited.Database != named {
		t.Fatalf("delivery = %+v, want the named %+v", *inherited.Database, named)
	}
}

// TestInheritDatabaseDeliveryLeavesAPlanWithoutAServedFileAlone pins the
// first-install shape: nothing is served yet, so there is nothing to keep.
func TestInheritDatabaseDeliveryLeavesAPlanWithoutAServedFileAlone(t *testing.T) {
	inherited := InheritDatabaseDelivery(Plan{Mode: ModeExisting, Owner: "owner"}, DatabaseDelivery{}, false)
	if inherited.Database != nil {
		t.Fatalf("delivery = %+v, want none: a cluster that serves no database has none to keep", inherited.Database)
	}
}

// unpinnedWorkload renders the installation's workload and strips the node pin,
// which is the shape an older release wrote: a hostPath database with no
// affinity, so the pod's own node is the only record of which filesystem holds
// the file.
func unpinnedWorkload(t *testing.T) *appsv1.Deployment {
	t.Helper()
	delivery := DatabaseDelivery{Path: "/var/lib/kosd/kos.db", Node: "node-a", Owner: DatabaseIdentity{UID: 1000, GID: 1000}}
	documents, err := Render(Plan{
		Mode:      ModeExisting,
		Namespace: upgradeNamespace,
		Owner:     "owner",
		Image:     upgradeImage,
		Database:  &delivery,
	})
	if err != nil {
		t.Fatalf("render the installation workload: %v", err)
	}
	for _, document := range documents {
		if document.GVR.Resource != "deployments" {
			continue
		}
		encoded, err := json.Marshal(document.Object)
		if err != nil {
			t.Fatalf("encode the installation workload: %v", err)
		}
		rendered := &appsv1.Deployment{}
		if err := json.Unmarshal(encoded, rendered); err != nil {
			t.Fatalf("decode the installation workload: %v", err)
		}
		rendered.Spec.Template.Spec.Affinity = nil
		rendered.Spec.Template.Spec.NodeName = ""
		rendered.UID = workloadUID
		return rendered
	}
	t.Fatal("the installation renders no control-plane workload")
	return nil
}

// TestServedDatabaseReadsTheNodeOfAnUnpinnedWorkload pins the upgrade side of wipe
// and reinstall: a workload from a release that pinned no node still serves one
// machine's file, and the pod it runs is the record of which. Without that node
// the replacement would be pinned to whatever host runs the upgrade and read
// that machine's copy of the path (C-54, C-63).
func TestServedDatabaseReadsTheNodeOfAnUnpinnedWorkload(t *testing.T) {
	deployment := unpinnedWorkload(t)
	pod := servingPod("kos-control-plane-abc", "node-a", "/var/lib/kosd/kos.db")

	served, found, err := ServedDatabase(context.Background(), fake.NewSimpleClientset(deployment, servingReplicaSet(), pod), upgradeNamespace)
	if err != nil {
		t.Fatalf("read the served database: %v", err)
	}
	if !found {
		t.Fatal("the workload serves a database and none was read")
	}
	if served.Node != "node-a" {
		t.Fatalf("served node = %q, want node-a, the node its pod runs on", served.Node)
	}
	if served.Path != "/var/lib/kosd/kos.db" {
		t.Fatalf("served path = %q, want the file the workload serves", served.Path)
	}
}

// TestServedDatabaseLeavesAnUnpinnedWorkloadWithoutANode pins the other outcome:
// no running pod names a node, so the upgrade has no evidence and must not
// invent one. A second node's pod is the same refusal: two machines' files mean
// no single node names the database.
func TestServedDatabaseLeavesAnUnpinnedWorkloadWithoutANode(t *testing.T) {
	deployment := unpinnedWorkload(t)
	path := "/var/lib/kosd/kos.db"
	pending := servingPod("kos-control-plane-pending", "node-b", path)
	pending.Status = corev1.PodStatus{Phase: corev1.PodPending}
	unrelated := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-app-abc",
			Namespace: upgradeNamespace,
			Labels:    map[string]string{"app": ControlPlaneName},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node-b",
			Containers: []corev1.Container{{Name: "nginx", Image: "nginx:1.27"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}},
	}
	for name, objects := range map[string][]runtime.Object{
		"no pod":                   {deployment, servingReplicaSet()},
		"two nodes":                {deployment, servingReplicaSet(), servingPod("one", "node-a", path), servingPod("two", "node-b", path)},
		"bound but not started":    {deployment, servingReplicaSet(), pending},
		"unrelated same-label pod": {deployment, servingReplicaSet(), unrelated},
	} {
		t.Run(name, func(t *testing.T) {
			served, found, err := ServedDatabase(context.Background(), fake.NewSimpleClientset(objects...), upgradeNamespace)
			if err != nil {
				t.Fatalf("read the served database: %v", err)
			}
			if !found || served.Node != "" {
				t.Fatalf("served = %+v found = %t, want the file with no node", served, found)
			}
		})
	}
}

// TestServedDatabaseRefusesAPodOfAnotherDeployment pins the ownership chain a
// name prefix cannot stand for: another deployment in the namespace owns a
// ReplicaSet whose name also starts with the control plane's name, and its pod
// runs the same image, args and mount on another node. Accepting it would pin
// the original control plane to a machine that serves a different database.
func TestServedDatabaseRefusesAPodOfAnotherDeployment(t *testing.T) {
	deployment := unpinnedWorkload(t)
	path := "/var/lib/kosd/kos.db"
	for name, objects := range map[string][]runtime.Object{
		// The pod's controller is a ReplicaSet of kos-control-plane-canary,
		// whose name shares the control plane's prefix.
		"replica set of another deployment": {
			deployment,
			canaryReplicaSet(),
			canaryPod("canary-abc", "node-b", path),
		},
		// The pod names the control plane's own ReplicaSet, but by a UID that
		// is not the stored object's: a stale pod, not this workload's.
		"stale replica set uid": {
			deployment,
			servingReplicaSet(),
			stalePod("kos-control-plane-stale", "node-b", path),
		},
	} {
		t.Run(name, func(t *testing.T) {
			served, found, err := ServedDatabase(context.Background(), fake.NewSimpleClientset(objects...), upgradeNamespace)
			if err != nil {
				t.Fatalf("read the served database: %v", err)
			}
			if !found || served.Node != "" {
				t.Fatalf("served = %+v found = %t, want the file with no node", served, found)
			}
		})
	}
}

// TestServedDatabaseRefusesAMountThatReadsAnotherFile pins the volume mapping:
// the mount's root and the container's path match, but a subPath sends the
// container path to another host file, and the replacement renders no sub path.
// The pod would not serve the file the upgrade keeps, so it is no evidence.
func TestServedDatabaseRefusesAMountThatReadsAnotherFile(t *testing.T) {
	deployment := unpinnedWorkload(t)
	path := "/var/lib/kosd/kos.db"
	controller := true
	redirected := servingPod("kos-control-plane-subpath", "node-b", path)
	redirected.OwnerReferences[0].Controller = &controller
	redirected.Spec.Containers[0].VolumeMounts[0].SubPath = "other/kos.db"
	expanded := servingPod("kos-control-plane-subpathexpr", "node-b", path)
	expanded.Spec.Containers[0].VolumeMounts[0].SubPathExpr = "$(KOS_DATABASE)"
	for name, pod := range map[string]*corev1.Pod{
		"subPath":     redirected,
		"subPathExpr": expanded,
	} {
		t.Run(name, func(t *testing.T) {
			served, found, err := ServedDatabase(context.Background(), fake.NewSimpleClientset(deployment, servingReplicaSet(), pod), upgradeNamespace)
			if err != nil {
				t.Fatalf("read the served database: %v", err)
			}
			if !found || served.Node != "" {
				t.Fatalf("served = %+v found = %t, want the file with no node", served, found)
			}
		})
	}
}

// TestServedDatabaseRefusesAFileMountedOverTheDirectory pins the mount that
// wins without changing the directory mount: a second volume mounts the database
// file itself, which hides the parent directory's copy. The process reads the
// file mount, and the replacement renders only the directory mount, so the pod
// is no evidence that the node serves the file the upgrade keeps.
func TestServedDatabaseRefusesAFileMountedOverTheDirectory(t *testing.T) {
	deployment := unpinnedWorkload(t)
	path := "/var/lib/kosd/kos.db"
	for name, shadow := range map[string]corev1.VolumeMount{
		"file mounted from another host path": {Name: "shadow", MountPath: path},
		"file mounted through a sub path":     {Name: "shadow", MountPath: path, SubPath: "other/kos.db"},
	} {
		t.Run(name, func(t *testing.T) {
			pod := servingPod("kos-control-plane-shadowed", "node-b", path)
			pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, shadow)
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
				Name: "shadow",
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
					Path: filepath.Join(t.TempDir(), "kos.db"),
				}},
			})
			served, found, err := ServedDatabase(context.Background(), fake.NewSimpleClientset(deployment, servingReplicaSet(), pod), upgradeNamespace)
			if err != nil {
				t.Fatalf("read the served database: %v", err)
			}
			if !found || served.Node != "" {
				t.Fatalf("served = %+v found = %t, want the file with no node", served, found)
			}
		})
	}
}

// TestServedDatabaseRefusesAMountOfAnEquivalentFilePath pins the comparison
// after cleaning: a second volume can mount the database file through a path
// the kernel resolves to the same file while the raw strings differ, such as a
// "." component, repeated separators or a trailing slash. That file mount hides
// the plain directory mount, so the pod reads a different volume than the
// replacement renders, and the node it runs on proves nothing about the file
// the upgrade keeps.
func TestServedDatabaseRefusesAMountOfAnEquivalentFilePath(t *testing.T) {
	deployment := unpinnedWorkload(t)
	path := "/var/lib/kosd/kos.db"
	for _, testCase := range []struct {
		name      string
		mountPath string
	}{
		{"dot component", "/var/lib/kosd/./kos.db"},
		{"repeated separators", "/var//lib/kosd//kos.db"},
		{"trailing slash", path + "/"},
		{"dot component and trailing slash", "/var/lib/kosd/./kos.db/"},
		{"relative path", "var/lib/kosd/kos.db"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pod := servingPod("kos-control-plane-equivalent", "node-b", path)
			pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "shadow", MountPath: testCase.mountPath})
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
				Name: "shadow",
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
					Path: filepath.Join(t.TempDir(), "kos.db"),
				}},
			})
			served, found, err := ServedDatabase(context.Background(), fake.NewSimpleClientset(deployment, servingReplicaSet(), pod), upgradeNamespace)
			if err != nil {
				t.Fatalf("read the served database: %v", err)
			}
			if !found || served.Node != "" {
				t.Fatalf("served = %+v found = %t, want the file with no node", served, found)
			}
		})
	}
}

// TestServedDatabaseAcceptsAPlainDirectoryMountInAnySpelling pins the other
// side of cleaning: the database's directory and the database path itself may
// be written with a "." component, repeated separators or a trailing slash.
// Those name the same host directory the replacement renders, so the pod is
// evidence of the node that holds the file.
func TestServedDatabaseAcceptsAPlainDirectoryMountInAnySpelling(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		databasePath string
		mountPath    string
		hostPath     string
	}{
		{"dot component", "/var/lib/kosd/kos.db", "/var/lib/kosd/.", "/var/lib/kosd"},
		{"repeated separators", "/var/lib/kosd/kos.db", "/var//lib/kosd", "//var/lib/kosd"},
		{"trailing slash", "/var/lib/kosd/kos.db", "/var/lib/kosd/", "/var/lib/kosd/"},
		{"relative path", "/var/lib/kosd/kos.db", "var/lib/kosd", "/var/lib/kosd"},
		{"database path with a dot component", "/var/lib/kosd/./kos.db", "/var/lib/kosd", "/var/lib/kosd"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			deployment := unpinnedWorkload(t)
			deployment.Spec.Template.Spec.Containers[0].Args = []string{"-database", testCase.databasePath}
			pod := servingPod("kos-control-plane-plain", "node-a", testCase.databasePath)
			pod.Spec.Containers[0].VolumeMounts[0].MountPath = testCase.mountPath
			pod.Spec.Volumes[0].HostPath.Path = testCase.hostPath
			served, found, err := ServedDatabase(context.Background(), fake.NewSimpleClientset(deployment, servingReplicaSet(), pod), upgradeNamespace)
			if err != nil {
				t.Fatalf("read the served database: %v", err)
			}
			if !found || served.Node != "node-a" {
				t.Fatalf("served = %+v found = %t, want the file on node-a", served, found)
			}
			if served.Path != testCase.databasePath {
				t.Fatalf("served path = %q, want %q", served.Path, testCase.databasePath)
			}
		})
	}
}

// TestServedDatabaseRefusesTwoMountsAtTheSameDirectory pins the order boundary:
// two mounts can normalize to the same container directory, and the runtime
// applies them in order, so the later volume can serve even though the first is
// the plain directory mount. Nothing in the pod says which one the control
// plane reads, so the node is not evidence of the file the upgrade keeps.
func TestServedDatabaseRefusesTwoMountsAtTheSameDirectory(t *testing.T) {
	deployment := unpinnedWorkload(t)
	path := "/var/lib/kosd/kos.db"
	for name, second := range map[string]corev1.VolumeMount{
		"dot component":       {Name: "shadow", MountPath: "/var/lib/kosd/."},
		"repeated separators": {Name: "shadow", MountPath: "/var//lib/kosd"},
		"same path again":     {Name: "shadow", MountPath: "/var/lib/kosd"},
	} {
		t.Run(name, func(t *testing.T) {
			pod := servingPod("kos-control-plane-duplicate", "node-b", path)
			pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, second)
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
				Name:         "shadow",
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/srv/other"}},
			})
			served, found, err := ServedDatabase(context.Background(), fake.NewSimpleClientset(deployment, servingReplicaSet(), pod), upgradeNamespace)
			if err != nil {
				t.Fatalf("read the served database: %v", err)
			}
			if !found || served.Node != "" {
				t.Fatalf("served = %+v found = %t, want the file with no node", served, found)
			}
		})
	}
}

// servingReplicaSet is the ReplicaSet the workload's pods name as their
// controller, owned by the workload by UID.
func servingReplicaSet() *appsv1.ReplicaSet {
	controller := true
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ControlPlaneName + "-abc",
			Namespace: upgradeNamespace,
			UID:       workloadSetUID,
			OwnerReferences: []metav1.OwnerReference{{
				Kind: "Deployment", Name: ControlPlaneName, UID: workloadUID, Controller: &controller,
			}},
		},
	}
}

// canaryReplicaSet is a ReplicaSet of a different deployment whose name shares
// the control plane's prefix.
func canaryReplicaSet() *appsv1.ReplicaSet {
	controller := true
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      canaryWorkload + "-abc",
			Namespace: upgradeNamespace,
			UID:       canarySetUID,
			OwnerReferences: []metav1.OwnerReference{{
				Kind: "Deployment", Name: canaryWorkload, UID: canarySetWorkUID, Controller: &controller,
			}},
		},
	}
}

// canaryPod is a ready control-plane-shaped pod on node whose controller is the
// canary deployment's ReplicaSet, not the workload's.
func canaryPod(name, node, path string) *corev1.Pod {
	pod := servingPod(name, node, path)
	pod.OwnerReferences[0].Name = canaryWorkload + "-abc"
	pod.OwnerReferences[0].UID = canarySetUID
	return pod
}

// stalePod names the workload's ReplicaSet by a UID the stored object does not
// carry, which is what a pod of a deleted and recreated ReplicaSet looks like.
func stalePod(name, node, path string) *corev1.Pod {
	pod := servingPod(name, node, path)
	pod.OwnerReferences[0].UID = "uid-stale-replicaset"
	return pod
}

// servingPod is a control-plane pod that is really serving path: running and
// ready, controlled by a ReplicaSet of the workload, and running the
// control-plane container with the database mounted from that host directory.
// A pod that is merely bound or merely labelled proves nothing about which
// filesystem the running control plane reads.
func servingPod(name, node, path string) *corev1.Pod {
	directory := filepath.Dir(path)
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: upgradeNamespace,
			Labels:    map[string]string{"app": ControlPlaneName},
			OwnerReferences: []metav1.OwnerReference{{
				Kind: "ReplicaSet", Name: ControlPlaneName + "-abc", UID: workloadSetUID, Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{
			NodeName: node,
			Containers: []corev1.Container{{
				Name:         ControlPlaneName,
				Args:         []string{"-database", path},
				VolumeMounts: []corev1.VolumeMount{{Name: DatabaseVolume, MountPath: directory}},
			}},
			Volumes: []corev1.Volume{{Name: DatabaseVolume,
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: directory}}}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}},
	}
}

// pointer returns the address of a value, so a fixture can name an optional
// field.
func pointer[T any](value T) *T { return &value }
