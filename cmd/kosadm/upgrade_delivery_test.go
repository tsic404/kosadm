package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/tsic404/kosadm/pkg/install"
)

// pinnedWorkload is a running control plane that serves path, pinned to node or,
// with an empty node, pinned to nothing: the shape a release that predates the
// pin left behind.
func pinnedWorkload(path, node string) *appsv1.Deployment {
	user, group := int64(1000), int64(1000)
	spec := corev1.PodSpec{Containers: []corev1.Container{{
		Name:            install.ControlPlaneName,
		Args:            []string{"-database", path},
		SecurityContext: &corev1.SecurityContext{RunAsUser: &user, RunAsGroup: &group},
	}}}
	if node != "" {
		spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchFields: []corev1.NodeSelectorRequirement{{
						Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{node},
					}},
				}},
			},
		}}
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: install.ControlPlaneName, Namespace: install.DefaultNamespace, UID: workloadUID},
		Spec:       appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: spec}},
	}
}

// The controller chain the fixtures carry: the workload's UID and the UID of the
// ReplicaSet its pods name as their controller.
var (
	workloadUID       = types.UID("uid-" + install.ControlPlaneName)
	workloadSetUID    = types.UID("uid-" + install.ControlPlaneName + "-rs")
	canaryWorkload    = install.ControlPlaneName + "-canary"
	canarySetUID      = types.UID("uid-" + canaryWorkload + "-rs")
	canaryWorkloadUID = types.UID("uid-" + canaryWorkload)
)

// servingReplicaSet is the ReplicaSet the workload's pods name as their
// controller, owned by the workload by UID.
func servingReplicaSet() *appsv1.ReplicaSet {
	controller := true
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      install.ControlPlaneName + "-abc",
			Namespace: install.DefaultNamespace,
			UID:       workloadSetUID,
			OwnerReferences: []metav1.OwnerReference{{
				Kind: "Deployment", Name: install.ControlPlaneName, UID: workloadUID, Controller: &controller,
			}},
		},
	}
}

// runningPod is one control-plane pod the scheduler placed on node and that is
// really serving the delivery at path: it is running and ready, a ReplicaSet of
// the workload controls it, and its control-plane container mounts the file's
// directory from that host. A pod that only carries the label proves nothing
// about which filesystem the running control plane reads, so the fixtures build
// the whole shape.
func runningPod(name, node, path string) *corev1.Pod {
	directory := filepath.Dir(path)
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: install.DefaultNamespace,
			Labels:    map[string]string{"app": install.ControlPlaneName},
			OwnerReferences: []metav1.OwnerReference{{
				Kind: "ReplicaSet", Name: install.ControlPlaneName + "-abc", UID: workloadSetUID, Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{
			NodeName: node,
			Containers: []corev1.Container{{
				Name:         install.ControlPlaneName,
				Args:         []string{"-database", path},
				VolumeMounts: []corev1.VolumeMount{{Name: install.DatabaseVolume, MountPath: directory}},
			}},
			Volumes: []corev1.Volume{{Name: install.DatabaseVolume,
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: directory}}}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}},
	}
}

// pendingPod is a control-plane pod the scheduler bound to node that has not
// started its control plane: its container never mounted the host directory, so
// it says nothing about which file the workload serves.
func pendingPod(name, node, path string) *corev1.Pod {
	pod := runningPod(name, node, path)
	pod.Status = corev1.PodStatus{Phase: corev1.PodPending}
	return pod
}

// unrelatedPod carries the workload's label but is not its pod and runs no
// control plane: another application's pod that happens to share the label.
func unrelatedPod(name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: install.DefaultNamespace,
			Labels:    map[string]string{"app": install.ControlPlaneName},
		},
		Spec: corev1.PodSpec{
			NodeName:   node,
			Containers: []corev1.Container{{Name: "nginx", Image: "nginx:1.27"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}},
	}
}

// clusterNode is one node that reports hostname as its machine name.
func clusterNode(name, hostname string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: name, Labels: map[string]string{corev1.LabelHostname: hostname},
	}}
}

// databaseOnThisHost writes a database file the command really opens.
func databaseOnThisHost(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kos.db")
	if err := os.WriteFile(path, []byte("seeded"), 0o600); err != nil {
		t.Fatalf("write the database: %v", err)
	}
	return path
}

// settleOnHost runs the CLI's delivery decision against a cluster this command
// reaches as hostname, with the flags the operator named.
func settleOnHost(t *testing.T, hostname, path, namedNode string, objects ...runtime.Object) (install.Plan, error) {
	t.Helper()
	original := hostName
	hostName = func() (string, error) { return hostname, nil }
	t.Cleanup(func() { hostName = original })
	plan := install.Plan{Mode: install.ModeExisting, Namespace: install.DefaultNamespace, Owner: "owner"}
	flags := platformFlags{identityDatabase: path, controlPlaneNode: namedNode}
	if path != "" {
		delivery := install.DatabaseDelivery{Path: path, Node: namedNode}
		plan.Database = &delivery
	}
	return settleUpgradeDelivery(context.Background(), fake.NewSimpleClientset(objects...), plan, flags)
}

// TestSettleUpgradeDeliveryRefusesANodeThisHostDoesNotHold covers the upgrade
// run from B while the control plane serves the file on A: the file B opens is
// B's copy, so the node the operator names cannot stand for where that file is.
// A delivery pinned to A would audit on B and render B's owner onto A's
// workload. The run refuses before anything is replaced (ADM-C-54, ADM-C-56).
func TestSettleUpgradeDeliveryRefusesANodeThisHostDoesNotHold(t *testing.T) {
	path := databaseOnThisHost(t)
	plan, err := settleOnHost(t, "node-b", path, "node-a",
		clusterNode("node-a", "node-a"),
		clusterNode("node-b", "node-b"),
		pinnedWorkload(path, "node-a"),
	)
	if err == nil {
		t.Fatalf("a database pinned to node-a was accepted on host node-b as %+v", plan.Database)
	}
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 2 {
		t.Fatalf("refusal = %v, want a usage error", err)
	}
	for _, want := range []string{"node-a", "node-b", path} {
		if !strings.Contains(coded.message, want) {
			t.Fatalf("refusal %q does not name %q", coded.message, want)
		}
	}
}

// TestSettleUpgradeDeliveryKeepsAnUnpinnedServedDeliveryOnItsPodNode covers a
// cluster an older release left: the control plane serves a hostPath database
// but the workload pins no node. The pod it runs is the only record of which
// filesystem holds the file, and that is evidence the upgrade can use. A pure
// platform upgrade from any host therefore keeps node-a rather than filling the
// gap with the host it happens to run on (ADM-C-54, ADM-C-55).
func TestSettleUpgradeDeliveryKeepsAnUnpinnedServedDeliveryOnItsPodNode(t *testing.T) {
	path := "/var/lib/kosd/kos.db"
	plan, err := settleOnHost(t, "node-b", "", "",
		clusterNode("node-a", "node-a"),
		clusterNode("node-b", "node-b"),
		pinnedWorkload(path, ""),
		servingReplicaSet(),
		runningPod("kos-control-plane-abc", "node-a", path),
	)
	if err != nil {
		t.Fatalf("a platform upgrade of a workload whose pod shows its node was refused: %v", err)
	}
	if plan.Database == nil || plan.Database.Node != "node-a" || plan.Database.Path != path {
		t.Fatalf("delivery = %+v, want the served file on node-a, the node its pod runs on", plan.Database)
	}
}

// TestSettleUpgradeDeliveryRefusesAnUnpinnedWorkloadsCopyElsewhere covers the
// same unpinned cluster from B while this command opened B's copy of the path.
// The serving pod puts the file on A, so a delivery built from this host would
// audit B and render B's owner onto A's workload, and the accounts the running
// pod serves would be gone. The run refuses before anything is replaced, and
// naming this host's node does not turn B's file into A's (ADM-C-54, ADM-C-56).
func TestSettleUpgradeDeliveryRefusesAnUnpinnedWorkloadsCopyElsewhere(t *testing.T) {
	path := databaseOnThisHost(t)
	objects := []runtime.Object{
		clusterNode("node-a", "node-a"),
		clusterNode("node-b", "node-b"),
		pinnedWorkload(path, ""),
		servingReplicaSet(),
		runningPod("kos-control-plane-abc", "node-a", path),
	}
	for _, testCase := range []struct {
		name     string
		database string
		node     string
	}{
		{"this host's copy named", path, ""},
		{"this host's copy and node named", path, "node-b"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			plan, err := settleOnHost(t, "node-b", testCase.database, testCase.node, objects...)
			if err == nil {
				t.Fatalf("an unpinned delivery served on node-a was accepted from node-b as %+v", plan.Database)
			}
			var coded exitError
			if !errors.As(err, &coded) || coded.code != 2 {
				t.Fatalf("refusal = %v, want a usage error", err)
			}
			if !strings.Contains(coded.message, "node-a") {
				t.Fatalf("refusal %q does not name the node that serves the file", coded.message)
			}
		})
	}
}

// TestSettleUpgradeDeliveryRefusesAnUnpinnedWorkloadWithNoServingPod covers the
// unpinned cluster whose pods say nothing about where the file is: no pod at
// all, pods on two nodes (two different files), a pod that is bound but never
// started its control plane, or a same-label pod that is some other application.
// None of them proves which node serves the delivery, so the upgrade refuses
// instead of pinning this host and serving a database nobody installed
// (ADM-C-54, ADM-C-56).
func TestSettleUpgradeDeliveryRefusesAnUnpinnedWorkloadWithNoServingPod(t *testing.T) {
	path := "/var/lib/kosd/kos.db"
	for name, pods := range map[string][]runtime.Object{
		"no pod":                   nil,
		"two nodes":                {runningPod("kos-control-plane-one", "node-a", path), runningPod("kos-control-plane-two", "node-b", path)},
		"bound but not started":    {pendingPod("kos-control-plane-abc", "node-b", path)},
		"unrelated same-label pod": {unrelatedPod("other-app-abc", "node-b")},
	} {
		t.Run(name, func(t *testing.T) {
			objects := append([]runtime.Object{
				clusterNode("node-a", "node-a"),
				clusterNode("node-b", "node-b"),
				pinnedWorkload(path, ""),
				servingReplicaSet(),
			}, pods...)
			plan, err := settleOnHost(t, "node-b", "", "", objects...)
			if err == nil {
				t.Fatalf("an unpinned delivery with no single serving node was accepted as %+v", plan.Database)
			}
			var coded exitError
			if !errors.As(err, &coded) || coded.code != 2 {
				t.Fatalf("refusal = %v, want a usage error", err)
			}
			if !strings.Contains(coded.message, path) {
				t.Fatalf("refusal %q does not name the file it would have kept", coded.message)
			}
		})
	}
}

// TestSettleUpgradeDeliveryRefusesAPodOfAnotherDeployment covers the pod that
// looks like the workload's but is not: its ReplicaSet belongs to
// kos-control-plane-canary, whose name shares the control plane's prefix. The
// pod is running and ready on another node and mounts the same directory, so
// only ownership tells them apart. Accepting it would pin the original control
// plane to a machine that serves a different database.
func TestSettleUpgradeDeliveryRefusesAPodOfAnotherDeployment(t *testing.T) {
	path := "/var/lib/kosd/kos.db"
	controller := true
	canarySet := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Name:      canaryWorkload + "-abc",
		Namespace: install.DefaultNamespace,
		UID:       canarySetUID,
		OwnerReferences: []metav1.OwnerReference{{
			Kind: "Deployment", Name: canaryWorkload, UID: canaryWorkloadUID, Controller: &controller,
		}},
	}}
	pod := runningPod("kos-control-plane-canary-abc", "node-b", path)
	pod.OwnerReferences[0].Name = canaryWorkload + "-abc"
	pod.OwnerReferences[0].UID = canarySetUID

	plan, err := settleOnHost(t, "node-b", "", "",
		clusterNode("node-a", "node-a"),
		clusterNode("node-b", "node-b"),
		pinnedWorkload(path, ""),
		canarySet,
		pod,
	)
	if err == nil {
		t.Fatalf("a pod owned by another deployment was accepted as %+v", plan.Database)
	}
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 2 {
		t.Fatalf("refusal = %v, want a usage error", err)
	}
	if !strings.Contains(coded.message, path) {
		t.Fatalf("refusal %q does not name the file it would have kept", coded.message)
	}
}

// TestSettleUpgradeDeliveryRefusesAMountThatReadsAnotherFile covers the mount
// whose root and container path match but whose subPath sends the container to
// another host file: the replacement renders no sub path, so the pod does not
// serve the file the upgrade would keep.
func TestSettleUpgradeDeliveryRefusesAMountThatReadsAnotherFile(t *testing.T) {
	path := "/var/lib/kosd/kos.db"
	subPath := runningPod("kos-control-plane-subpath", "node-b", path)
	subPath.Spec.Containers[0].VolumeMounts[0].SubPath = "other/kos.db"
	subPathExpr := runningPod("kos-control-plane-subpathexpr", "node-b", path)
	subPathExpr.Spec.Containers[0].VolumeMounts[0].SubPathExpr = "$(KOS_DATABASE)"
	for name, pod := range map[string]*corev1.Pod{"subPath": subPath, "subPathExpr": subPathExpr} {
		t.Run(name, func(t *testing.T) {
			plan, err := settleOnHost(t, "node-b", "", "",
				clusterNode("node-a", "node-a"),
				clusterNode("node-b", "node-b"),
				pinnedWorkload(path, ""),
				servingReplicaSet(),
				pod,
			)
			if err == nil {
				t.Fatalf("a pod whose sub path reads another file was accepted as %+v", plan.Database)
			}
			var coded exitError
			if !errors.As(err, &coded) || coded.code != 2 {
				t.Fatalf("refusal = %v, want a usage error", err)
			}
			if !strings.Contains(coded.message, path) {
				t.Fatalf("refusal %q does not name the file it would have kept", coded.message)
			}
		})
	}
}

// TestSettleUpgradeDeliveryRefusesAFileMountedOverTheDirectory covers the pod
// whose plain directory mount is still there but is hidden by a second volume
// that mounts the database file itself. The process reads the file mount, and
// the replacement renders only the directory mount, so the pod does not show
// that the node serves the file the upgrade keeps.
func TestSettleUpgradeDeliveryRefusesAFileMountedOverTheDirectory(t *testing.T) {
	path := "/var/lib/kosd/kos.db"
	for name, shadow := range map[string]corev1.VolumeMount{
		"file mounted from another host path":      {Name: "shadow", MountPath: path},
		"file mounted through a sub path":          {Name: "shadow", MountPath: path, SubPath: "other/kos.db"},
		"file mounted through a dot component":     {Name: "shadow", MountPath: "/var/lib/kosd/./kos.db"},
		"file mounted through repeated separators": {Name: "shadow", MountPath: "/var//lib/kosd//kos.db"},
		"file mounted through a trailing slash":    {Name: "shadow", MountPath: path + "/"},
		"file mounted through a relative path":     {Name: "shadow", MountPath: strings.TrimPrefix(path, "/")},
	} {
		t.Run(name, func(t *testing.T) {
			pod := runningPod("kos-control-plane-shadowed", "node-b", path)
			pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, shadow)
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
				Name: "shadow",
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
					Path: "/srv/other/kos.db",
				}},
			})
			plan, err := settleOnHost(t, "node-b", "", "",
				clusterNode("node-a", "node-a"),
				clusterNode("node-b", "node-b"),
				pinnedWorkload(path, ""),
				servingReplicaSet(),
				pod,
			)
			if err == nil {
				t.Fatalf("a pod whose file mount hides the directory was accepted as %+v", plan.Database)
			}
			var coded exitError
			if !errors.As(err, &coded) || coded.code != 2 {
				t.Fatalf("refusal = %v, want a usage error", err)
			}
			if !strings.Contains(coded.message, path) {
				t.Fatalf("refusal %q does not name the file it would have kept", coded.message)
			}
		})
	}
}

// TestSettleUpgradeDeliveryKeepsAPlainDirectoryMountInAnySpelling pins the
// positive case against the same normalization: a directory mount written with
// a "." component, repeated separators or a trailing slash still mounts the
// host directory the replacement renders, so the upgrade keeps the served file
// on the pod's node instead of refusing.
func TestSettleUpgradeDeliveryKeepsAPlainDirectoryMountInAnySpelling(t *testing.T) {
	path := "/var/lib/kosd/kos.db"
	for _, testCase := range []struct {
		name      string
		mountPath string
		hostPath  string
	}{
		{"dot component", "/var/lib/kosd/.", "/var/lib/kosd"},
		{"repeated separators", "/var//lib/kosd", "//var/lib/kosd"},
		{"trailing slash", "/var/lib/kosd/", "/var/lib/kosd/"},
		{"relative path", "var/lib/kosd", "/var/lib/kosd"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pod := runningPod("kos-control-plane-plain", "node-a", path)
			pod.Spec.Containers[0].VolumeMounts[0].MountPath = testCase.mountPath
			pod.Spec.Volumes[0].HostPath.Path = testCase.hostPath
			plan, err := settleOnHost(t, "node-b", "", "",
				clusterNode("node-a", "node-a"),
				clusterNode("node-b", "node-b"),
				pinnedWorkload(path, ""),
				servingReplicaSet(),
				pod,
			)
			if err != nil {
				t.Fatalf("a plain directory mount was refused: %v", err)
			}
			if plan.Database == nil || plan.Database.Node != "node-a" || plan.Database.Path != path {
				t.Fatalf("delivery = %+v, want the served file on node-a", plan.Database)
			}
		})
	}
}

// TestSettleUpgradeDeliveryRefusesTwoMountsAtTheSameDirectory covers the order
// boundary after cleaning: two volumes mounted at paths that normalize to the
// same directory are applied in order, so the later one can serve. The pod no
// longer shows which volume the control plane reads, so the upgrade refuses
// before it writes.
func TestSettleUpgradeDeliveryRefusesTwoMountsAtTheSameDirectory(t *testing.T) {
	path := "/var/lib/kosd/kos.db"
	for name, second := range map[string]corev1.VolumeMount{
		"dot component":       {Name: "shadow", MountPath: "/var/lib/kosd/."},
		"repeated separators": {Name: "shadow", MountPath: "/var//lib/kosd"},
	} {
		t.Run(name, func(t *testing.T) {
			pod := runningPod("kos-control-plane-duplicate", "node-b", path)
			pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, second)
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
				Name:         "shadow",
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/srv/other"}},
			})
			plan, err := settleOnHost(t, "node-b", "", "",
				clusterNode("node-a", "node-a"),
				clusterNode("node-b", "node-b"),
				pinnedWorkload(path, ""),
				servingReplicaSet(),
				pod,
			)
			if err == nil {
				t.Fatalf("two mounts at the same directory were accepted as %+v", plan.Database)
			}
			var coded exitError
			if !errors.As(err, &coded) || coded.code != 2 {
				t.Fatalf("refusal = %v, want a usage error", err)
			}
			if !strings.Contains(coded.message, path) {
				t.Fatalf("refusal %q does not name the file it would have kept", coded.message)
			}
		})
	}
}

// TestSettleUpgradeDeliveryKeepsAPinnedServedDeliveryFromAnyHost pins the other
// side: a pure platform upgrade names no database, so it opens no local file and
// needs no host. It keeps the pin the workload already records, from B as well
// as from A (ADM-C-55).
func TestSettleUpgradeDeliveryKeepsAPinnedServedDeliveryFromAnyHost(t *testing.T) {
	path := "/var/lib/kosd/kos.db"
	plan, err := settleOnHost(t, "node-b", "", "",
		clusterNode("node-a", "node-a"),
		clusterNode("node-b", "node-b"),
		pinnedWorkload(path, "node-a"),
	)
	if err != nil {
		t.Fatalf("a platform upgrade that names no database was refused: %v", err)
	}
	if plan.Database == nil || plan.Database.Node != "node-a" || plan.Database.Path != path {
		t.Fatalf("delivery = %+v, want the served file on node-a", plan.Database)
	}
}

// TestSettleUpgradeDeliveryAcceptsThisHostsFileOnTheServedNode pins the accepted
// maintenance form: the command runs on the node that holds the database, names
// that node, and the served workload serves the same file there.
func TestSettleUpgradeDeliveryAcceptsThisHostsFileOnTheServedNode(t *testing.T) {
	path := databaseOnThisHost(t)
	plan, err := settleOnHost(t, "node-a", path, "node-a",
		clusterNode("node-a", "node-a"),
		pinnedWorkload(path, "node-a"),
	)
	if err != nil {
		t.Fatalf("the file on the node that serves it was refused: %v", err)
	}
	if plan.Database == nil || plan.Database.Node != "node-a" {
		t.Fatalf("delivery = %+v, want node-a", plan.Database)
	}
}

// TestSettleUpgradeDeliveryRefusesAFileOnAHostNoNodeNames pins the boundary
// behind the proof: an operator who names the right file but runs where no node
// reports this host cannot show the file is any node's.
func TestSettleUpgradeDeliveryRefusesAFileOnAHostNoNodeNames(t *testing.T) {
	path := databaseOnThisHost(t)
	plan, err := settleOnHost(t, "node-c", path, "",
		clusterNode("node-a", "node-a"),
	)
	if err == nil {
		t.Fatalf("a file on a host no node reports was accepted as %+v", plan.Database)
	}
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 2 {
		t.Fatalf("refusal = %v, want a usage error", err)
	}
	if !strings.Contains(coded.message, "node-c") {
		t.Fatalf("refusal %q does not name this host", coded.message)
	}
}
