package admtc95

import (
	"context"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
)

// unpinControlPlane strips the node pin from the workload the installation
// wrote, which is the shape a release that predates the pin left behind: the
// database is a hostPath, so the pod the workload runs is then the only record
// of which filesystem holds the file.
func unpinControlPlane(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	ctx := context.Background()
	deployments := cluster.Kube.AppsV1().Deployments(install.DefaultNamespace)
	deployment, err := deployments.Get(ctx, install.ControlPlaneName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the control plane: %v", err)
	}
	deployment.Spec.Template.Spec.Affinity = nil
	deployment.Spec.Template.Spec.NodeName = ""
	updated, err := deployments.Update(ctx, deployment, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("strip the control plane's node pin: %v", err)
	}
	if pinned := harness.RequiredNode(updated.Spec.Template.Spec); pinned != "" {
		t.Fatalf("the workload still pins node %q after the pin was stripped", pinned)
	}
}

// controlPlanePodManifest is a pod of the control plane's workload on node. The
// ReplicaSet named by setName/setUID controls it, and its control-plane container
// runs against path with the file's directory mounted from that host, which is
// what shows the process reads that machine's filesystem.
func controlPlanePodManifest(name, node, path, setName, setUID string) map[string]any {
	directory := filepath.Dir(path)
	controller := true
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      name,
			"namespace": install.DefaultNamespace,
			"labels":    map[string]any{"app": install.ControlPlaneName},
			"ownerReferences": []any{map[string]any{
				"apiVersion": "apps/v1", "kind": "ReplicaSet",
				"name":       setName,
				"uid":        setUID,
				"controller": controller,
			}},
		},
		"spec": map[string]any{
			"nodeName":                      node,
			"terminationGracePeriodSeconds": int64(0),
			"containers": []any{map[string]any{
				"name":         install.ControlPlaneName,
				"image":        "ghcr.io/tsic404/kos-control-plane:v0.1.0",
				"args":         []any{"-database", path},
				"volumeMounts": []any{map[string]any{"name": install.DatabaseVolume, "mountPath": directory}},
			}},
			"volumes": []any{map[string]any{
				"name":     install.DatabaseVolume,
				"hostPath": map[string]any{"path": directory},
			}},
		},
	}
}

// ensureControlPlaneReplicaSet writes the ReplicaSet the workload's pods name as
// their controller and returns its UID. envtest runs no deployment controller,
// so the case writes that link itself: the ReplicaSet names the stored
// Deployment, by UID, as its controller, which is the chain the ownership check
// follows.
func ensureControlPlaneReplicaSet(t *testing.T, cluster *harness.Cluster) string {
	t.Helper()
	ctx := context.Background()
	sets := cluster.Kube.AppsV1().ReplicaSets(install.DefaultNamespace)
	existing, err := sets.Get(ctx, install.ControlPlaneName+"-abc", metav1.GetOptions{})
	if err == nil {
		return string(existing.UID)
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("read the control plane's ReplicaSet: %v", err)
	}
	deployment, err := cluster.Kube.AppsV1().Deployments(install.DefaultNamespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the control plane: %v", err)
	}
	controller := true
	created, err := sets.Create(ctx, &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      install.ControlPlaneName + "-abc",
			Namespace: install.DefaultNamespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name,
				UID: deployment.UID, Controller: &controller,
			}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": install.ControlPlaneName}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": install.ControlPlaneName}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "nginx:1.27"}}},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("write the control plane's ReplicaSet: %v", err)
	}
	return string(created.UID)
}

// foreignReplicaSetOn writes a ReplicaSet of a different deployment whose name
// still starts with the control plane's name, and returns its UID. Its pods
// carry the workload's label and run the same container; only the ownership
// chain tells them apart from the workload's own pods.
func foreignReplicaSetOn(t *testing.T, cluster *harness.Cluster) string {
	t.Helper()
	controller := true
	return string(cluster.CreateClusterObject(t, "apps", "v1", "replicasets", install.DefaultNamespace, map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "ReplicaSet",
		"metadata": map[string]any{
			"name":      install.ControlPlaneName + "-canary-abc",
			"namespace": install.DefaultNamespace,
			"ownerReferences": []any{map[string]any{
				"apiVersion": "apps/v1", "kind": "Deployment",
				"name": install.ControlPlaneName + "-canary",
				"uid":  "uid-" + install.ControlPlaneName + "-canary", "controller": controller,
			}},
		},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"app": install.ControlPlaneName}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": install.ControlPlaneName}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "app", "image": "nginx:1.27"}}},
			},
		},
	}).GetUID())
}

// runningStatus is the pod status a started control plane reports: Running and
// Ready. Only this status makes a pod evidence that its node serves the file.
func runningStatus() map[string]any {
	return map[string]any{
		"phase": "Running",
		"conditions": []any{map[string]any{
			"type": "Ready", "status": "True",
			"lastTransitionTime": "2026-01-01T00:00:00Z",
			"reason":             "KubeletReady",
			"message":            "ready",
		}},
	}
}

// controlPlanePodOn writes one pod that is really serving the control plane on
// node, the evidence a real scheduler leaves.
func controlPlanePodOn(t *testing.T, cluster *harness.Cluster, name, node, path string) {
	t.Helper()
	setUID := ensureControlPlaneReplicaSet(t, cluster)
	createPodOn(t, cluster, name, controlPlanePodManifest(name, node, path, install.ControlPlaneName+"-abc", setUID), runningStatus())
}

// controlPlanePodOwnedBy writes a pod with the workload's container, args and
// mount, controlled by the ReplicaSet named by setName/setUID. It is the
// counter-case for a chain that proves a different deployment owns the pod.
func controlPlanePodOwnedBy(t *testing.T, cluster *harness.Cluster, name, node, path, setName, setUID string) {
	t.Helper()
	createPodOn(t, cluster, name, controlPlanePodManifest(name, node, path, setName, setUID), runningStatus())
}

// pendingControlPlanePodOn writes the workload's pod for node bound to it but
// not started: the container never mounted the host directory, so the pod says
// nothing about which file the control plane really reads.
func pendingControlPlanePodOn(t *testing.T, cluster *harness.Cluster, name, node, path string) {
	t.Helper()
	setUID := ensureControlPlaneReplicaSet(t, cluster)
	createPodOn(t, cluster, name, controlPlanePodManifest(name, node, path, install.ControlPlaneName+"-abc", setUID), map[string]any{"phase": "Pending"})
}

// mountedThroughSubPathOn writes a ready pod of the workload whose database
// mount names a sub path: the container's -database path is the same string,
// but the volume resolves it to another host file, and the replacement renders
// no sub path. The pod therefore does not serve the file the upgrade keeps.
func mountedThroughSubPathOn(t *testing.T, cluster *harness.Cluster, name, node, path string) {
	t.Helper()
	setUID := ensureControlPlaneReplicaSet(t, cluster)
	manifest := controlPlanePodManifest(name, node, path, install.ControlPlaneName+"-abc", setUID)
	spec := manifest["spec"].(map[string]any)
	mounts := spec["containers"].([]any)[0].(map[string]any)["volumeMounts"].([]any)
	mounts[0].(map[string]any)["subPath"] = "other/kos.db"
	createPodOn(t, cluster, name, manifest, runningStatus())
}

// fileMountedOverTheDirectoryOn writes a ready pod of the workload that keeps
// the plain directory mount but adds a second volume mounting the database file
// itself at mountPath, which hides the directory's copy. The process reads the
// file mount, and the replacement renders only the directory mount, so the pod
// does not show that the node serves the file the upgrade keeps.
func fileMountedOverTheDirectoryOn(t *testing.T, cluster *harness.Cluster, name, node, path, mountPath string) {
	t.Helper()
	extraHostPathMountOn(t, cluster, name, node, path, mountPath, filepath.Join(filepath.Dir(path), "other", "kos.db"))
}

// extraHostPathMountOn writes a ready pod of the workload with an extra
// hostPath volume mounted at mountPath, alongside the plain directory mount.
func extraHostPathMountOn(t *testing.T, cluster *harness.Cluster, name, node, path, mountPath, hostPath string) {
	t.Helper()
	setUID := ensureControlPlaneReplicaSet(t, cluster)
	manifest := controlPlanePodManifest(name, node, path, install.ControlPlaneName+"-abc", setUID)
	spec := manifest["spec"].(map[string]any)
	container := spec["containers"].([]any)[0].(map[string]any)
	container["volumeMounts"] = append(container["volumeMounts"].([]any),
		map[string]any{"name": "extra", "mountPath": mountPath})
	spec["volumes"] = append(spec["volumes"].([]any), map[string]any{
		"name":     "extra",
		"hostPath": map[string]any{"path": hostPath},
	})
	createPodOn(t, cluster, name, manifest, runningStatus())
}

// strangerPodOn writes a pod that carries the workload's label on node but is
// not the workload's: no controlling ReplicaSet, another container, no database
// mount. The label alone is not evidence that node serves the file.
func strangerPodOn(t *testing.T, cluster *harness.Cluster, name, node string) {
	t.Helper()
	createPodOn(t, cluster, name, map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      name,
			"namespace": install.DefaultNamespace,
			"labels":    map[string]any{"app": install.ControlPlaneName},
		},
		"spec": map[string]any{
			"nodeName":                      node,
			"terminationGracePeriodSeconds": int64(0),
			"containers":                    []any{map[string]any{"name": "app", "image": "nginx:1.27"}},
		},
	}, runningStatus())
}

// createPodOn writes one pod and then the status a kubelet would write. envtest
// runs no kubelet, so without the status write every pod would look Pending and
// the cases could not tell a serving pod from a bound one.
func createPodOn(t *testing.T, cluster *harness.Cluster, name string, manifest, status map[string]any) {
	t.Helper()
	created := cluster.CreateClusterObject(t, "", "v1", "pods", install.DefaultNamespace, manifest)
	created.Object["status"] = status
	if _, err := cluster.Dynamic.Resource(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}).
		Namespace(install.DefaultNamespace).UpdateStatus(context.Background(), created, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("write the status of pod %s: %v", name, err)
	}
}

// assertWorkloadDelivery checks the workload still serves the installation's
// file at path, still mounts the package directory, and pins wantNode: a refused
// upgrade writes nothing, and an accepted one keeps the node its serving pod
// showed (ADM-C-54, ADM-C-56).
func assertWorkloadDelivery(t *testing.T, cluster *harness.Cluster, path, wantNode string) {
	t.Helper()
	deployment, err := cluster.Kube.AppsV1().Deployments(install.DefaultNamespace).Get(
		context.Background(), install.ControlPlaneName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the control plane: %v", err)
	}
	if pinned := harness.RequiredNode(deployment.Spec.Template.Spec); pinned != wantNode {
		t.Fatalf("the workload's required node affinity = %q, want %q", pinned, wantNode)
	}
	if bound := deployment.Spec.Template.Spec.NodeName; bound != "" {
		t.Fatalf("the workload nodeName = %q, want none: a nodeName defeats the drain", bound)
	}
	harness.AssertDatabaseDelivery(t, deployment, install.DatabaseDelivery{
		Path:  path,
		Node:  wantNode,
		Owner: databaseOwnerOf(t, deployment),
	})
	harness.AssertPackageDirectory(t, deployment)
}

// databaseOwnerOf reads the account the workload runs as, so a delivery
// comparison names the owner the workload already carries.
func databaseOwnerOf(t *testing.T, deployment *appsv1.Deployment) install.DatabaseIdentity {
	t.Helper()
	account := deployment.Spec.Template.Spec.Containers[0].SecurityContext
	if account == nil || account.RunAsUser == nil || account.RunAsGroup == nil {
		t.Fatal("the workload names no account to run the database as")
	}
	return install.DatabaseIdentity{UID: *account.RunAsUser, GID: *account.RunAsGroup}
}

// installedControlPlane runs the real installation the unpinned cases upgrade,
// and returns the cluster, the delivery it wrote, and the CLI entry an operator
// drives the upgrade through.
func installedControlPlane(t *testing.T) (*harness.Cluster, install.DatabaseDelivery, string, []string) {
	t.Helper()
	cluster := harness.Start(t)
	installerIdentity := harness.InstallerIdentity()
	cluster.GrantClusterAdmin(t, installerIdentity)
	accounts := harness.OpenIdentity(t, nil, nil)
	cluster.RegisterControlPlaneNode(t)
	database, err := install.SeededDatabase(accounts.DatabasePath, harness.ControlPlaneNode)
	if err != nil {
		t.Fatalf("resolve the database the installation seeds: %v", err)
	}
	clients := cluster.InstallClients(t, installerIdentity)
	runner, err := install.New(install.Config{
		NoNodeAgentController: true,
		Plan: harness.InstallerPlan(func(plan *install.Plan) {
			plan.Database = &database
		}),
		Dynamic:       clients.Dynamic,
		Kube:          clients.Kube,
		APIExtensions: clients.APIExtensions,
		Definitions:   harness.Definitions(t),
		Accounts:      accounts.Service,
		QuietWindow:   200 * time.Millisecond,
		SettleTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("build installer: %v", err)
	}
	installed, err := runner.Install(context.Background())
	if err != nil {
		t.Fatalf("install: %v", err)
	}
	if !installed.Completed {
		t.Fatal("the installation did not complete")
	}
	binary := buildManagementCLI(t)
	platform := []string{
		"upgrade", "platform",
		"--kubeconfig", writeKubeconfig(t, cluster),
		"--definitions", harness.CRDDirectory(t),
		"--installed-framework", "1.0",
	}
	return cluster, database, binary, platform
}

// TestTC95PlatformUpgradeRefusesAnUnpinnedWorkloadsCopyElsewhere covers a cluster
// an older release left: the control plane serves a hostPath database but its
// workload pins no node, so the pod it runs is the only record of which machine's
// file it reads. Run from another host, the upgrade opens that host's copy of the
// path; naming the served node does not make the copy the served file. It refuses
// before replacing anything, and the pure platform form keeps the node the
// serving pod showed (ADM-C-54, ADM-C-55, ADM-TC-95).
func TestTC95PlatformUpgradeRefusesAnUnpinnedWorkloadsCopyElsewhere(t *testing.T) {
	cluster, database, binary, platform := installedControlPlane(t)
	host := harness.HostShortName(t)
	cluster.RegisterNode(t, host, host)
	unpinControlPlane(t, cluster)
	controlPlanePodOn(t, cluster, install.ControlPlaneName+"-abc", harness.ControlPlaneNode, database.Path)

	for _, testCase := range []struct {
		name string
		args []string
	}{
		// Naming this host's copy is the case the pin exists to stop: the node on
		// the command line says where the replacement should run, not where the
		// file came from, and this host's copy is another database (ADM-C-56).
		{"refuses this host's copy named with the served node", []string{
			"--identity-database", database.Path,
			"--control-plane-node", harness.ControlPlaneNode,
		}},
		{"refuses this host's copy named alone", []string{
			"--identity-database", database.Path,
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			output, err := exec.Command(binary, append(platform, testCase.args...)...).CombinedOutput()
			assertRefused(t, err, output, 2, database.Path, harness.ControlPlaneNode)
			assertWorkloadDelivery(t, cluster, database.Path, "")
			// The run opened no database: a refused upgrade neither audits nor
			// migrates the copy this host holds (ADM-C-56).
			assertNoMaintenanceRecordIn(t, database.Path, host)
		})
	}

	// The pure platform form opens no local file: the pod puts the served file on
	// its own node, so the delivery is kept rather than filled with this host.
	t.Run("keeps the node the serving pod shows", func(t *testing.T) {
		output, err := exec.Command(binary, platform...).CombinedOutput()
		if err != nil {
			t.Fatalf("platform upgrade of a workload whose pod shows its node = %v\n%s", err, output)
		}
		assertWorkloadDelivery(t, cluster, database.Path, harness.ControlPlaneNode)
	})
}

// TestTC95PlatformUpgradeRefusesAnUnpinnedWorkloadWithNoServingPod covers the
// same cluster before any pod shows where the file is: no running pod, or pods
// on two nodes, leaves the node unobservable. The upgrade refuses instead of
// pinning this host and serving a database nobody installed (ADM-C-54, ADM-C-56).
func TestTC95PlatformUpgradeRefusesAnUnpinnedWorkloadWithNoServingPod(t *testing.T) {
	cluster, database, binary, platform := installedControlPlane(t)
	host := harness.HostShortName(t)
	cluster.RegisterNode(t, host, host)
	unpinControlPlane(t, cluster)

	output, err := exec.Command(binary, platform...).CombinedOutput()
	assertRefused(t, err, output, 2, database.Path)
	assertWorkloadDelivery(t, cluster, database.Path, "")
	assertNoMaintenanceRecordIn(t, database.Path, host)
}

// withDefinitions points the platform form at another definition directory, so
// a case can render a marked copy and prove a refused run replaced none of it.
func withDefinitions(platform []string, definitions string) []string {
	replaced := append([]string(nil), platform...)
	for index := range replaced {
		if replaced[index] == "--definitions" && index+1 < len(replaced) {
			replaced[index+1] = definitions
		}
	}
	return replaced
}

// controlPlaneSnapshot is the stored workload a refused upgrade must leave
// alone: a write that changes nothing still bumps the resource version, so the
// snapshot is the object, not a projection of it.
func controlPlaneSnapshot(t *testing.T, cluster *harness.Cluster) *appsv1.Deployment {
	t.Helper()
	deployment, err := cluster.Kube.AppsV1().Deployments(install.DefaultNamespace).Get(
		context.Background(), install.ControlPlaneName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the control plane: %v", err)
	}
	return deployment.DeepCopy()
}

// assertControlPlaneUnchanged proves the refused run replaced no workload: the
// stored object still carries the snapshot's resource version, generation and
// pod template, so nothing was applied over it.
func assertControlPlaneUnchanged(t *testing.T, cluster *harness.Cluster, snapshot *appsv1.Deployment) {
	t.Helper()
	deployment, err := cluster.Kube.AppsV1().Deployments(install.DefaultNamespace).Get(
		context.Background(), install.ControlPlaneName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the control plane: %v", err)
	}
	if deployment.ResourceVersion != snapshot.ResourceVersion {
		t.Fatalf("the refused upgrade wrote the control plane: resourceVersion %s -> %s",
			snapshot.ResourceVersion, deployment.ResourceVersion)
	}
	if deployment.Generation != snapshot.Generation {
		t.Fatalf("the refused upgrade changed the control plane's spec: generation %d -> %d",
			snapshot.Generation, deployment.Generation)
	}
	if !reflect.DeepEqual(deployment.Spec.Template, snapshot.Spec.Template) {
		t.Fatalf("the refused upgrade changed the control plane's pod template")
	}
}

// TestTC95PlatformUpgradeIgnoresPodsThatOnlyLookLikeEvidence covers the ways an
// unpinned workload can appear to name a node without one pod actually serving
// the database the installation wrote. Pinning the replacement to such a node
// would serve an empty or unrelated database, and the accounts would be gone, so
// the upgrade refuses before it replaces anything: the object definitions and
// the Deployment stay as they were (ADM-C-54, ADM-C-56, ADM-TC-95).
func TestTC95PlatformUpgradeIgnoresPodsThatOnlyLookLikeEvidence(t *testing.T) {
	for _, testCase := range []struct {
		name string
		// pod places the only same-label pod the workload runs.
		pod func(t *testing.T, cluster *harness.Cluster, path string)
	}{
		{
			// The scheduler bound a replacement pod, but its container never
			// mounted the host directory: it is waiting, not serving.
			name: "bound but not started",
			pod: func(t *testing.T, cluster *harness.Cluster, path string) {
				pendingControlPlanePodOn(t, cluster, install.ControlPlaneName+"-pending", harness.ControlPlaneNode, path)
			},
		},
		{
			// Another application's pod happens to carry the workload's label.
			// It has no owner in the workload, runs no control plane, and
			// mounts no database.
			name: "same label another application",
			pod: func(t *testing.T, cluster *harness.Cluster, path string) {
				strangerPodOn(t, cluster, "other-app-abc", harness.ControlPlaneNode)
			},
		},
		{
			// A different deployment named kos-control-plane-canary owns a
			// ReplicaSet whose name shares the control plane's prefix. Its pod
			// is ready on another node and runs the same container, args and
			// mount; only the controller chain says it is not this workload's.
			name: "owned by another deployment",
			pod: func(t *testing.T, cluster *harness.Cluster, path string) {
				setUID := foreignReplicaSetOn(t, cluster)
				controlPlanePodOwnedBy(t, cluster, "kos-control-plane-canary-abc",
					harness.HostShortName(t), path, install.ControlPlaneName+"-canary-abc", setUID)
			},
		},
		{
			// The mount's root and container path match, but a subPath sends the
			// container's -database path to another host file. The replacement
			// renders no sub path, so this pod does not serve the file kept.
			name: "mount reads another file through a sub path",
			pod: func(t *testing.T, cluster *harness.Cluster, path string) {
				mountedThroughSubPathOn(t, cluster, "kos-control-plane-subpath", harness.ControlPlaneNode, path)
			},
		},
		{
			// The plain directory mount is still there, but a second volume
			// mounts the database file itself and hides it. The process reads
			// that file, and the replacement renders only the directory mount.
			name: "file mounted over the directory",
			pod: func(t *testing.T, cluster *harness.Cluster, path string) {
				fileMountedOverTheDirectoryOn(t, cluster, "kos-control-plane-shadowed", harness.ControlPlaneNode, path, path)
			},
		},
		{
			// The hidden file mount may spell the same container path with a
			// "." component, which the kernel resolves to the database file.
			// The string comparison must not let the plain directory mount
			// stand for it.
			name: "file mounted through an equivalent path",
			pod: func(t *testing.T, cluster *harness.Cluster, path string) {
				mountPath := filepath.Dir(path) + "/./" + filepath.Base(path)
				fileMountedOverTheDirectoryOn(t, cluster, "kos-control-plane-equivalent", harness.ControlPlaneNode, path, mountPath)
			},
		},
		{
			// Linux kubelet prefixes a relative mount path with the container
			// root, so this volume serves the database file even though the
			// stored string has no leading slash.
			name: "file mounted through a relative path",
			pod: func(t *testing.T, cluster *harness.Cluster, path string) {
				extraHostPathMountOn(t, cluster, "kos-control-plane-relative", harness.ControlPlaneNode, path,
					strings.TrimPrefix(path, "/"), filepath.Join(filepath.Dir(path), "other", "kos.db"))
			},
		},
		{
			// Two volumes whose mount paths normalize to the same directory are
			// applied in order, so the later one can serve a different file and
			// the pod no longer shows which volume the control plane reads.
			name: "two mounts at the same directory",
			pod: func(t *testing.T, cluster *harness.Cluster, path string) {
				extraHostPathMountOn(t, cluster, "kos-control-plane-duplicate", harness.ControlPlaneNode, path,
					filepath.Dir(path)+"/.", filepath.Join(filepath.Dir(path), "other"))
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cluster, database, binary, platform := installedControlPlane(t)
			host := harness.HostShortName(t)
			cluster.RegisterNode(t, host, host)
			unpinControlPlane(t, cluster)
			testCase.pod(t, cluster, database.Path)

			before := controlPlaneSnapshot(t, cluster)
			definitionsBefore := definitionSnapshot(t, cluster)
			definitions := alteredDefinitions(t, harness.CRDDirectory(t))
			output, err := exec.Command(binary, withDefinitions(platform, definitions)...).CombinedOutput()
			assertRefused(t, err, output, 2, database.Path)
			assertControlPlaneUnchanged(t, cluster, before)
			assertDefinitionsUnchanged(t, cluster, definitionsBefore)
			assertNoDefinitionReplaced(t, cluster)
			assertNoMaintenanceRecordIn(t, database.Path, host)
		})
	}
}
