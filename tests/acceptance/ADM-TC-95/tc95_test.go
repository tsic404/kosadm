// Package admtc95 verifies C-55's cluster upgrade over a node that holds a
// node-local application: the preflight names the application and the declared
// downtime, the upgrade stops it before the node is drained, the restore brings
// it back with its data in place, an extension upgrade reports the contribution
// difference item by item, and a replacement with an unmet dependency is
// refused with the reason (ADM-AC-48, ADM-TC-95).
package admtc95

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/tsic404/kosadm/pkg/maintenance"
	"github.com/tsic404/kosadm/pkg/upgrade"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/api/v1beta1"
	"github.com/tsic404/kosd/pkg/app"
	"github.com/tsic404/kosd/pkg/controlplane"
	"github.com/tsic404/kosd/pkg/extension"
	"github.com/tsic404/kosd/pkg/identity"
	"github.com/tsic404/kosd/pkg/reconcile"
	"github.com/tsic404/kosd/pkg/settings/kossettings"
)

const (
	appsNamespace = "apps"
	application   = "shop"
	brokenApp     = "broken"
	nodeName      = "node-a"
	volumeName    = "data"
	storageClass  = "local"
	claimName     = "data"
	ownerName     = "owner"
	downtime      = "10m"
	fromVersion   = "v1.37.0"
	targetVersion = "v1.38.0"
	product       = "demo"
)

// shopTemplate renders the Deployment of the node-local application.
const shopTemplate = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Name }}
  namespace: {{ .Namespace }}
spec:
  replicas: 2
  selector:
    matchLabels:
      app: {{ .Name }}
  template:
    metadata:
      labels:
        app: {{ .Name }}
    spec:
      containers:
        - name: app
          image: nginx:1.27
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: {{ index .Volumes "data" }}
`

// TestTC95UpgradePreflightStopsAndRestores runs one cluster upgrade and one
// extension upgrade over the same cluster.
func TestTC95UpgradePreflightStopsAndRestores(t *testing.T) {
	cluster := harness.Start(t)
	// The maintenance records land in the control-plane database, which is the
	// trail the node setting's origin is judged from (C-14, C-65).
	surface := harness.OpenIdentity(t, nil, nil)
	cluster.EnsureNamespace(t, appsNamespace)
	cluster.StorageClass(t, storageClass, "local.example", false, "Delete", nil)
	cluster.CreateNode(t, nodeName, fromVersion)
	cluster.CreateClaim(t, appsNamespace, claimName, storageClass)
	cluster.CreateIn(t, appsNamespace, boundVolume())
	cluster.InstallDefinition(t, harness.TemplateDefinition(t, application, shopTemplate, nil))

	gateway := cluster.StartControlPlane(t, func(config *controlplane.Config) {
		config.Maintenance = maintenanceService(cluster, config.Auditor)
	})
	manifest := harness.ApplicationManifest(application, harness.Namespace, application, appsNamespace, nil, []string{volumeName})
	manifest["spec"].(map[string]any)["replicas"] = int64(2)
	if status, body := gateway.Call(t, http.MethodPost, "/api/v1/resources/Application", manifest, nil); status != http.StatusAccepted {
		t.Fatalf("create the application: status %d, body %v", status, body)
	}
	createUnhealthyApplication(t, cluster)

	controller := cluster.ApplicationController(t, app.Options{})
	harness.Eventually(t, 30*time.Second, "the application reaches Ready", func() bool {
		cluster.ApplicationRound(t, controller, application)
		return cluster.ApplicationPhase(t, application) == app.PhaseReady
	})
	cluster.SetWorkloadStatus(t, "Deployment", appsNamespace, application, 2, 2)
	cluster.ApplicationRound(t, controller, application)

	// The node carries one pod of the node-local application and one pod nothing
	// binds to it: the drain must keep the first and move the second, and it may
	// only do so after the application stopped.
	shopPod := cluster.CreatePod(t, appsNamespace, "shop-abc-1", nodeName, "shop")
	movablePod := cluster.CreatePod(t, appsNamespace, "batch-abc-1", nodeName, "")

	flow := &upgrade.Flow{
		Maintenance:  maintenanceService(cluster, surface.Trail),
		Applications: upgrade.ClusterApplications{Dynamic: cluster.Dynamic},
		Nodes:        upgrade.ClusterNodes{Dynamic: cluster.Dynamic},
		Poll:         20 * time.Millisecond,
		Wait:         60 * time.Second,
	}
	request := upgrade.Request{Nodes: []string{nodeName}, Version: targetVersion, Downtime: downtime}

	// The preflight names what the upgrade affects and what it cannot judge
	// (C-55).
	report, failure := flow.Preflight(context.Background(), request)
	if failure != nil {
		t.Fatalf("preflight: %v", failure)
	}
	if len(report.Nodes) != 1 {
		t.Fatalf("the preflight plans %d nodes, want 1", len(report.Nodes))
	}
	plan := report.Nodes[0]
	if plan.Node != nodeName || plan.KubeletVersion != fromVersion {
		t.Fatalf("node plan = %+v, want %s at %s", plan, nodeName, fromVersion)
	}
	if len(plan.Applications) != 1 || plan.Applications[0] != harness.Namespace+"/"+application {
		t.Fatalf("affected applications = %v, want [%s/%s]", plan.Applications, harness.Namespace, application)
	}
	if len(plan.Volumes) != 1 || plan.Volumes[0] != appsNamespace+"/"+volumeName {
		t.Fatalf("affected volumes = %v, want [%s/%s]", plan.Volumes, appsNamespace, volumeName)
	}
	if plan.Downtime != downtime {
		t.Fatalf("downtime = %q, want %q", plan.Downtime, downtime)
	}
	if report.UnhealthyApplications != 1 {
		t.Fatalf("unhealthy applications = %d, want 1", report.UnhealthyApplications)
	}
	if report.Blocked {
		t.Fatalf("the preflight blocks an upgrade it can judge: %v", report.Undetermined)
	}

	// The drain refuses while the node-local application still runs, so the stop
	// always precedes the eviction, and a run that fails there puts the node back
	// instead of leaving it out of scheduling (C-55, C-65).
	impatient := &upgrade.Flow{
		Maintenance:  maintenanceService(cluster, surface.Trail),
		Applications: upgrade.ClusterApplications{Dynamic: cluster.Dynamic},
		Nodes:        upgrade.ClusterNodes{Dynamic: cluster.Dynamic},
		Poll:         20 * time.Millisecond,
		Wait:         200 * time.Millisecond,
	}
	if _, failure := impatient.Begin(context.Background(), request, ownerName); failure == nil {
		t.Fatal("the upgrade drained a node whose node-local application still runs")
	}
	if cluster.NodeUnschedulable(t, nodeName) {
		t.Fatal("the failed begin left the node out of scheduling")
	}
	if !cluster.PodExists(t, appsNamespace, shopPod) || !cluster.PodExists(t, appsNamespace, movablePod) {
		t.Fatal("the refused drain moved a pod")
	}

	// Begin takes the node out and waits for the application to stop; the
	// application's own reconciliation performs the stop (C-65).
	stopRounds := driveRounds(cluster, controller)
	result, failure := flow.Begin(context.Background(), request, ownerName)
	stopRounds()
	if failure != nil {
		t.Fatalf("begin the upgrade: %v", failure)
	}
	if !cluster.NodeUnschedulable(t, nodeName) {
		t.Fatal("the upgrade did not take the node out of scheduling")
	}
	if replicas := cluster.WorkloadReplicas(t, "Deployment", appsNamespace, application); replicas != 0 {
		t.Fatalf("the application still runs %d replicas during the upgrade", replicas)
	}
	if len(result.Nodes) != 1 {
		t.Fatalf("the upgrade reports %d node results, want 1", len(result.Nodes))
	}
	drain := result.Nodes[0].Drain
	if !only(drain.Evicted, appsNamespace+"/"+movablePod) {
		t.Fatalf("evicted = %v, want [%s/%s]", drain.Evicted, appsNamespace, movablePod)
	}
	if !harness.RetainedPods(drain)[appsNamespace+"/"+shopPod] {
		t.Fatalf("retained = %+v, want the node-local application's pod", drain.Retained)
	}
	if cluster.PodExists(t, appsNamespace, movablePod) {
		t.Fatal("the drain left the movable pod on the node")
	}
	if !cluster.PodExists(t, appsNamespace, shopPod) {
		t.Fatal("the drain moved the node-local application off its node")
	}
	if !cluster.ClaimExists(t, appsNamespace, claimName) {
		t.Fatal("the upgrade removed the claim that holds the data")
	}
	if node := cluster.VolumeNode(t, appsNamespace, volumeName); node != nodeName {
		t.Fatalf("the data moved to node %q, want %q", node, nodeName)
	}

	// The cordon is recorded as the operator's action, so the node setting
	// reports planned maintenance for the window (C-14, C-65).
	if entries := maintenanceRecords(t, surface, nodeName); len(entries) == 0 ||
		entries[0].Verb != controlplane.VerbMaintain || entries[0].Actor != ownerName {
		t.Fatalf("newest maintenance record = %+v, want an accepted maintain by %s", entries, ownerName)
	}
	if origin := maintenanceOrigin(t, cluster, surface, nodeName); origin != kossettings.MaintenancePlanned {
		t.Fatalf("maintenance origin = %q, want %q", origin, kossettings.MaintenancePlanned)
	}

	// The operator finishes the node, which reports the target version, and the
	// upgrade restores what it stopped (C-55).
	cluster.SetKubeletVersion(t, nodeName, targetVersion)
	if _, failure := flow.Finish(context.Background(), result, ownerName); failure != nil {
		t.Fatalf("finish the upgrade: %v", failure)
	}
	if cluster.NodeUnschedulable(t, nodeName) {
		t.Fatal("the upgrade left the node out of scheduling")
	}
	harness.Eventually(t, 30*time.Second, "the application returns to Ready", func() bool {
		cluster.ApplicationRound(t, controller, application)
		return cluster.WorkloadReplicas(t, "Deployment", appsNamespace, application) == 2 &&
			cluster.ApplicationPhase(t, application) == app.PhaseReady
	})
	if !cluster.ClaimExists(t, appsNamespace, claimName) || cluster.VolumeNode(t, appsNamespace, volumeName) != nodeName {
		t.Fatal("the data moved during the upgrade")
	}
	if entries := maintenanceRecords(t, surface, nodeName); len(entries) == 0 ||
		entries[0].Verb != controlplane.VerbRestore || entries[0].Actor != ownerName {
		t.Fatalf("newest maintenance record = %+v, want an accepted restore by %s", entries, ownerName)
	}

	// The extension upgrade reports its contribution difference item by item
	// (C-55).
	checkExtensionUpgrade(t, cluster)
}

// checkExtensionUpgrade installs one product, preflights its upgrade and applies
// it, asserting the difference the preflight reports (C-55).
func checkExtensionUpgrade(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	artifacts := extension.MemoryArtifacts{}
	before := extension.Declaration{
		Identifier: product,
		Version:    "1.0.0",
		Contributions: extension.Contributions{
			Pages: []extension.Page{demoPage()},
		},
	}
	cluster.InstallPackageWith(t, product, before, artifacts, nil)

	after := extension.Declaration{
		Identifier: product,
		Version:    "2.0.0",
		Contributions: extension.Contributions{
			Settings: []extension.Setting{{
				ID: "demo-setting", Scope: extension.SettingScopeCluster, Writer: extension.SettingWriterUser, Editable: true,
			}},
		},
	}
	content, err := extension.Marshal(after)
	if err != nil {
		t.Fatalf("marshal the target declaration: %v", err)
	}
	reference := "test://" + product + "-2"
	artifacts[reference] = content
	digest := extension.DigestOf(content)

	preflight, err := cluster.ExtensionInstaller(artifacts).UpgradePreflight(context.Background(), []extension.UpgradeTarget{{
		Component: product, Reference: reference, Digest: digest,
	}})
	if err != nil {
		t.Fatalf("preflight the extension upgrade: %v", err)
	}
	if len(preflight.Components) != 1 {
		t.Fatalf("the extension preflight reports %d components, want 1", len(preflight.Components))
	}
	changes := preflight.Components[0].Changes
	if len(changes) != 2 {
		t.Fatalf("contribution changes = %+v, want one per moved class", changes)
	}
	if change := changeOf(t, changes, extension.ClassPages); !only(change.Removed, "demo") || len(change.Added) != 0 {
		t.Fatalf("pages change = %+v, want demo removed", change)
	}
	if change := changeOf(t, changes, extension.ClassSettings); !only(change.Added, "demo-setting") || len(change.Removed) != 0 {
		t.Fatalf("settings change = %+v, want demo-setting added", change)
	}
	if preflight.IsBlocked {
		t.Fatalf("the extension upgrade is blocked: %+v", preflight.Components[0].Problems)
	}

	// The upgrade itself is the replacement the component declares, judged by
	// the same evaluation an install runs (C-55).
	cluster.RepointComponent(t, product, reference, digest)
	installation := cluster.ExtensionState(t, artifacts)
	component, found := installation.Component(product)
	if !found {
		t.Fatal("the upgraded component disappeared from the installation")
	}
	if len(component.Contributions.Pages) != 0 {
		t.Fatalf("the upgraded product still contributes pages: %+v", component.Contributions.Pages)
	}
	if _, served := installation.Registry().Setting("demo-setting"); !served {
		t.Fatal("the upgraded product does not contribute its new setting")
	}

	// A replacement whose declared dependency nothing provides is refused and
	// the reason names the dependency; the component keeps the package the
	// successful upgrade applied (C-55).
	blocked := extension.Declaration{
		Identifier: product,
		Version:    "3.0.0",
		Dependencies: extension.Dependencies{Capabilities: []extension.CapabilityRequirement{{
			Name: "demo.absent/capability", Versions: "v1",
		}}},
	}
	content, err = extension.Marshal(blocked)
	if err != nil {
		t.Fatalf("marshal the refused declaration: %v", err)
	}
	refusedReference := "test://" + product + "-3"
	artifacts[refusedReference] = content
	refused, err := cluster.ExtensionInstaller(artifacts).UpgradePreflight(context.Background(), []extension.UpgradeTarget{{
		Component: product, Reference: refusedReference, Digest: extension.DigestOf(content),
	}})
	if err != nil {
		t.Fatalf("preflight the refused extension upgrade: %v", err)
	}
	if !refused.IsBlocked || !refused.Components[0].IsBlocked {
		t.Fatalf("the extension upgrade with an unmet dependency was accepted: %+v", refused.Components[0])
	}
	if problems := strings.Join(refused.Components[0].Problems, "; "); !strings.Contains(problems, "demo.absent/capability") {
		t.Fatalf("problems = %q, want the missing capability named", problems)
	}
	stored := cluster.Get(t, "v1beta1", "SystemComponent", product)
	kept, _, err := unstructured.NestedString(stored.Object, "spec", "package", "reference")
	if err != nil || kept != reference {
		t.Fatalf("the refused upgrade moved the component to %q, want %q", kept, reference)
	}
}

// demoPage is the page the first version of the product contributes.
func demoPage() extension.Page {
	return extension.Page{
		ID:             "demo",
		Title:          "演示",
		Location:       "panel",
		ReadOnly:       true,
		ReadOnlyReason: "本页只读：演示",
		Items: []extension.PageItem{{
			ID: "demo-item", Label: "演示", Source: extension.PageSourceComputed, Reference: "demo/demo-item",
		}},
	}
}

// changeOf reads one class's difference.
func changeOf(t *testing.T, changes []extension.ContributionChange, class string) extension.ContributionChange {
	t.Helper()
	for _, change := range changes {
		if change.Class == class {
			return change
		}
	}
	t.Fatalf("the preflight reports no change for %q: %+v", class, changes)
	return extension.ContributionChange{}
}

// driveRounds advances the application reconciler in the background until the
// returned function is called. The loop never touches the test, so it is safe
// to run beside the upgrade: the case under test is the orchestration.
func driveRounds(cluster *harness.Cluster, controller *app.Controller) func() {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			select {
			case <-done:
				return
			default:
			}
			_, _ = controller.Reconcile(context.Background(), reconcile.Request{
				Kind: "Application", Namespace: harness.Namespace, Name: application,
			})
			time.Sleep(10 * time.Millisecond)
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}

// maintenanceService builds the C-65 flow over the case's cluster.
func maintenanceService(cluster *harness.Cluster, auditor controlplane.Auditor) *maintenance.Service {
	return &maintenance.Service{
		Dynamic: cluster.Dynamic,
		Writer:  cluster.Writer(),
		Pods:    maintenance.Cluster{Kube: cluster.Kube},
		Auditor: auditor,
	}
}

// maintenanceRecords reads one node's accepted maintenance records the way the
// settings projection reads them (C-14, C-65).
func maintenanceRecords(t *testing.T, surface *harness.Identity, node string) []identity.AuditEntry {
	t.Helper()
	entries, err := surface.Store.RecentAuditEntriesForObject(context.Background(),
		api.Group+"/Node/"+node,
		[]string{controlplane.VerbMaintain, controlplane.VerbRestore},
		[]string{controlplane.ResultAccepted}, 10)
	if err != nil {
		t.Fatalf("read the maintenance trail of %s: %v", node, err)
	}
	return entries
}

// maintenanceOrigin reads one node's maintenance origin as the settings page
// reports it (C-36, C-65).
func maintenanceOrigin(t *testing.T, cluster *harness.Cluster, surface *harness.Identity, node string) string {
	t.Helper()
	readings := &kossettings.Readings{Dynamic: cluster.Dynamic, Audit: surface.Store}
	reading, failure := readings.Read(context.Background(), identity.Session{},
		extension.PageItem{ID: kossettings.NodeMaintenanceOrigin})
	if failure != nil {
		t.Fatalf("read the maintenance origin: %v", failure)
	}
	encoded, err := json.Marshal(reading.Value)
	if err != nil {
		t.Fatalf("encode the maintenance origin: %v", err)
	}
	origins := []struct {
		Name   string `json:"name"`
		Origin string `json:"origin"`
	}{}
	if err := json.Unmarshal(encoded, &origins); err != nil {
		t.Fatalf("decode the maintenance origin %s: %v", encoded, err)
	}
	for _, origin := range origins {
		if origin.Name == node {
			return origin.Origin
		}
	}
	t.Fatalf("the maintenance origin does not name node %s: %s", node, encoded)
	return ""
}

// createUnhealthyApplication writes one application whose health is Degraded,
// which the platform upgrade warns about (C-55).
func createUnhealthyApplication(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	body := harness.Manifest("Application", harness.Namespace, brokenApp, nil, nil)
	body["spec"] = map[string]any{"definitionRef": "missing", "namespace": appsNamespace}
	body["status"] = map[string]any{"phase": "Failed", "health": map[string]any{"state": string(v1beta1.HealthStateDegraded)}}
	cluster.Create(t, &unstructured.Unstructured{Object: body})
}

// boundVolume is the node-local volume whose data sits on the case's node.
func boundVolume() *unstructured.Unstructured {
	object := harness.Volume(appsNamespace, volumeName, map[string]any{
		"owner":        ownerName,
		"mobility":     "node-local",
		"capacity":     "1Gi",
		"storageClass": storageClass,
	})
	object.Object["status"] = map[string]any{"node": nodeName}
	return object
}

// only reports whether values holds exactly want.
func only(values []string, want string) bool {
	return len(values) == 1 && values[0] == want
}
