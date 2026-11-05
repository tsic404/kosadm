package upgrade

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/kosd/api/v1beta1"
	"github.com/tsic404/kosd/pkg/controlplane"
)

// fakeMaintenance records the steps the flow drives and answers the way the
// real flow does while the applications are still running.
type fakeMaintenance struct {
	report      controlplane.MaintenanceReport
	drainErrs   int
	cordoned    []string
	drains      int
	restored    []string
	blocked     bool
	preflighted int
	// drainFailure names the node whose drain fails for good.
	drainFailure string
	// restoreFailure names the node whose restore fails.
	restoreFailure string
	// alreadyCordoned names the nodes another writer took out of scheduling
	// before the run, so the flow's own cordon writes nothing there.
	alreadyCordoned map[string]bool
}

func (f *fakeMaintenance) Preflight(context.Context, controlplane.MaintenanceRequest) (controlplane.MaintenanceReport, *controlplane.Error) {
	f.preflighted++
	if f.blocked {
		return controlplane.MaintenanceReport{}, &controlplane.Error{
			Code:    controlplane.CodeNotFound,
			Message: "node does not exist",
		}
	}
	return f.report, nil
}

func (f *fakeMaintenance) Cordon(_ context.Context, request controlplane.MaintenanceRequest, _ string) (controlplane.MaintenanceReport, bool, *controlplane.Error) {
	f.cordoned = append(f.cordoned, request.Node)
	return f.report, !f.alreadyCordoned[request.Node], nil
}

func (f *fakeMaintenance) Drain(_ context.Context, node, _ string) (controlplane.MaintenanceDrain, *controlplane.Error) {
	f.drains++
	if node == f.drainFailure {
		return controlplane.MaintenanceDrain{}, &controlplane.Error{
			Code:    controlplane.CodeConflict,
			Message: "the kernel refuses to evict a pod on " + node,
		}
	}
	if f.drainErrs > 0 {
		f.drainErrs--
		return controlplane.MaintenanceDrain{}, &controlplane.Error{
			Code:      controlplane.CodeConflict,
			Message:   "node-local 应用仍在运行",
			Retryable: true,
		}
	}
	return controlplane.MaintenanceDrain{Node: node, Evicted: []string{"apps/cache"}}, nil
}

func (f *fakeMaintenance) Restore(_ context.Context, node, _ string) *controlplane.Error {
	f.restored = append(f.restored, node)
	if node == f.restoreFailure {
		return &controlplane.Error{Code: controlplane.CodeWriteFailed, Message: "the node write failed"}
	}
	return nil
}

// fakeApplications answers the health the preflight counts.
type fakeApplications struct {
	states []v1beta1.HealthState
}

func (f fakeApplications) Health(context.Context) ([]v1beta1.HealthState, error) {
	return f.states, nil
}

// fakeNodes reports one node's version, which a case changes to end the
// maintenance.
type fakeNodes struct {
	version string
}

func (f *fakeNodes) KubeletVersion(context.Context, string) (string, error) {
	return f.version, nil
}

// flowUnderTest builds a flow over the fakes.
func flowUnderTest(maintained *fakeMaintenance, nodes *fakeNodes) *Flow {
	return &Flow{
		Maintenance:  maintained,
		Applications: fakeApplications{states: []v1beta1.HealthState{v1beta1.HealthStateHealthy, v1beta1.HealthStateDegraded}},
		Nodes:        nodes,
		Poll:         time.Millisecond,
		Wait:         2 * time.Second,
	}
}

// TestPreflightRefusesAnUndeterminedUpgrade checks that an item the flow cannot
// judge stops the upgrade before any node is taken out (C-55, C-65).
func TestPreflightRefusesAnUndeterminedUpgrade(t *testing.T) {
	maintained := &fakeMaintenance{report: controlplane.MaintenanceReport{
		Node:         "node-a",
		Applications: []string{"default/shop"},
		Downtime:     "10m",
		Undetermined: []controlplane.MaintenanceUndetermined{{Item: "卷 apps/data", Reason: "卷没有记录数据所在节点"}},
	}}
	flow := flowUnderTest(maintained, &fakeNodes{version: "v1.37.0"})
	request := Request{Nodes: []string{"node-a"}, Version: "v1.38.0", Downtime: "10m"}

	report, failure := flow.Preflight(context.Background(), request)
	if failure != nil {
		t.Fatalf("preflight: %v", failure)
	}
	if !report.Blocked || len(report.Undetermined) != 1 {
		t.Fatalf("report = %+v, want a blocked report naming the unknown item", report)
	}
	if len(report.Nodes) != 1 || len(report.Nodes[0].Applications) != 1 || report.Nodes[0].Applications[0] != "default/shop" {
		t.Fatalf("node plan = %+v, want the affected application", report.Nodes)
	}
	if report.UnhealthyApplications != 1 {
		t.Fatalf("unhealthy applications = %d, want 1", report.UnhealthyApplications)
	}
	if _, failure := flow.Begin(context.Background(), request, "owner"); failure == nil {
		t.Fatal("a blocked preflight started the upgrade")
	}
	if len(maintained.cordoned) != 0 {
		t.Fatalf("the blocked upgrade cordoned %v", maintained.cordoned)
	}
}

// TestBeginWaitsForTheStopBeforeDraining checks the ordering rule: the drain is
// retried while the node-local applications run, and the node is only taken out
// once (C-65).
func TestBeginWaitsForTheStopBeforeDraining(t *testing.T) {
	maintained := &fakeMaintenance{
		report:    controlplane.MaintenanceReport{Node: "node-a", Applications: []string{"default/shop"}, Downtime: "10m"},
		drainErrs: 2,
	}
	flow := flowUnderTest(maintained, &fakeNodes{version: "v1.37.0"})
	request := Request{Nodes: []string{"node-a"}, Version: "v1.38.0", Downtime: "10m"}

	result, failure := flow.Begin(context.Background(), request, "owner")
	if failure != nil {
		t.Fatalf("begin: %v", failure)
	}
	if len(maintained.cordoned) != 1 || maintained.cordoned[0] != "node-a" {
		t.Fatalf("cordoned %v, want node-a once", maintained.cordoned)
	}
	if maintained.drains != 3 {
		t.Fatalf("drains = %d, want the two refusals and the successful one", maintained.drains)
	}
	if len(result.Nodes) != 1 || len(result.Nodes[0].Drain.Evicted) != 1 {
		t.Fatalf("result = %+v, want the drain report", result)
	}
}

// TestBeginPutsBackTheNodesItTookOutWhenAStepFails covers C-55's failure path:
// a run that never started must not leave the nodes it cordoned out of
// scheduling, and the result names the nodes it put back.
func TestBeginPutsBackTheNodesItTookOutWhenAStepFails(t *testing.T) {
	maintained := &fakeMaintenance{
		report:       controlplane.MaintenanceReport{Node: "node-a", Downtime: "10m"},
		drainFailure: "node-b",
	}
	flow := flowUnderTest(maintained, &fakeNodes{version: "v1.37.0"})
	request := Request{Nodes: []string{"node-a", "node-b"}, Version: "v1.38.0", Downtime: "10m"}

	result, failure := flow.Begin(context.Background(), request, "owner")
	if failure == nil {
		t.Fatal("a drain the kernel refused was reported as a started upgrade")
	}
	if len(maintained.cordoned) != 2 {
		t.Fatalf("cordoned %v, want both nodes before the failure", maintained.cordoned)
	}
	if len(result.RolledBack) != 2 || result.RolledBack[0] != "node-a" || result.RolledBack[1] != "node-b" {
		t.Fatalf("rolled back %v, want both nodes put back", result.RolledBack)
	}
	if len(maintained.restored) != 2 {
		t.Fatalf("restored %v, want both nodes", maintained.restored)
	}
}

// TestBeginNamesTheNodeItCouldNotPutBack covers the failure of the failure path:
// a node the restore cannot lift stays out of scheduling, so the answer names it
// instead of claiming the rollback completed (C-55).
func TestBeginNamesTheNodeItCouldNotPutBack(t *testing.T) {
	maintained := &fakeMaintenance{
		report:         controlplane.MaintenanceReport{Node: "node-a", Downtime: "10m"},
		drainFailure:   "node-b",
		restoreFailure: "node-b",
	}
	flow := flowUnderTest(maintained, &fakeNodes{version: "v1.37.0"})
	request := Request{Nodes: []string{"node-a", "node-b"}, Version: "v1.38.0", Downtime: "10m"}

	result, failure := flow.Begin(context.Background(), request, "owner")
	if failure == nil {
		t.Fatal("a drain the kernel refused was reported as a started upgrade")
	}
	if len(result.RolledBack) != 1 || result.RolledBack[0] != "node-a" {
		t.Fatalf("rolled back %v, want only the node it restored", result.RolledBack)
	}
	if !strings.Contains(failure.Message, "node-b") || !strings.Contains(failure.Message, "保持不可调度") {
		t.Fatalf("failure %q does not name the node left out of scheduling", failure.Message)
	}
}

// TestRunRestoresOnlyAfterTheNodeReportsTheTargetVersion checks that a node that
// never reaches the target version stays out of scheduling (C-55).
func TestRunRestoresOnlyAfterTheNodeReportsTheTargetVersion(t *testing.T) {
	maintained := &fakeMaintenance{report: controlplane.MaintenanceReport{Node: "node-a", Downtime: "10m"}}
	nodes := &fakeNodes{version: "v1.37.0"}
	flow := flowUnderTest(maintained, nodes)
	flow.Wait = 50 * time.Millisecond
	request := Request{Nodes: []string{"node-a"}, Version: "v1.38.0", Downtime: "10m"}

	if _, failure := flow.Run(context.Background(), request, "owner"); failure == nil {
		t.Fatal("the run restored a node that never reported the target version")
	}
	if len(maintained.restored) != 0 {
		t.Fatalf("restored %v without the version change", maintained.restored)
	}

	// Once the node reports the version, the same run restores it.
	nodes.version = "v1.38.0"
	maintained.drains = 0
	if _, failure := flow.Run(context.Background(), request, "owner"); failure != nil {
		t.Fatalf("run after the version change: %v", failure)
	}
	if len(maintained.restored) != 1 || maintained.restored[0] != "node-a" {
		t.Fatalf("restored %v, want node-a", maintained.restored)
	}
}

// TestFinishRestoresWhatTheBeginHalfCordoned checks the restore half on its own:
// it lifts the cordons the run wrote and nothing else.
func TestFinishRestoresWhatTheBeginHalfCordoned(t *testing.T) {
	maintained := &fakeMaintenance{}
	flow := flowUnderTest(maintained, &fakeNodes{})
	begun := Result{Cordoned: []string{"node-a", "node-b"}}
	if _, failure := flow.Finish(context.Background(), begun, "owner"); failure != nil {
		t.Fatalf("finish: %v", failure)
	}
	if len(maintained.restored) != 2 || maintained.restored[0] != "node-a" || maintained.restored[1] != "node-b" {
		t.Fatalf("restored %v, want the nodes the run cordoned", maintained.restored)
	}
	if maintained.preflighted != 0 {
		t.Fatalf("the restore half ran %d preflights, want none", maintained.preflighted)
	}
}

// TestBeginLeavesACordonItDidNotWriteAlone covers the boundary of the rollback:
// a node another writer took out of scheduling before the run keeps that state,
// even when a later step of the run fails (C-55).
func TestBeginLeavesACordonItDidNotWriteAlone(t *testing.T) {
	maintained := &fakeMaintenance{
		report:          controlplane.MaintenanceReport{Node: "node-a", Downtime: "10m"},
		alreadyCordoned: map[string]bool{"node-b": true},
		drainFailure:    "node-b",
	}
	flow := flowUnderTest(maintained, &fakeNodes{version: "v1.37.0"})
	request := Request{Nodes: []string{"node-a", "node-b"}, Version: "v1.38.0", Downtime: "10m"}

	result, failure := flow.Begin(context.Background(), request, "owner")
	if failure == nil {
		t.Fatal("a drain the kernel refused was reported as a started upgrade")
	}
	if len(result.Cordoned) != 1 || result.Cordoned[0] != "node-a" {
		t.Fatalf("cordoned = %v, want only the node this run wrote", result.Cordoned)
	}
	if len(maintained.restored) != 1 || maintained.restored[0] != "node-a" {
		t.Fatalf("restored %v, want only the node this run wrote", maintained.restored)
	}
	if len(result.RolledBack) != 1 || result.RolledBack[0] != "node-a" {
		t.Fatalf("rolled back %v, want only the node this run wrote", result.RolledBack)
	}
}

// TestFinishLeavesACordonItDidNotWriteAlone covers the same boundary on the
// restore half: the run finishes without lifting a cordon it never set.
func TestFinishLeavesACordonItDidNotWriteAlone(t *testing.T) {
	maintained := &fakeMaintenance{alreadyCordoned: map[string]bool{"node-b": true}}
	flow := flowUnderTest(maintained, &fakeNodes{version: "v1.38.0"})
	request := Request{Nodes: []string{"node-a", "node-b"}, Version: "v1.38.0", Downtime: "10m"}

	result, failure := flow.Run(context.Background(), request, "owner")
	if failure != nil {
		t.Fatalf("run: %v", failure)
	}
	if len(result.Cordoned) != 1 || result.Cordoned[0] != "node-a" {
		t.Fatalf("cordoned = %v, want only the node this run wrote", result.Cordoned)
	}
	if len(maintained.restored) != 1 || maintained.restored[0] != "node-a" {
		t.Fatalf("restored %v, want only the node this run wrote", maintained.restored)
	}
}
