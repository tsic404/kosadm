// Package upgrade drives the platform upgrade flow of C-55 for the part the run
// kernel can move: a cluster upgrade stops the node-local applications bound to
// a node before the kernel evicts it, and restores them once the node reports
// the target version.
//
// The steps are the node maintenance flow of C-65. This package only sequences
// them and adds what makes an upgrade a first-class flow: the preflight that
// names the affected applications and the declared downtime, the unhealthy
// application count the upgrade warns about, and the item it cannot judge.
//
// The target-version gate lives here: a node ends its maintenance when it
// reports the version this flow waits for. kosd keeps the reconciler side: an
// application bound to an unschedulable node stops itself and returns once the
// node is schedulable again (C-65).
package upgrade

import (
	"context"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/api/v1beta1"
	"github.com/tsic404/kosd/pkg/controlplane"
)

// applicationResource is where the health of an application is read (C-08).
var applicationResource = schema.GroupVersionResource{Group: api.Group, Version: "v1beta1", Resource: "applications"}

// Defaults bound how long one step waits for the run kernel to catch up.
const (
	defaultPoll = time.Second
	defaultWait = 10 * time.Minute
)

// Request is one cluster upgrade (C-55).
type Request struct {
	// Nodes are the nodes the upgrade takes out, in the order it takes them.
	Nodes []string `json:"nodes"`
	// Version is the version the upgrade brings the nodes to. A node ends its
	// maintenance when it reports this version (C-55).
	Version string `json:"version"`
	// Downtime is the declared estimate of how long one node stays out. The
	// preflight reports it per node and marks it undetermined when absent.
	Downtime string `json:"downtime,omitempty"`
}

// Report is the upgrade preflight (C-55): what the upgrade affects per node and
// what it cannot judge.
type Report struct {
	// Nodes carries one plan per node the upgrade takes out.
	Nodes []NodePlan `json:"nodes"`
	// UnhealthyApplications counts the applications whose health is not
	// Healthy, which the upgrade warns about without blocking on them (C-55).
	UnhealthyApplications int `json:"unhealthyApplications"`
	// Undetermined lists the items the preflight could not judge; any of them
	// blocks the upgrade (C-55, C-65).
	Undetermined []string `json:"undetermined,omitempty"`
	// Blocked reports whether an undetermined item stops the upgrade.
	Blocked bool `json:"blocked"`
}

// NodePlan is what one node's maintenance affects (C-55).
type NodePlan struct {
	// Node is the node the upgrade takes out.
	Node string `json:"node"`
	// Applications are the node-local applications it stops.
	Applications []string `json:"applications"`
	// Volumes are the node-local volumes whose data sits on it.
	Volumes []string `json:"volumes"`
	// Downtime is the declared per-node estimate.
	Downtime string `json:"downtime,omitempty"`
	// KubeletVersion is the version the node reports before the upgrade.
	KubeletVersion string `json:"kubeletVersion,omitempty"`
}

// NodeResult is what one node's maintenance did.
type NodeResult struct {
	// Node is the node that was taken out.
	Node string `json:"node"`
	// Drain is what the stop-and-drain step moved off it.
	Drain controlplane.MaintenanceDrain `json:"drain"`
}

// Result is what one upgrade run did.
type Result struct {
	// Preflight is the report the run was accepted with.
	Preflight Report `json:"preflight"`
	// Nodes is what each node's maintenance did, in the order it ran.
	Nodes []NodeResult `json:"nodes"`
	// Cordoned are the nodes this run took out of scheduling itself. A node that
	// was already out of scheduling takes no second write, so it is not listed:
	// the restore half lifts only the cordons this run wrote.
	Cordoned []string `json:"cordoned,omitempty"`
	// RolledBack are the nodes a failed Begin took out and put back. A node it
	// could not put back is not listed: the failure names it instead, because
	// that node stays out of scheduling until an operator lifts the cordon.
	RolledBack []string `json:"rolledBack,omitempty"`
}

// Applications reports the health of the installed applications, which a
// platform upgrade warns about before it starts (C-55, C-08).
type Applications interface {
	// Health returns one state per application.
	Health(ctx context.Context) ([]v1beta1.HealthState, error)
}

// ClusterApplications reads application health from the run kernel.
type ClusterApplications struct {
	// Dynamic reads the application objects; required.
	Dynamic dynamic.Interface
}

// Health implements Applications.
func (c ClusterApplications) Health(ctx context.Context) ([]v1beta1.HealthState, error) {
	objects, err := c.Dynamic.Resource(applicationResource).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	states := make([]v1beta1.HealthState, 0, len(objects.Items))
	for index := range objects.Items {
		var application v1beta1.Application
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(objects.Items[index].Object, &application); err != nil {
			continue
		}
		states = append(states, application.Status.Health.State)
	}
	return states, nil
}

// Nodes reports the version a node runs, which is what ends its maintenance
// (C-55).
type Nodes interface {
	// KubeletVersion returns the version the node reports.
	KubeletVersion(ctx context.Context, node string) (string, error)
}

// ClusterNodes reads node versions from the run kernel.
type ClusterNodes struct {
	// Dynamic reads the node objects; required.
	Dynamic dynamic.Interface
}

// KubeletVersion implements Nodes.
func (c ClusterNodes) KubeletVersion(ctx context.Context, node string) (string, error) {
	object, err := c.Dynamic.Resource(schema.GroupVersionResource{Version: "v1", Resource: "nodes"}).Get(ctx, node, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	version, _, err := unstructured.NestedString(object.Object, "status", "nodeInfo", "kubeletVersion")
	return version, err
}

// Maintenance is the node maintenance flow the upgrade sequences (C-65).
// *maintenance.Service implements it.
type Maintenance interface {
	Preflight(ctx context.Context, request controlplane.MaintenanceRequest) (controlplane.MaintenanceReport, *controlplane.Error)
	Cordon(ctx context.Context, request controlplane.MaintenanceRequest, actor string) (controlplane.MaintenanceReport, bool, *controlplane.Error)
	Drain(ctx context.Context, node, actor string) (controlplane.MaintenanceDrain, *controlplane.Error)
	Restore(ctx context.Context, node, actor string) *controlplane.Error
}

// Flow drives one cluster upgrade through the node maintenance steps.
type Flow struct {
	// Maintenance drives the C-65 steps; required.
	Maintenance Maintenance
	// Applications reports the health the preflight warns about; required.
	Applications Applications
	// Nodes reports the version each node runs; required.
	Nodes Nodes
	// Poll is how long the flow waits between two observations of the kernel.
	Poll time.Duration
	// Wait bounds how long one node's stop or version change may take.
	Wait time.Duration
}

// Preflight answers what the upgrade affects without writing anything (C-55).
func (f *Flow) Preflight(ctx context.Context, request Request) (Report, *controlplane.Error) {
	if len(request.Nodes) == 0 {
		return Report{}, invalid("nodes 至少给一个节点")
	}
	if len(f.requirement()) > 0 {
		return Report{}, unavailable(f.requirement())
	}
	report := Report{Nodes: make([]NodePlan, 0, len(request.Nodes))}
	for _, node := range request.Nodes {
		maintained, failure := f.Maintenance.Preflight(ctx, controlplane.MaintenanceRequest{Node: node, Downtime: request.Downtime})
		if failure != nil {
			return Report{}, failure
		}
		plan := NodePlan{
			Node:         node,
			Applications: maintained.Applications,
			Volumes:      maintained.Volumes,
			Downtime:     maintained.Downtime,
		}
		version, err := f.Nodes.KubeletVersion(ctx, node)
		if err != nil {
			report.Undetermined = append(report.Undetermined, "节点 "+node+" 的版本不可读")
		}
		plan.KubeletVersion = version
		for _, item := range maintained.Undetermined {
			report.Undetermined = append(report.Undetermined, item.Item+"（"+item.Reason+"）")
		}
		report.Nodes = append(report.Nodes, plan)
	}
	unhealthy, failure := f.unhealthyApplications(ctx)
	if failure != nil {
		return Report{}, failure
	}
	report.UnhealthyApplications = unhealthy
	report.Blocked = len(report.Undetermined) > 0
	return report, nil
}

// Begin takes every node out and moves its movable workloads off (C-55). The
// node-local applications are stopped before the eviction: the drain refuses
// while one of them runs, and this step waits for the stop.
//
// A step that fails puts back every node this run had already taken out. The
// upgrade has not started at that point, so leaving them out of scheduling
// would hold the cluster in maintenance for a run that never happened; the
// result names the nodes it restored (C-55).
func (f *Flow) Begin(ctx context.Context, request Request, actor string) (Result, *controlplane.Error) {
	report, failure := f.Preflight(ctx, request)
	if failure != nil {
		return Result{}, failure
	}
	if report.Blocked {
		return Result{}, &controlplane.Error{
			Code:               controlplane.CodeConflict,
			Message:            "预检有不可判定项，升级不发起：" + strings.Join(report.Undetermined, "、"),
			Stage:              controlplane.StageValidate,
			RequiresUserAction: true,
			SideEffects:        controlplane.SideEffectsNone,
		}
	}
	result := Result{Preflight: report, Nodes: make([]NodeResult, 0, len(report.Nodes))}
	for _, plan := range report.Nodes {
		_, wrote, failure := f.Maintenance.Cordon(ctx, controlplane.MaintenanceRequest{Node: plan.Node, Downtime: request.Downtime}, actor)
		if failure != nil {
			return f.abandon(ctx, result, actor, failure)
		}
		// A node this run did not cordon keeps whatever took it out of
		// scheduling: the restore half undoes only this run's own writes.
		if wrote {
			result.Cordoned = append(result.Cordoned, plan.Node)
		}
	}
	for _, plan := range report.Nodes {
		drain, failure := f.waitDrained(ctx, plan.Node, actor)
		if failure != nil {
			return f.abandon(ctx, result, actor, failure)
		}
		result.Nodes = append(result.Nodes, NodeResult{Node: plan.Node, Drain: drain})
	}
	return result, nil
}

// abandon restores the nodes a failed Begin took out and reports which of them
// it put back. It lifts only the cordons this run wrote: a node that was already
// out of scheduling before the run keeps that state. A node the restore cannot
// lift stays out, so the failure names it: the operator has to undo that cordon
// by hand (C-55).
func (f *Flow) abandon(ctx context.Context, result Result, actor string, failure *controlplane.Error) (Result, *controlplane.Error) {
	var stuck []string
	for _, node := range result.Cordoned {
		if restoreFailure := f.Maintenance.Restore(ctx, node, actor); restoreFailure != nil {
			stuck = append(stuck, node)
			continue
		}
		result.RolledBack = append(result.RolledBack, node)
	}
	if len(stuck) > 0 {
		failure.Message += "；节点 " + strings.Join(stuck, "、") + " 未能恢复，保持不可调度"
	}
	return result, failure
}

// Finish restores the nodes one Begin took out (C-55): the cordon is lifted and
// the stopped applications return to their own reconciliation. It takes the
// begin half's result rather than the request, because only that result knows
// which cordons this run wrote: a node that was already out of scheduling keeps
// its cordon.
func (f *Flow) Finish(ctx context.Context, begun Result, actor string) (Result, *controlplane.Error) {
	result := Result{Nodes: make([]NodeResult, 0, len(begun.Cordoned))}
	for _, node := range begun.Cordoned {
		if failure := f.Maintenance.Restore(ctx, node, actor); failure != nil {
			return result, failure
		}
		result.Nodes = append(result.Nodes, NodeResult{Node: node})
	}
	return result, nil
}

// Run brackets the node upgrade: Begin takes the nodes out, the flow waits until
// each reports the target version, and Finish restores them. A node that does
// not reach the version stays out of scheduling, which is the safe side of a
// failed upgrade (C-55).
func (f *Flow) Run(ctx context.Context, request Request, actor string) (Result, *controlplane.Error) {
	if request.Version == "" {
		return Result{}, invalid("version 必填：恢复以节点报告的版本为准")
	}
	result, failure := f.Begin(ctx, request, actor)
	if failure != nil {
		return result, failure
	}
	for _, plan := range result.Preflight.Nodes {
		if failure := f.waitVersion(ctx, plan.Node, request.Version); failure != nil {
			return result, failure
		}
	}
	if _, failure := f.Finish(ctx, result, actor); failure != nil {
		return result, failure
	}
	return result, nil
}

// waitDrained retries the drain until the node-local applications have stopped
// and the kernel has moved the rest, or the wait runs out (C-65).
func (f *Flow) waitDrained(ctx context.Context, node, actor string) (controlplane.MaintenanceDrain, *controlplane.Error) {
	deadline := time.Now().Add(f.wait())
	for {
		drain, failure := f.Maintenance.Drain(ctx, node, actor)
		if failure == nil {
			return drain, nil
		}
		if !failure.Retryable || time.Now().After(deadline) {
			return controlplane.MaintenanceDrain{}, failure
		}
		select {
		case <-ctx.Done():
			return controlplane.MaintenanceDrain{}, &controlplane.Error{
				Code:        controlplane.CodeInternal,
				Message:     "升级在等待节点 " + node + " 停止应用时被取消",
				Stage:       controlplane.StageApply,
				Retryable:   true,
				SideEffects: controlplane.SideEffectsNone,
			}
		case <-time.After(f.poll()):
		}
	}
}

// waitVersion waits until the node reports the target version, which is how the
// flow learns the operator finished the node (C-55).
func (f *Flow) waitVersion(ctx context.Context, node, version string) *controlplane.Error {
	deadline := time.Now().Add(f.wait())
	for {
		reported, err := f.Nodes.KubeletVersion(ctx, node)
		if err == nil && reported == version {
			return nil
		}
		if time.Now().After(deadline) {
			return &controlplane.Error{
				Code:               controlplane.CodeConflict,
				Message:            "节点 " + node + " 未在等待期内报告版本 " + version + "，节点保持不可调度",
				Stage:              controlplane.StageApply,
				Object:             &controlplane.ObjectRef{Group: api.Group, Kind: "Node", Name: node},
				RequiresUserAction: true,
				SideEffects:        controlplane.SideEffectsNone,
			}
		}
		select {
		case <-ctx.Done():
			return &controlplane.Error{
				Code:        controlplane.CodeInternal,
				Message:     "升级在等待节点 " + node + " 报告版本时被取消",
				Stage:       controlplane.StageApply,
				Retryable:   true,
				SideEffects: controlplane.SideEffectsNone,
			}
		case <-time.After(f.poll()):
		}
	}
}

// unhealthyApplications counts the applications whose health is not Healthy
// (C-55, C-08). A cluster that cannot answer the count blocks the preflight:
// the upgrade prompt would otherwise claim there is nothing wrong.
func (f *Flow) unhealthyApplications(ctx context.Context) (int, *controlplane.Error) {
	states, err := f.Applications.Health(ctx)
	if err != nil {
		return 0, &controlplane.Error{
			Code:        controlplane.CodeClusterUnavailable,
			Message:     "集群 API 暂不可达，无法统计健康异常的应用",
			Stage:       controlplane.StageRead,
			Retryable:   true,
			SideEffects: controlplane.SideEffectsNone,
		}
	}
	unhealthy := 0
	for _, state := range states {
		if state != v1beta1.HealthStateHealthy {
			unhealthy++
		}
	}
	return unhealthy, nil
}

// requirement names what this flow is missing, empty when it is complete.
func (f *Flow) requirement() string {
	switch {
	case f.Maintenance == nil:
		return "升级流程没有接入节点维护"
	case f.Applications == nil:
		return "升级流程没有接入应用健康读取"
	case f.Nodes == nil:
		return "升级流程没有接入节点版本读取"
	}
	return ""
}

// poll returns the observation interval.
func (f *Flow) poll() time.Duration {
	if f.Poll > 0 {
		return f.Poll
	}
	return defaultPoll
}

// wait returns how long one node's step may take.
func (f *Flow) wait() time.Duration {
	if f.Wait > 0 {
		return f.Wait
	}
	return defaultWait
}

// invalid rejects a request the caller can correct.
func invalid(message string) *controlplane.Error {
	return &controlplane.Error{
		Code:               controlplane.CodeInvalidField,
		Message:            message,
		Stage:              controlplane.StageValidate,
		RequiresUserAction: true,
		SideEffects:        controlplane.SideEffectsNone,
	}
}

// unavailable reports a flow this deployment did not wire.
func unavailable(message string) *controlplane.Error {
	return &controlplane.Error{
		Code:        controlplane.CodeCapabilityMissing,
		Message:     message,
		Stage:       controlplane.StageCapability,
		SideEffects: controlplane.SideEffectsNone,
	}
}
