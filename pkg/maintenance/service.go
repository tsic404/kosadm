package maintenance

import (
	"context"
	"log/slog"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/pkg/controlplane"
	"github.com/tsic404/kosd/pkg/write"
)

// nodeResource is the run-kernel class the maintenance state lives on (C-65).
var nodeResource = schema.GroupVersionResource{Version: "v1", Resource: "nodes"}

// volumeResource is where a volume records the node its data sits on (C-38).
var volumeResource = schema.GroupVersionResource{Group: api.Group, Version: "v1beta1", Resource: "volumes"}

// applicationResource is where an application records what it rendered (C-33).
var applicationResource = schema.GroupVersionResource{Group: api.Group, Version: "v1beta1", Resource: "applications"}

// Service is the node maintenance flow (C-65).
type Service struct {
	// Dynamic reads the objects the preflight judges; required.
	Dynamic dynamic.Interface
	// Writer writes the run kernel's unschedulable field as the API process
	// (C-04, C-65); required.
	Writer *write.Writer
	// Pods reads and evicts the pods the run kernel runs on a node; nil leaves
	// the drain unable to move anything.
	Pods PodSource
	// Auditor records every write attempt (C-14); nil records nothing.
	Auditor controlplane.Auditor
	// Log receives the trail failures a caller must not learn about.
	Log *slog.Logger
	// Now supplies the clock the audit record carries; defaults to time.Now.
	Now func() time.Time
}

// Cordon takes the node out of scheduling (C-65 step two). The preflight runs
// first: a flow with an item it cannot judge does not proceed, and an already
// cordoned node takes no second write. The second result reports whether this
// call wrote the field, so a caller that later restores only what it cordoned
// does not lift a cordon another writer set.
func (s *Service) Cordon(ctx context.Context, request controlplane.MaintenanceRequest, actor string) (controlplane.MaintenanceReport, bool, *controlplane.Error) {
	report, failure := s.Preflight(ctx, request)
	if failure != nil {
		return controlplane.MaintenanceReport{}, false, failure
	}
	if len(report.Undetermined) > 0 {
		s.record(ctx, actor, request.Node, controlplane.VerbMaintain, controlplane.ResultRejected)
		return controlplane.MaintenanceReport{}, false, undetermined(report)
	}
	node, failure := s.node(ctx, request.Node)
	if failure != nil {
		return controlplane.MaintenanceReport{}, false, failure
	}
	if unschedulable(node.Object) {
		return report, false, nil
	}
	if failure := s.write(ctx, request.Node, true); failure != nil {
		s.record(ctx, actor, request.Node, controlplane.VerbMaintain, controlplane.ResultFailed)
		return controlplane.MaintenanceReport{}, false, failure
	}
	s.record(ctx, actor, request.Node, controlplane.VerbMaintain, controlplane.ResultAccepted)
	return report, true, nil
}

// Restore lifts the cordon (C-65 step four). A node that is already schedulable
// has nothing to undo, so no second record is written and the attribution of a
// finished maintenance does not change.
func (s *Service) Restore(ctx context.Context, node string, actor string) *controlplane.Error {
	stored, failure := s.node(ctx, node)
	if failure != nil {
		return failure
	}
	if !unschedulable(stored.Object) {
		return nil
	}
	if failure := s.write(ctx, node, false); failure != nil {
		s.record(ctx, actor, node, controlplane.VerbRestore, controlplane.ResultFailed)
		return failure
	}
	s.record(ctx, actor, node, controlplane.VerbRestore, controlplane.ResultAccepted)
	return nil
}

// node reads one node object.
func (s *Service) node(ctx context.Context, name string) (*unstructured.Unstructured, *controlplane.Error) {
	object, err := s.Dynamic.Resource(nodeResource).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return nil, &controlplane.Error{
			Code:               controlplane.CodeNotFound,
			Message:            "节点 " + name + " 不存在",
			Stage:              controlplane.StageRead,
			Object:             &controlplane.ObjectRef{Group: api.Group, Kind: "Node", Name: name},
			RequiresUserAction: true,
			SideEffects:        controlplane.SideEffectsNone,
		}
	case err != nil:
		return nil, s.readFailure(err)
	}
	return object, nil
}

// write assigns the unschedulable field as the API process writes it (C-04,
// C-65). Nothing else on the node is touched.
func (s *Service) write(ctx context.Context, name string, cordoned bool) *controlplane.Error {
	if s.Writer == nil {
		return unavailable("维护流程没有接入写入路径")
	}
	document := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"unschedulable": cordoned},
	}}
	if _, err := s.Writer.Apply(ctx, write.SubjectAPIProcess, write.Session{}, write.PartSpec, document); err != nil {
		s.log().Error("node maintenance write failed", "node", name, "cordoned", cordoned, "error", err)
		return &controlplane.Error{
			Code:        controlplane.CodeWriteFailed,
			Message:     "节点不可调度字段写入失败",
			Stage:       controlplane.StageApply,
			Object:      &controlplane.ObjectRef{Group: api.Group, Kind: "Node", Name: name},
			Retryable:   true,
			SideEffects: controlplane.SideEffectsNone,
		}
	}
	return nil
}

// record appends the maintenance write to the trail the attribution rule reads
// (C-14, C-65). The object carries the kernel group, which is what makes the
// record the node's own: the settings projection judges the origin from exactly
// this pair of verbs.
func (s *Service) record(ctx context.Context, actor, node, verb, result string) {
	controlplane.RecordAudit(ctx, s.Auditor, s.Log, controlplane.AuditRecord{
		Actor:      actor,
		Object:     controlplane.ObjectRef{Group: api.Group, Kind: "Node", Name: node},
		Verb:       verb,
		Result:     result,
		Source:     controlplane.SourceAPI,
		OccurredAt: s.now(),
	})
}

// readFailure renders a read the cluster API could not answer, so an empty
// answer never claims there was nothing to read (C-13).
func (s *Service) readFailure(err error) *controlplane.Error {
	s.log().Error("node maintenance read failed", "error", err)
	return &controlplane.Error{
		Code:        controlplane.CodeClusterUnavailable,
		Message:     "集群 API 暂不可达，无法判定维护影响面",
		Stage:       controlplane.StageRead,
		Retryable:   true,
		SideEffects: controlplane.SideEffectsNone,
	}
}

// undetermined refuses a flow the preflight could not judge (C-65): an item
// that cannot be decided is not approved by default.
func undetermined(report controlplane.MaintenanceReport) *controlplane.Error {
	items := make([]string, 0, len(report.Undetermined))
	for _, entry := range report.Undetermined {
		items = append(items, entry.Item+"（"+entry.Reason+"）")
	}
	return &controlplane.Error{
		Code:               controlplane.CodeConflict,
		Message:            "预检有不可判定项，维护不发起：" + strings.Join(items, "、"),
		Stage:              controlplane.StageValidate,
		Object:             &controlplane.ObjectRef{Group: api.Group, Kind: "Node", Name: report.Node},
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
		Retryable:   false,
		SideEffects: controlplane.SideEffectsNone,
	}
}

// unschedulable reports whether the stored node is out of scheduling.
func unschedulable(object map[string]any) bool {
	cordoned, _, err := unstructured.NestedBool(object, "spec", "unschedulable")
	return err == nil && cordoned
}

// now returns the flow's clock.
func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// log returns the logger trail failures go to.
func (s *Service) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.New(slog.DiscardHandler)
}
