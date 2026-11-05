package maintenance

import (
	"context"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/api/v1beta1"
	"github.com/tsic404/kosd/pkg/controlplane"
)

// Preflight lists what a maintenance of one node would take out of service
// (C-65): the node-local applications whose data sits on it, the volumes that
// hold that data, and the declared downtime estimate. An item the flow cannot
// judge is reported as undetermined rather than approved (C-65).
func (s *Service) Preflight(ctx context.Context, request controlplane.MaintenanceRequest) (controlplane.MaintenanceReport, *controlplane.Error) {
	if _, failure := s.node(ctx, request.Node); failure != nil {
		return controlplane.MaintenanceReport{}, failure
	}
	report := controlplane.MaintenanceReport{
		Node:         request.Node,
		Applications: []string{},
		Volumes:      []string{},
	}
	if request.Downtime == "" {
		report.Undetermined = append(report.Undetermined, controlplane.MaintenanceUndetermined{
			Item:   "downtime",
			Reason: "未声明预计停机时长，无法给出影响面",
		})
	} else if downtime, err := time.ParseDuration(request.Downtime); err != nil || downtime < 0 {
		return controlplane.MaintenanceReport{}, &controlplane.Error{
			Code:               controlplane.CodeInvalidField,
			Message:            "downtime 必须是正时长，例如 15m",
			Stage:              controlplane.StageValidate,
			RequiresUserAction: true,
			SideEffects:        controlplane.SideEffectsNone,
		}
	} else {
		report.Downtime = request.Downtime
	}
	placement, failure := s.placed(ctx, request.Node)
	if failure != nil {
		return controlplane.MaintenanceReport{}, failure
	}
	report.Applications = placement.names()
	report.Volumes = placement.volumes
	report.Undetermined = append(report.Undetermined, placement.undetermined...)
	return report, nil
}

// placement is what one node's maintenance takes out of service (C-65).
type placement struct {
	// applications are the node-local applications bound to the node.
	applications []boundApplication
	// volumes are the node-local volumes whose data sits on the node.
	volumes []string
	// undetermined are the items the preflight could not judge.
	undetermined []controlplane.MaintenanceUndetermined
}

// boundApplication is one application whose data sits on the node, with the
// objects it rendered so the drain knows which workloads are node-bound.
type boundApplication struct {
	// namespace and name identify the application object, which is how every
	// reader names it.
	namespace string
	name      string
	// target is the namespace its workloads run in; a rendered reference without
	// a namespace of its own resolves there.
	target   string
	rendered []v1beta1.ObjectRef
}

// names renders the bound applications the way a reader names them.
func (p placement) names() []string {
	names := make([]string, 0, len(p.applications))
	for _, application := range p.applications {
		names = append(names, application.namespace+"/"+application.name)
	}
	return names
}

// placed computes what sits on one node. A volume that declares node-local
// mobility but records no node, and an application whose declared volume has no
// object, are both undetermined: the flow cannot prove they are unaffected, so
// it does not approve them by default (C-65).
func (s *Service) placed(ctx context.Context, node string) (placement, *controlplane.Error) {
	objects, err := s.Dynamic.Resource(volumeResource).List(ctx, metav1.ListOptions{})
	if err != nil {
		return placement{}, s.readFailure(err)
	}
	result := placement{volumes: []string{}}
	known := map[string]bool{}
	bound := map[string]bool{}
	for index := range objects.Items {
		object := objects.Items[index]
		var volume v1beta1.Volume
		if failure := decodeObject(object, &volume); failure != nil {
			return placement{}, failure
		}
		key := object.GetNamespace() + "/" + object.GetName()
		known[key] = true
		if volume.Spec.Mobility != v1beta1.VolumeMobilityNodeLocal {
			continue
		}
		if volume.Status.Node == "" {
			result.undetermined = append(result.undetermined, controlplane.MaintenanceUndetermined{
				Item:   "卷 " + key,
				Reason: "卷没有记录数据所在节点",
				Ref:    &controlplane.ObjectRef{Group: api.Group, Kind: "Volume", Namespace: object.GetNamespace(), Name: object.GetName()},
			})
			continue
		}
		if volume.Status.Node != node {
			continue
		}
		bound[key] = true
		result.volumes = append(result.volumes, key)
	}
	applications, err := s.Dynamic.Resource(applicationResource).List(ctx, metav1.ListOptions{})
	if err != nil {
		return placement{}, s.readFailure(err)
	}
	for index := range applications.Items {
		object := applications.Items[index]
		var application v1beta1.Application
		if failure := decodeObject(object, &application); failure != nil {
			return placement{}, failure
		}
		namespace := application.Spec.Namespace
		if namespace == "" {
			namespace = object.GetNamespace()
		}
		affected := false
		for _, name := range application.Spec.Volumes {
			key := namespace + "/" + name
			if !known[key] {
				result.undetermined = append(result.undetermined, controlplane.MaintenanceUndetermined{
					Item:   "应用 " + object.GetNamespace() + "/" + object.GetName(),
					Reason: "声明的卷 " + name + " 没有卷对象",
					Ref:    &controlplane.ObjectRef{Group: api.Group, Kind: "Application", Namespace: object.GetNamespace(), Name: object.GetName()},
				})
				continue
			}
			if bound[key] {
				affected = true
			}
		}
		if !affected {
			continue
		}
		result.applications = append(result.applications, boundApplication{
			namespace: object.GetNamespace(),
			name:      object.GetName(),
			target:    namespace,
			rendered:  application.Status.Rendered,
		})
	}
	sort.Strings(result.volumes)
	sort.Slice(result.applications, func(left, right int) bool {
		if result.applications[left].namespace != result.applications[right].namespace {
			return result.applications[left].namespace < result.applications[right].namespace
		}
		return result.applications[left].name < result.applications[right].name
	})
	sort.Slice(result.undetermined, func(left, right int) bool {
		return result.undetermined[left].Item < result.undetermined[right].Item
	})
	return result, nil
}

// decodeObject reads one kernel object into its typed shape.
func decodeObject(object unstructured.Unstructured, target any) *controlplane.Error {
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, target); err != nil {
		return &controlplane.Error{
			Code:        controlplane.CodeInternal,
			Message:     "对象无法解码",
			Stage:       controlplane.StageRead,
			Retryable:   true,
			SideEffects: controlplane.SideEffectsNone,
		}
	}
	return nil
}
