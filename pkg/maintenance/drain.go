package maintenance

import (
	"context"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/pkg/controlplane"
)

// scalableWorkloads are the rendered kinds maintenance can drive to zero (C-66).
var scalableWorkloads = map[string]struct {
	group    string
	resource string
}{
	"Deployment":  {group: "apps", resource: "deployments"},
	"StatefulSet": {group: "apps", resource: "statefulsets"},
	"ReplicaSet":  {group: "apps", resource: "replicasets"},
}

// Drain moves the workloads the run kernel may still run off the node (C-65
// step three). The node-local applications must already be stopped: their
// reconciliation stops them when it observes the cordon, and this step refuses
// while one of them still runs, so the stop always precedes the eviction.
//
// Every removal goes through the run kernel's eviction interface, which judges
// the disruption itself. A kernel that holds a pod keeps it and the drain
// reports the refusal: no path force-deletes a workload (C-55).
func (s *Service) Drain(ctx context.Context, node, actor string) (controlplane.MaintenanceDrain, *controlplane.Error) {
	stored, failure := s.node(ctx, node)
	if failure != nil {
		return controlplane.MaintenanceDrain{}, failure
	}
	if !unschedulable(stored.Object) {
		return controlplane.MaintenanceDrain{}, &controlplane.Error{
			Code:               controlplane.CodeConflict,
			Message:            "节点 " + node + " 尚未摘除，先发起维护再驱逐",
			Stage:              controlplane.StageValidate,
			Object:             &controlplane.ObjectRef{Group: api.Group, Kind: "Node", Name: node},
			RequiresUserAction: true,
			SideEffects:        controlplane.SideEffectsNone,
		}
	}
	placement, failure := s.placed(ctx, node)
	if failure != nil {
		return controlplane.MaintenanceDrain{}, failure
	}
	if len(placement.undetermined) > 0 {
		report := controlplane.MaintenanceReport{Node: node, Undetermined: placement.undetermined}
		return controlplane.MaintenanceDrain{}, undetermined(report)
	}
	stopped, failure := s.stopped(ctx, placement.applications)
	if failure != nil {
		return controlplane.MaintenanceDrain{}, failure
	}
	if len(stopped) > 0 {
		return controlplane.MaintenanceDrain{}, &controlplane.Error{
			Code:               controlplane.CodeConflict,
			Message:            "node-local 应用仍在运行，等待它们停止后再驱逐：" + strings.Join(stopped, "、"),
			Stage:              controlplane.StageValidate,
			Object:             &controlplane.ObjectRef{Group: api.Group, Kind: "Node", Name: node},
			Retryable:          true,
			RequiresUserAction: false,
			SideEffects:        controlplane.SideEffectsNone,
		}
	}
	if s.Pods == nil {
		return controlplane.MaintenanceDrain{}, unavailable("维护流程没有接入驱逐路径")
	}
	pods, err := s.Pods.PodsOn(ctx, node)
	if err != nil {
		s.log().Error("list the pods of a node failed", "node", node, "error", err)
		return controlplane.MaintenanceDrain{}, &controlplane.Error{
			Code:        controlplane.CodeClusterUnavailable,
			Message:     "集群 API 暂不可达，无法读取节点上的工作负载",
			Stage:       controlplane.StageRead,
			Retryable:   true,
			SideEffects: controlplane.SideEffectsNone,
		}
	}
	protected := placement.protectedWorkloads()
	report := controlplane.MaintenanceDrain{Node: node, Evicted: []string{}}
	var evicted []controlplane.ObjectRef
	for _, pod := range pods {
		if reason, keep := retained(pod, protected); keep {
			report.Retained = append(report.Retained, controlplane.MaintenanceRetained{Pod: pod.Namespace + "/" + pod.Name, Reason: reason})
			continue
		}
		if err := s.Pods.Evict(ctx, pod.Namespace, pod.Name); err != nil {
			s.log().Error("evict a pod failed", "node", node, "pod", pod.Namespace+"/"+pod.Name, "error", err)
			s.record(ctx, actor, node, controlplane.VerbDrain, controlplane.ResultFailed)
			return controlplane.MaintenanceDrain{}, &controlplane.Error{
				Code:        controlplane.CodeConflict,
				Message:     "运行内核拒绝驱逐 " + pod.Namespace + "/" + pod.Name + "：" + err.Error(),
				Stage:       controlplane.StageApply,
				Object:      &controlplane.ObjectRef{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name},
				Retryable:   true,
				SideEffects: controlplane.SideEffectsApplied,
				Applied:     evicted,
			}
		}
		report.Evicted = append(report.Evicted, pod.Namespace+"/"+pod.Name)
		evicted = append(evicted, controlplane.ObjectRef{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name})
	}
	sort.Strings(report.Evicted)
	sort.Slice(report.Retained, func(left, right int) bool {
		return report.Retained[left].Pod < report.Retained[right].Pod
	})
	s.record(ctx, actor, node, controlplane.VerbDrain, controlplane.ResultAccepted)
	return report, nil
}

// protectedWorkloads is the set of rendered workloads a drain leaves alone: the
// node-local applications stop themselves and keep their data on the node
// (C-65).
func (p placement) protectedWorkloads() map[string]bool {
	protected := map[string]bool{}
	for _, application := range p.applications {
		for _, ref := range application.rendered {
			namespace := ref.Namespace
			if namespace == "" {
				namespace = application.target
			}
			protected[workloadKey(ref.Kind, namespace, ref.Name)] = true
		}
	}
	return protected
}

// workloadKey names one workload the way a pod's owner is matched.
func workloadKey(kind, namespace, name string) string {
	return kind + "/" + namespace + "/" + name
}

// retained answers whether a drain leaves one pod on the node, and why.
func retained(pod Pod, protected map[string]bool) (string, bool) {
	if pod.IsUnresolved {
		return "属主已不存在，无法判定归属，留在原节点", true
	}
	if pod.WorkloadKind == "DaemonSet" {
		return "daemon set 按设计绑定在节点上", true
	}
	if pod.WorkloadKind != "" && protected[workloadKey(pod.WorkloadKind, pod.Namespace, pod.WorkloadName)] {
		return "node-local 应用停在原节点", true
	}
	return "", false
}

// stopped lists the node-local applications whose rendered workloads are still
// running. The stop is the application's own: this flow only observes it (C-65).
func (s *Service) stopped(ctx context.Context, applications []boundApplication) ([]string, *controlplane.Error) {
	var running []string
	for _, application := range applications {
		for _, ref := range application.rendered {
			workload, known := scalableWorkloads[ref.Kind]
			if !known {
				continue
			}
			namespace := ref.Namespace
			if namespace == "" {
				namespace = application.target
			}
			object, err := s.Dynamic.Resource(schema.GroupVersionResource{Group: workload.group, Version: "v1", Resource: workload.resource}).
				Namespace(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
			switch {
			case apierrors.IsNotFound(err):
				continue
			case err != nil:
				return nil, s.readFailure(err)
			}
			if replicas(object.Object) > 0 {
				running = append(running, application.namespace+"/"+application.name)
				break
			}
		}
	}
	sort.Strings(running)
	return running, nil
}

// replicas reads the desired replica count of one workload. A workload that
// names none runs a single replica, which is the run kernel's own default;
// reading the absence as zero would call a running workload stopped and let the
// drain move a node-local application off its node (C-65).
func replicas(object map[string]any) int64 {
	desired, found, err := unstructured.NestedInt64(object, "spec", "replicas")
	if err != nil || !found {
		return 1
	}
	return desired
}
