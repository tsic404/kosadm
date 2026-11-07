package install

import (
	"context"
	"fmt"
	"sort"

	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/api/v1beta1"
)

// Status is the read-only report of one installation: what the installation
// left in the cluster and how its components stand. The status module owns no
// state and writes nothing (C-54).
type Status struct {
	// Namespace is the installation's own namespace.
	Namespace string
	// Installed reports whether that namespace exists.
	Installed bool
	// ControlPlane is the control-plane workload's rollout.
	ControlPlane WorkloadStatus
	// Definitions are the kernel object definitions the cluster serves, sorted.
	Definitions []string
	// Components are the installed system components, ordered by name.
	Components []ComponentStatus
}

// WorkloadStatus is one workload's rollout at read time.
type WorkloadStatus struct {
	// Name is the workload's name.
	Name string
	// Present reports whether the workload exists.
	Present bool
	// Desired is the replica count the workload asks for.
	Desired int32
	// Ready is the replica count the workload reports ready.
	Ready int32
}

// ComponentStatus is one installed component's readiness.
type ComponentStatus struct {
	// Name is the component object's name.
	Name string
	// Ready reports the Ready condition.
	Ready bool
	// Reason explains a component that is not ready.
	Reason string
}

// StatusReader reads the installation and its components. It holds readers
// only, so the status surface cannot write.
type StatusReader struct {
	// Namespace is the installation's namespace; empty selects DefaultNamespace.
	Namespace string
	// Kube reads the namespace and the control-plane workload.
	Kube kubernetes.Interface
	// APIExtensions reads the object definitions.
	APIExtensions apiextensionsclientset.Interface
	// Dynamic reads the installed components.
	Dynamic dynamic.Interface
}

// NewStatusReader validates the wiring of one status read.
func NewStatusReader(kube kubernetes.Interface, extensions apiextensionsclientset.Interface, client dynamic.Interface) (*StatusReader, error) {
	if kube == nil || extensions == nil || client == nil {
		return nil, fmt.Errorf("status needs a kubernetes client, an apiextensions client and a dynamic client")
	}
	return &StatusReader{Kube: kube, APIExtensions: extensions, Dynamic: client}, nil
}

// Read reports the installation and its components. A missing installation is
// reported, not treated as a failure: the operator asks what is there.
func (r *StatusReader) Read(ctx context.Context) (Status, error) {
	namespace := r.Namespace
	if namespace == "" {
		namespace = DefaultNamespace
	}
	status := Status{Namespace: namespace}
	if _, err := r.Kube.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			return status, fmt.Errorf("read namespace %s: %w", namespace, err)
		}
	} else {
		status.Installed = true
	}
	workload, err := r.controlPlane(ctx, namespace)
	if err != nil {
		return status, err
	}
	status.ControlPlane = workload
	definitions, err := r.definitions(ctx)
	if err != nil {
		return status, err
	}
	status.Definitions = definitions
	components, err := r.components(ctx)
	if err != nil {
		return status, err
	}
	status.Components = components
	return status, nil
}

// controlPlane reads the control-plane deployment's rollout.
func (r *StatusReader) controlPlane(ctx context.Context, namespace string) (WorkloadStatus, error) {
	workload := WorkloadStatus{Name: ControlPlaneName}
	deployment, err := r.Kube.AppsV1().Deployments(namespace).Get(ctx, ControlPlaneName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return workload, nil
		}
		return workload, fmt.Errorf("read deployment %s: %w", ControlPlaneName, err)
	}
	workload.Present = true
	workload.Desired = 1
	if deployment.Spec.Replicas != nil {
		workload.Desired = *deployment.Spec.Replicas
	}
	workload.Ready = deployment.Status.ReadyReplicas
	return workload, nil
}

// definitions lists the kernel object definitions the cluster serves.
func (r *StatusReader) definitions(ctx context.Context) ([]string, error) {
	list, err := r.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list object definitions: %w", err)
	}
	names := make([]string, 0, len(list.Items))
	for index := range list.Items {
		if list.Items[index].Spec.Group != api.Group {
			continue
		}
		names = append(names, list.Items[index].Name)
	}
	sort.Strings(names)
	return names, nil
}

// components lists the installed components and reads their Ready condition.
func (r *StatusReader) components(ctx context.Context) ([]ComponentStatus, error) {
	resource, ok := api.ResourceName("SystemComponent")
	if !ok {
		return nil, fmt.Errorf("the kernel model names no SystemComponent resource")
	}
	list, err := r.Dynamic.Resource(schema.GroupVersionResource{
		Group: v1beta1.GroupVersion.Group, Version: v1beta1.GroupVersion.Version, Resource: resource,
	}).List(ctx, metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return []ComponentStatus{}, nil
		}
		return nil, fmt.Errorf("list components: %w", err)
	}
	components := make([]ComponentStatus, 0, len(list.Items))
	for index := range list.Items {
		item := &list.Items[index]
		component := ComponentStatus{Name: item.GetName()}
		conditions, _, err := readConditions(item)
		if err != nil {
			return nil, fmt.Errorf("read conditions of component %s: %w", component.Name, err)
		}
		component.Ready, component.Reason = readyReason(conditions)
		components = append(components, component)
	}
	sort.Slice(components, func(left, right int) bool { return components[left].Name < components[right].Name })
	return components, nil
}

// readyReason reads the Ready condition of a component.
func readyReason(conditions []metav1.Condition) (bool, string) {
	for _, condition := range conditions {
		if condition.Type != "Ready" {
			continue
		}
		if condition.Status == metav1.ConditionTrue {
			return true, ""
		}
		switch {
		case condition.Reason != "":
			return false, condition.Reason
		case condition.Message != "":
			return false, condition.Message
		}
		return false, "not ready"
	}
	return false, "no Ready condition"
}
