package install

import (
	"context"
	"fmt"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
)

// Rescue is the read-only rescue mode of C-54: it reads native resources
// straight from the API server, skipping capability checks and caches. The type
// holds no writer and starts no reconciler, so "read only" is structural rather
// than a flag a caller could flip.
type Rescue struct {
	dynamic   dynamic.Interface
	discovery discovery.DiscoveryInterface
}

// NewRescue builds the rescue reader.
func NewRescue(client dynamic.Interface, discoveryClient discovery.DiscoveryInterface) (*Rescue, error) {
	if client == nil || discoveryClient == nil {
		return nil, fmt.Errorf("rescue mode needs a dynamic client and discovery")
	}
	return &Rescue{dynamic: client, discovery: discoveryClient}, nil
}

// NativeView is one native resource and the objects the server holds for it.
type NativeView struct {
	// Resource is the resource that was read.
	Resource schema.GroupVersionResource
	// Objects are what the API server returned; they are not projected through
	// any KOS cache or status writer.
	Objects []*unstructured.Unstructured
}

// NativeFailure is one resource a rescue read could not complete.
type NativeFailure struct {
	// Resource is what could not be read. It names only a group version when
	// discovery itself reported that group version as failed.
	Resource schema.GroupVersionResource
	// Err is why the read failed.
	Err error
}

// NativeReport is what one native read collected and what it could not. Rescue
// runs on a cluster that may already be broken, so one unreadable resource does
// not hide the rest; the caller reports both lists.
type NativeReport struct {
	// Views are the resources that answered, including those holding no object.
	Views []NativeView
	// Failures are the resources or group versions that did not answer.
	Failures []NativeFailure
}

// Native projects every listable resource the API server serves. Callers filter
// the view; the rescue path itself never narrows the truth. A discovery that
// answers only some groups still yields those groups' resources, and a resource
// the server refuses to list is recorded as a failure instead of hiding the
// resources that answered: a partial view with its gaps named is what a damaged
// cluster can offer.
func (r *Rescue) Native(ctx context.Context) (NativeReport, error) {
	report := NativeReport{Views: make([]NativeView, 0)}
	lists, err := r.discovery.ServerPreferredResources()
	if err != nil {
		groups, partial := discovery.GroupDiscoveryFailedErrorGroups(err)
		if !partial {
			return report, fmt.Errorf("discover native resources: %w", err)
		}
		// A group whose backend is down does not make the served groups
		// unreadable, and the gap has to be visible in the report.
		failed := make([]string, 0, len(groups))
		for groupVersion := range groups {
			failed = append(failed, groupVersion.String())
		}
		sort.Strings(failed)
		for _, name := range failed {
			groupVersion, parseErr := schema.ParseGroupVersion(name)
			if parseErr != nil {
				report.Failures = append(report.Failures, NativeFailure{Err: fmt.Errorf("discover %s: %w", name, parseErr)})
				continue
			}
			report.Failures = append(report.Failures, NativeFailure{
				Resource: schema.GroupVersionResource{Group: groupVersion.Group, Version: groupVersion.Version},
				Err:      fmt.Errorf("discover %s: %w", name, groups[groupVersion]),
			})
		}
	}
	for _, list := range lists {
		groupVersion, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			report.Failures = append(report.Failures, NativeFailure{
				Err: fmt.Errorf("parse discovered group version %q: %w", list.GroupVersion, err),
			})
			continue
		}
		for _, resource := range list.APIResources {
			if !listable(resource) {
				continue
			}
			gvr := schema.GroupVersionResource{
				Group: groupVersion.Group, Version: groupVersion.Version, Resource: resource.Name,
			}
			objects, err := r.read(ctx, gvr, resource.Namespaced, "")
			if err != nil {
				report.Failures = append(report.Failures, NativeFailure{Resource: gvr, Err: err})
				continue
			}
			report.Views = append(report.Views, NativeView{Resource: gvr, Objects: objects})
		}
	}
	return report, nil
}

// List reads one resource in one namespace; empty means every namespace, and a
// cluster-scoped resource ignores it. It is the query path of rescue mode. The
// server decides the scope, and a resource the server does not serve fails
// instead of reading as an empty result: a wrong name is not an empty cluster.
func (r *Rescue) List(ctx context.Context, resource schema.GroupVersionResource, namespace string) ([]*unstructured.Unstructured, error) {
	namespaced, err := r.namespaced(resource)
	if err != nil {
		return nil, err
	}
	return r.read(ctx, resource, namespaced, namespace)
}

// namespaced reports the scope the server gives one resource.
func (r *Rescue) namespaced(resource schema.GroupVersionResource) (bool, error) {
	groupVersion := resource.GroupVersion()
	served, err := r.discovery.ServerResourcesForGroupVersion(groupVersion.String())
	if err != nil {
		return false, fmt.Errorf("discover %s: %w", groupVersion, err)
	}
	for _, candidate := range served.APIResources {
		if candidate.Name == resource.Resource {
			return candidate.Namespaced, nil
		}
	}
	return false, fmt.Errorf("resource %s/%s is not served by the API server", groupVersion, resource.Resource)
}

// read lists one resource in a scope the caller already resolved.
func (r *Rescue) read(ctx context.Context, resource schema.GroupVersionResource, namespaced bool, namespace string) ([]*unstructured.Unstructured, error) {
	var lister dynamic.ResourceInterface = r.dynamic.Resource(resource)
	if namespaced {
		if namespace == "" {
			namespace = metav1.NamespaceAll
		}
		lister = r.dynamic.Resource(resource).Namespace(namespace)
	}
	list, err := lister.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", resource.Resource, err)
	}
	objects := make([]*unstructured.Unstructured, 0, len(list.Items))
	for index := range list.Items {
		objects = append(objects, &list.Items[index])
	}
	return objects, nil
}
