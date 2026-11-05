package install

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/pkg/markers"
	"github.com/tsic404/kosd/pkg/state"
)

// WaitSettled waits for the observation points C-15 names: every object that
// carries Reconciled is True, no failure condition is True, no KOS-created job
// is running and no KOS-created claim is pending. An object that never settles
// fails the install instead of being reported as measured.
func WaitSettled(ctx context.Context, client dynamic.Interface, discoveryClient discovery.DiscoveryInterface, extensionKinds []schema.GroupKind, timeout time.Duration, poll func(context.Context, time.Duration) error) error {
	if timeout <= 0 {
		timeout = DefaultSettleTimeout
	}
	if poll == nil {
		poll = sleepContext
	}
	deadline := time.Now().Add(timeout)
	for {
		reason, err := settled(ctx, client, discoveryClient, extensionKinds)
		if err != nil {
			return err
		}
		if reason == "" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("install window did not settle after %s: %s", timeout, reason)
		}
		if err := poll(ctx, 50*time.Millisecond); err != nil {
			return err
		}
	}
}

// settled reports what still holds the window open; an empty reason means the
// window settled.
func settled(ctx context.Context, client dynamic.Interface, discoveryClient discovery.DiscoveryInterface, extensionKinds []schema.GroupKind) (string, error) {
	resources, err := reconcilableResources(discoveryClient, extensionKinds)
	if err != nil {
		return "", err
	}
	for _, resource := range resources {
		items, err := listResource(ctx, client, resource.GVR, resource.Namespaced, "")
		if err != nil {
			return "", err
		}
		for index := range items {
			item := &items[index]
			conditions, found, err := readConditions(item)
			if err != nil {
				return "", err
			}
			if !found {
				continue
			}
			reconciled := state.ReconciledOf(conditions)
			if reconciled.Status != metav1.ConditionTrue {
				return fmt.Sprintf("%s %s is %s=%s", resource.Kind, item.GetName(), api.ConditionReconciled, reconciled.Status), nil
			}
			// A non-Reconciled condition that is false reports an unsettled or
			// failed state; the positive conditions an object carries (verified
			// package, dependencies met) are true when the object is healthy.
			for _, condition := range conditions {
				if condition.Type != api.ConditionReconciled && condition.Status != metav1.ConditionTrue {
					return fmt.Sprintf("%s %s reports %s=%s", resource.Kind, item.GetName(), condition.Type, condition.Status), nil
				}
			}
		}
	}
	if reason, err := runningJobs(ctx, client, discoveryClient); err != nil || reason != "" {
		return reason, err
	}
	return pendingClaims(ctx, client, discoveryClient)
}

// objectResource is one discovered resource with the kind its objects carry.
type objectResource struct {
	GVR        schema.GroupVersionResource
	Kind       string
	Namespaced bool
}

// reconcilableResources names the resources whose objects carry a Reconciled
// condition: the kernel kinds and the classes installed extensions declare.
func reconcilableResources(discoveryClient discovery.DiscoveryInterface, extensionKinds []schema.GroupKind) ([]objectResource, error) {
	if discoveryClient == nil {
		return nil, fmt.Errorf("settle needs discovery")
	}
	lists, err := discoveryClient.ServerPreferredResources()
	if err != nil {
		return nil, fmt.Errorf("discover objects for the install window: %w", err)
	}
	extensionSet := make(map[schema.GroupKind]bool, len(extensionKinds))
	for _, kind := range extensionKinds {
		extensionSet[kind] = true
	}
	resources := make([]objectResource, 0, len(api.Kinds())+len(extensionKinds))
	seen := map[schema.GroupKind]bool{}
	for _, list := range lists {
		groupVersion, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			return nil, err
		}
		for _, resource := range list.APIResources {
			if strings.Contains(resource.Name, "/") {
				continue
			}
			kind := schema.GroupKind{Group: groupVersion.Group, Kind: resource.Kind}
			if groupVersion.Group != api.Group && !extensionSet[kind] {
				continue
			}
			if seen[kind] {
				continue
			}
			seen[kind] = true
			resources = append(resources, objectResource{
				GVR: schema.GroupVersionResource{
					Group: groupVersion.Group, Version: groupVersion.Version, Resource: resource.Name,
				},
				Kind:       resource.Kind,
				Namespaced: resource.Namespaced,
			})
		}
	}
	return resources, nil
}

// readConditions reads status.conditions of one object.
func readConditions(object *unstructured.Unstructured) ([]metav1.Condition, bool, error) {
	raw, found, err := unstructured.NestedSlice(object.Object, "status", "conditions")
	if err != nil || !found {
		return nil, false, err
	}
	conditions := make([]metav1.Condition, 0, len(raw))
	for _, entry := range raw {
		fields, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		condition := metav1.Condition{}
		if value, ok := fields["type"].(string); ok {
			condition.Type = value
		}
		if value, ok := fields["status"].(string); ok {
			condition.Status = metav1.ConditionStatus(value)
		}
		if value, ok := fields["reason"].(string); ok {
			condition.Reason = value
		}
		if value, ok := fields["message"].(string); ok {
			condition.Message = value
		}
		conditions = append(conditions, condition)
	}
	return conditions, true, nil
}

// runningJobs reports a KOS-derived job that is still running.
func runningJobs(ctx context.Context, client dynamic.Interface, discoveryClient discovery.DiscoveryInterface) (string, error) {
	resource, ok, err := preferredResource(discoveryClient, "batch", "Job")
	if err != nil || !ok {
		return "", err
	}
	items, err := listResource(ctx, client, resource, true, markers.ManagedBy)
	if err != nil {
		return "", err
	}
	for index := range items {
		active, _, err := unstructured.NestedInt64(items[index].Object, "status", "active")
		if err != nil {
			return "", err
		}
		if active > 0 {
			return fmt.Sprintf("job %s/%s is still running", items[index].GetNamespace(), items[index].GetName()), nil
		}
	}
	return "", nil
}

// pendingClaims reports a KOS-created claim that is not bound yet.
func pendingClaims(ctx context.Context, client dynamic.Interface, discoveryClient discovery.DiscoveryInterface) (string, error) {
	resource, ok, err := preferredResource(discoveryClient, "", "PersistentVolumeClaim")
	if err != nil || !ok {
		return "", err
	}
	items, err := listResource(ctx, client, resource, true, "")
	if err != nil {
		return "", err
	}
	for index := range items {
		claim := &items[index]
		if claim.GetLabels()[markers.ManagedBy] == "" {
			continue
		}
		phase, _, err := unstructured.NestedString(claim.Object, "status", "phase")
		if err != nil {
			return "", err
		}
		if phase == "Pending" || phase == "" {
			return fmt.Sprintf("claim %s/%s is pending", claim.GetNamespace(), claim.GetName()), nil
		}
	}
	return "", nil
}

// preferredResource finds the resource of one kind in a preferred group.
func preferredResource(discoveryClient discovery.DiscoveryInterface, group, kind string) (schema.GroupVersionResource, bool, error) {
	lists, err := discoveryClient.ServerPreferredResources()
	if err != nil {
		return schema.GroupVersionResource{}, false, fmt.Errorf("discover %s: %w", kind, err)
	}
	for _, list := range lists {
		groupVersion, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			return schema.GroupVersionResource{}, false, err
		}
		if groupVersion.Group != group {
			continue
		}
		for _, resource := range list.APIResources {
			if resource.Kind == kind && !strings.Contains(resource.Name, "/") {
				return schema.GroupVersionResource{
					Group: groupVersion.Group, Version: groupVersion.Version, Resource: resource.Name,
				}, true, nil
			}
		}
	}
	return schema.GroupVersionResource{}, false, nil
}

// listResource lists one resource; namespaced is true when the resource lives
// in namespaces, and namespace "" lists every namespace.
func listResource(ctx context.Context, client dynamic.Interface, resource schema.GroupVersionResource, namespaced bool, namespace string) ([]unstructured.Unstructured, error) {
	var lister dynamic.ResourceInterface = client.Resource(resource)
	if namespaced {
		if namespace == "" {
			namespace = metav1.NamespaceAll
		}
		lister = client.Resource(resource).Namespace(namespace)
	}
	list, err := lister.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", resource.Resource, err)
	}
	return list.Items, nil
}

// WaitQuiet waits until no KOS subject wrote anything for quiet. A window whose
// audit cannot be read is unmeasured, so the wait reports ErrAuditUnavailable
// rather than passing (C-15).
func WaitQuiet(ctx context.Context, audit AuditSource, subjects []string, quiet time.Duration) error {
	if quiet <= 0 {
		quiet = DefaultQuietWindow
	}
	subjectSet := make(map[string]bool, len(subjects))
	for _, subject := range subjects {
		subjectSet[subject] = true
	}
	quietSince := time.Now()
	for {
		now := time.Now()
		events, err := audit.Events(ctx, quietSince, now)
		if err != nil {
			return err
		}
		lastWrite := quietSince
		for _, event := range events {
			if subjectSet[event.User] && event.At.After(lastWrite) {
				lastWrite = event.At
			}
		}
		if lastWrite.After(quietSince) {
			quietSince = lastWrite
			continue
		}
		if now.Sub(quietSince) >= quiet {
			return nil
		}
		if err := sleepContext(ctx, 50*time.Millisecond); err != nil {
			return err
		}
	}
}

// sleepContext sleeps unless the context ends first.
func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// IsUnavailable reports whether err means the measurement was unavailable
// rather than failed.
func IsUnavailable(err error) bool {
	return errors.Is(err, ErrAuditUnavailable)
}
