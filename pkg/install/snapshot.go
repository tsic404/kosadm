package install

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
)

// ObjectKey identifies one object across snapshots.
type ObjectKey struct {
	Group     string
	Kind      string
	Namespace string
	Name      string
}

// String renders the key the way failure messages name an object.
func (k ObjectKey) String() string {
	group := k.Group
	if group == "" {
		group = "core"
	}
	name := k.Name
	if k.Namespace != "" {
		name = k.Namespace + "/" + name
	}
	return group + "/" + k.Kind + "/" + name
}

// ObjectState is what one snapshot keeps of an object: its attribution and a
// flattened field list, so a later diff can name the changed paths.
type ObjectState struct {
	UID    types.UID
	Labels map[string]string
	Fields map[string]string
}

// Snapshot is the cluster state at one observation point (C-15).
type Snapshot struct {
	At      time.Time
	Objects map[ObjectKey]ObjectState
}

// Lookup returns the state of one object; ok is false when the snapshot does
// not hold it.
func (s *Snapshot) Lookup(key ObjectKey) (ObjectState, bool) {
	if s == nil {
		return ObjectState{}, false
	}
	state, found := s.Objects[key]
	return state, found
}

// Snapshotter reads every object the API server lists. It runs as the installer
// identity, which the install invariant already trusts to read the cluster.
type Snapshotter struct {
	// Dynamic lists the objects.
	Dynamic dynamic.Interface
	// Discovery names the resources and their kinds.
	Discovery discovery.DiscoveryInterface
	// Now supplies the observation time; nil means the wall clock.
	Now func() time.Time
}

// Take reads one snapshot. A resource the server refuses to list fails the
// snapshot: a partial picture would turn an unobserved write into a pass.
func (s *Snapshotter) Take(ctx context.Context) (*Snapshot, error) {
	if s.Dynamic == nil || s.Discovery == nil {
		return nil, fmt.Errorf("snapshot needs a dynamic client and discovery")
	}
	lists, err := s.Discovery.ServerPreferredResources()
	if err != nil {
		return nil, fmt.Errorf("discover resources for the snapshot: %w", err)
	}
	snapshot := &Snapshot{Objects: map[ObjectKey]ObjectState{}, At: s.now()}
	for _, list := range lists {
		groupVersion, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			return nil, fmt.Errorf("parse discovered group version %q: %w", list.GroupVersion, err)
		}
		for _, resource := range list.APIResources {
			if !listable(resource) {
				continue
			}
			items, err := s.list(ctx, groupVersion, resource)
			if err != nil {
				return nil, fmt.Errorf("list %s/%s: %w", list.GroupVersion, resource.Name, err)
			}
			for index := range items {
				item := &items[index]
				key := ObjectKey{
					Group:     groupVersion.Group,
					Kind:      resource.Kind,
					Namespace: item.GetNamespace(),
					Name:      item.GetName(),
				}
				snapshot.Objects[key] = ObjectState{
					UID:    item.GetUID(),
					Labels: item.GetLabels(),
					Fields: flatten(item.Object),
				}
			}
		}
	}
	return snapshot, nil
}

// list reads one resource in the scope the server gives it.
func (s *Snapshotter) list(ctx context.Context, groupVersion schema.GroupVersion, resource metav1.APIResource) ([]unstructured.Unstructured, error) {
	client := s.Dynamic.Resource(schema.GroupVersionResource{
		Group:    groupVersion.Group,
		Version:  groupVersion.Version,
		Resource: resource.Name,
	})
	var lister dynamic.ResourceInterface = client
	if resource.Namespaced {
		lister = client.Namespace(metav1.NamespaceAll)
	}
	list, err := lister.List(ctx, metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsMethodNotSupported(err) {
			return nil, nil
		}
		return nil, err
	}
	return list.Items, nil
}

// listable reports whether a discovered resource can be listed as an object
// collection.
func listable(resource metav1.APIResource) bool {
	if strings.Contains(resource.Name, "/") {
		return false
	}
	for _, verb := range resource.Verbs {
		if verb == "list" {
			return true
		}
	}
	return false
}

// snapshotExcludedPaths are the fields the server rewrites on writes of its
// own: they name bookkeeping, not object content, and would drown a field diff.
var snapshotExcludedPaths = map[string]bool{
	"metadata.managedFields":     true,
	"metadata.resourceVersion":   true,
	"metadata.generation":        true,
	"metadata.creationTimestamp": true,
}

// flatten records every leaf and container of an object.
func flatten(object map[string]any) map[string]string {
	fields := make(map[string]string)
	flattenInto("", object, fields)
	return fields
}

// flattenInto records one node of the tree.
func flattenInto(path string, node any, fields map[string]string) {
	if snapshotExcludedPaths[path] {
		return
	}
	switch typed := node.(type) {
	case map[string]any:
		if path != "" {
			fields[path] = "object"
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			flattenInto(joinPath(path, key), typed[key], fields)
		}
	case []any:
		fields[path] = fmt.Sprintf("array[%d]", len(typed))
		for index, value := range typed {
			flattenInto(fmt.Sprintf("%s[%d]", path, index), value, fields)
		}
	default:
		fields[path] = fmt.Sprintf("%v", typed)
	}
}

// joinPath appends a JSON key to a field path.
func joinPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

// changedFields returns the paths whose presence or value differs between two
// states, so a violation names the fields the write touched (C-15).
func changedFields(before, after map[string]string) []string {
	changed := make([]string, 0)
	for path, value := range after {
		if previous, found := before[path]; !found || previous != value {
			changed = append(changed, path)
		}
	}
	for path := range before {
		if _, found := after[path]; !found {
			changed = append(changed, path)
		}
	}
	sort.Strings(changed)
	return changed
}

// now returns the observation time.
func (s *Snapshotter) now() time.Time {
	if s.Now == nil {
		return time.Now().UTC()
	}
	return s.Now().UTC()
}
