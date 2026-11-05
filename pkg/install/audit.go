package install

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"

	"github.com/tsic404/kosd/api/v1beta1"
)

// ErrAuditUnavailable reports that the API server audit log cannot be read.
// The install invariant treats that as unmeasured, never as a pass (C-15).
var ErrAuditUnavailable = errors.New("api server audit is unavailable")

// ResourceKinds resolves the kind an audit event names by its resource. The
// version argument carries the audit form "group/version", with an empty group
// for the core API.
type ResourceKinds func(apiVersion, resource string) (schema.GroupKind, bool)

// DiscoveryKinds builds the resolver from API discovery, preferring nothing:
// both served versions of a resource resolve to the same kind, so an audit
// event recorded against any version is recognised. The window may install an
// object class after the resolver is built, so a miss rediscovers once.
func DiscoveryKinds(client discovery.DiscoveryInterface) (ResourceKinds, error) {
	if client == nil {
		return nil, fmt.Errorf("discovery is required to resolve audit resources")
	}
	index, err := auditKindIndex(client)
	if err != nil {
		return nil, err
	}
	var mutex sync.Mutex
	return func(apiVersion, resource string) (schema.GroupKind, bool) {
		mutex.Lock()
		defer mutex.Unlock()
		group, _, _ := strings.Cut(apiVersion, "/")
		if kind, ok := index[group+"/"+resource]; ok {
			return kind, true
		}
		refreshed, err := auditKindIndex(client)
		if err != nil {
			return schema.GroupKind{}, false
		}
		index = refreshed
		kind, ok := index[group+"/"+resource]
		return kind, ok
	}, nil
}

// auditKindIndex reads every served resource and its kind.
func auditKindIndex(client discovery.DiscoveryInterface) (map[string]schema.GroupKind, error) {
	_, lists, err := client.ServerGroupsAndResources()
	if err != nil {
		var failed *discovery.ErrGroupDiscoveryFailed
		if !errors.As(err, &failed) {
			return nil, fmt.Errorf("discover audit resources: %w", err)
		}
	}
	index := make(map[string]schema.GroupKind)
	for _, list := range lists {
		groupVersion, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			return nil, fmt.Errorf("parse discovered group version %q: %w", list.GroupVersion, err)
		}
		for _, resource := range list.APIResources {
			if len(resource.Name) == 0 || resource.Kind == "" {
				continue
			}
			index[groupVersion.Group+"/"+resource.Name] = schema.GroupKind{
				Group: groupVersion.Group,
				Kind:  resource.Kind,
			}
		}
	}
	return index, nil
}

// AuditEvent is one write the API server recorded: who asked for it and which
// object it touched (C-15).
type AuditEvent struct {
	// Verb is the write verb the server saw.
	Verb string
	// User is the authenticated requester.
	User string
	// At is the stage timestamp of the finished request.
	At time.Time
	// Object is the object the request touched.
	Object v1beta1.ObjectRef
}

// AuditSource answers the writes in one time range.
type AuditSource interface {
	Events(ctx context.Context, from, to time.Time) ([]AuditEvent, error)
}

// AuditLog reads the API server audit log file in the JSON format. It is the
// measurement condition C-15 names: with no configured path, the window is
// unmeasured.
type AuditLog struct {
	// Path is the file the API server appends audit events to; empty means the
	// server does not record them.
	Path string
	// Kinds resolves an event's resource to its kind.
	Kinds ResourceKinds
}

// Events reads the writes the log holds between from and to.
func (l *AuditLog) Events(_ context.Context, from, to time.Time) ([]AuditEvent, error) {
	if l == nil || l.Path == "" {
		return nil, ErrAuditUnavailable
	}
	file, err := os.Open(l.Path)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAuditUnavailable, err)
	}
	defer file.Close()

	events := make([]AuditEvent, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		record, err := decodeAuditRecord(line)
		if err != nil {
			return nil, fmt.Errorf("decode audit event in %s: %w", l.Path, err)
		}
		if record.Stage != "ResponseComplete" || record.ObjectRef == nil {
			continue
		}
		if !writeVerb(record.Verb) || !objectWrite(record.ObjectRef.Subresource) {
			continue
		}
		at := record.StageTimestamp
		if at.IsZero() {
			at = record.RequestReceivedTimestamp
		}
		if at.Before(from) || at.After(to) {
			continue
		}
		group, kind, ok := l.resolve(record.ObjectRef)
		if !ok {
			return nil, fmt.Errorf("%w: audit event names unknown resource %s/%s",
				ErrAuditUnavailable, record.ObjectRef.APIVersion, record.ObjectRef.Resource)
		}
		events = append(events, AuditEvent{
			Verb: record.Verb,
			User: requester(record),
			At:   at,
			Object: v1beta1.ObjectRef{
				Group:     group,
				Version:   record.ObjectRef.APIVersion,
				Kind:      kind,
				Namespace: record.ObjectRef.Namespace,
				Name:      record.ObjectRef.Name,
			},
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read audit log %s: %w", l.Path, err)
	}
	sort.SliceStable(events, func(left, right int) bool { return events[left].At.Before(events[right].At) })
	return events, nil
}

// requester is the effective user of one event: an impersonated request acts
// as the impersonated identity, which is what the install invariant judges.
func requester(record auditRecord) string {
	if record.ImpersonatedUser != nil && record.ImpersonatedUser.Username != "" {
		return record.ImpersonatedUser.Username
	}
	return record.User.Username
}

// resolve maps one object reference to its group and kind.
func (l *AuditLog) resolve(ref *auditObjectRef) (string, string, bool) {
	if l.Kinds == nil {
		return "", "", false
	}
	groupKind, ok := l.Kinds(ref.APIGroup+"/"+ref.APIVersion, ref.Resource)
	if !ok {
		return "", "", false
	}
	return groupKind.Group, groupKind.Kind, true
}

// objectWrite reports whether a subresource names a persisted object write. A
// request on exec, proxy or portforward touches no object state.
func objectWrite(subresource string) bool {
	switch subresource {
	case "", "status", "scale":
		return true
	default:
		return false
	}
}

// writeVerb reports whether an audit verb is a write C-15 judges.
func writeVerb(verb string) bool {
	switch verb {
	case "create", "update", "patch", "delete", "deletecollection":
		return true
	default:
		return false
	}
}

// auditRecord is the audit.k8s.io/v1 event shape, reduced to the fields the
// invariant reads.
type auditRecord struct {
	Stage                    string          `json:"stage"`
	Verb                     string          `json:"verb"`
	User                     auditUser       `json:"user"`
	ImpersonatedUser         *auditUser      `json:"impersonatedUser"`
	ObjectRef                *auditObjectRef `json:"objectRef"`
	StageTimestamp           time.Time       `json:"stageTimestamp"`
	RequestReceivedTimestamp time.Time       `json:"requestReceivedTimestamp"`
}

// auditUser is the requester of one event.
type auditUser struct {
	Username string `json:"username"`
}

// auditObjectRef is the object one event touched.
type auditObjectRef struct {
	Resource    string `json:"resource"`
	Namespace   string `json:"namespace"`
	Name        string `json:"name"`
	APIGroup    string `json:"apiGroup"`
	APIVersion  string `json:"apiVersion"`
	Subresource string `json:"subresource"`
}

// decodeAuditRecord reads one JSON audit line.
func decodeAuditRecord(line []byte) (auditRecord, error) {
	record := auditRecord{}
	if err := json.Unmarshal(line, &record); err != nil {
		return auditRecord{}, err
	}
	return record, nil
}
