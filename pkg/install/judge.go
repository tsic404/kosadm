package install

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/api/v1beta1"
	"github.com/tsic404/kosd/pkg/markers"
)

// Violation is one write the install invariant refuses: a KOS subject touched a
// whitelisted-out object (C-15).
type Violation struct {
	// Object is the object the write touched.
	Object v1beta1.ObjectRef `json:"object"`
	// Verb is the write the API server recorded.
	Verb string `json:"verb"`
	// Fields are the paths the snapshots show as changed.
	Fields []string `json:"fields,omitempty"`
	// Requester is the authenticated subject behind the write.
	Requester string `json:"requester"`
}

// String names the object, the requester and the verb for a failure message.
func (v Violation) String() string {
	return fmt.Sprintf("%s %s by %q", v.Object, v.Verb, v.Requester)
}

// Verdict is what one install window measured (C-15). Measured is false when
// the audit log could not be read; an unmeasured window is never a pass
// (ADR-017).
type Verdict struct {
	// Measured reports whether the API server audit supported the judgment.
	Measured bool `json:"measured"`
	// Passed reports whether every KOS-subject write stayed whitelisted.
	Passed bool `json:"passed"`
	// Reason explains an unmeasured window.
	Reason string `json:"reason,omitempty"`
	// Violations lists the writes outside the whitelist.
	Violations []Violation `json:"violations,omitempty"`
}

// Judgement is the input of one install-window judgment.
type Judgement struct {
	// Before is the snapshot taken before the install started.
	Before *Snapshot
	// After is the snapshot taken once the install settled.
	After *Snapshot
	// Events are the writes the API server recorded inside the window.
	Events []AuditEvent
	// Subjects are the API server identities of the installation's components.
	Subjects []string
	// ExtensionKinds are the object classes installed extensions contribute.
	ExtensionKinds []schema.GroupKind
	// Namespace is the installation's own namespace.
	Namespace string
	// GrantNamespaces are the namespaces the installation creates namespaced
	// permission objects in. C-15 covers them only there.
	GrantNamespaces []string
	// AuditError is what prevented the window from being measured.
	AuditError error
	// ScopeError is what prevented the judgment from knowing the installed
	// extension classes; an unknown scope is unmeasured, not a pass.
	ScopeError error
	// grantedNamespaces is the set GrantNamespaces was read into.
	grantedNamespaces map[string]bool
}

// Verdict judges the window. A write by a subject outside the installation is
// never a violation: the cluster controllers create derived objects such as
// volumes and endpoints for every declared object (C-15, ADR-017).
func (j Judgement) Verdict() Verdict {
	if j.AuditError != nil {
		return Verdict{Measured: false, Passed: false, Reason: j.AuditError.Error()}
	}
	if j.ScopeError != nil {
		return Verdict{Measured: false, Passed: false, Reason: j.ScopeError.Error()}
	}
	subjects := make(map[string]bool, len(j.Subjects))
	for _, subject := range j.Subjects {
		subjects[subject] = true
	}
	extensionKinds := make(map[schema.GroupKind]bool, len(j.ExtensionKinds))
	for _, kind := range j.ExtensionKinds {
		extensionKinds[kind] = true
	}
	granted := make(map[string]bool, len(j.GrantNamespaces))
	for _, namespace := range j.GrantNamespaces {
		granted[namespace] = true
	}
	j.grantedNamespaces = granted
	verdict := Verdict{Measured: true, Passed: true}
	for _, event := range j.Events {
		if !subjects[event.User] {
			continue
		}
		if j.allowed(event.Object, event.Verb, extensionKinds) {
			continue
		}
		verdict.Passed = false
		verdict.Violations = append(verdict.Violations, Violation{
			Object:    event.Object,
			Verb:      event.Verb,
			Fields:    j.changedFields(event.Object),
			Requester: event.User,
		})
	}
	sort.SliceStable(verdict.Violations, func(left, right int) bool {
		return verdict.Violations[left].String() < verdict.Violations[right].String()
	})
	return verdict
}

// allowed applies the C-15 whitelist to one object a KOS subject wrote.
func (j Judgement) allowed(ref v1beta1.ObjectRef, verb string, extensionKinds map[schema.GroupKind]bool) bool {
	if ref.Group == api.Group {
		return true
	}
	// The installation's own namespace object and its kernel definitions live
	// outside any namespace, so the namespace rule does not reach them.
	if ref.Kind == "Namespace" && ref.Name == j.Namespace {
		return true
	}
	if ref.Group == "apiextensions.k8s.io" && ref.Kind == "CustomResourceDefinition" {
		// The kernel definitions and the class definitions an installed
		// extension contributes are both whitelisted (C-15).
		if strings.HasSuffix(ref.Name, "."+api.Group) {
			return true
		}
		for kind := range extensionKinds {
			if kind.Group != "" && strings.HasSuffix(ref.Name, "."+kind.Group) {
				return true
			}
		}
	}
	// The rendered cluster-scoped permissions have fixed names; a binding must
	// point at the role it is named after and bind only the KOS service account
	// the installation renders for it (C-15). A pair that appears in neither
	// snapshot was created and removed by this run, so its attribution is gone
	// and the name is what is left.
	if ref.Kind == "ClusterRole" || ref.Kind == "ClusterRoleBinding" {
		if serviceAccount, rendered := renderedClusterPermissions[ref.Name]; rendered {
			state, found := j.stateOf(ref)
			if !found {
				return true
			}
			return j.renderedClusterPermission(ref, serviceAccount, state)
		}
	}
	if extensionKinds[schema.GroupKind{Group: ref.Group, Kind: ref.Kind}] {
		return true
	}
	if ref.Namespace == j.Namespace {
		return true
	}
	// The installation's own namespaced permission pair is covered only inside
	// the namespaces it grants, and a binding is covered only when it binds the
	// role the installation rendered: its name alone would let a KOS subject
	// grant cluster rights wherever it can write (C-15). Any other Role or
	// RoleBinding falls through to the attribution rule, which is how an
	// extension controller's rendered permissions stay whitelisted.
	if (ref.Kind == "Role" || ref.Kind == "RoleBinding") && ref.Name == ControlPlaneName {
		return j.allowedNamespacedPermission(ref, verb)
	}
	state, found := j.stateOf(ref)
	if !found || state.Labels[markers.ManagedBy] == "" {
		return false
	}
	// A native object the kernel creates below a declared object carries the
	// owner's UID (C-02). Missing it means the object was not attributed to the
	// declaration that asked for it, which is an unattributed KOS write.
	if ref.Namespace != "" {
		return state.Labels[markers.OwnerUID] != ""
	}
	return true
}

// renderedClusterPermissions maps a rendered cluster-scoped permission to the
// service account it authorizes (C-54).
var renderedClusterPermissions = map[string]string{
	ControlPlaneName: ControlPlaneServiceAccount,
	NodeAgentName:    NodeAgentServiceAccount,
}

// renderedClusterPermission checks one rendered cluster-scoped permission a
// snapshot holds: a role carries the installation's attribution marker, and a
// binding points at its role and binds exactly the rendered KOS identity. A
// binding to any other subject would grant cluster rights the installation
// never rendered (C-15).
func (j Judgement) renderedClusterPermission(ref v1beta1.ObjectRef, serviceAccount string, state ObjectState) bool {
	if ref.Kind == "ClusterRole" {
		return state.Labels[markers.ManagedBy] != ""
	}
	if state.Fields["roleRef.apiGroup"] != "rbac.authorization.k8s.io" ||
		state.Fields["roleRef.kind"] != "ClusterRole" ||
		state.Fields["roleRef.name"] != ref.Name {
		return false
	}
	return state.Fields["subjects"] == "array[1]" &&
		state.Fields["subjects[0].kind"] == "ServiceAccount" &&
		state.Fields["subjects[0].name"] == serviceAccount &&
		state.Fields["subjects[0].namespace"] == j.Namespace
}

// allowedNamespacedPermission checks a Role or RoleBinding the installation
// rendered: the namespace must be one it grants, and a binding must point at
// the rendered Role, not at a cluster role.
func (j Judgement) allowedNamespacedPermission(ref v1beta1.ObjectRef, verb string) bool {
	if ref.Name != ControlPlaneName || !j.grantedNamespaces[ref.Namespace] {
		return false
	}
	if ref.Kind == "Role" {
		return true
	}
	state, found := j.stateOf(ref)
	if !found {
		// A binding this run rendered and a rollback then removed appears in
		// neither snapshot. Only the installer creates or deletes the rendered
		// namespaced binding: the granted Role gives every other KOS subject
		// read-only access to roles and role bindings, so these two verbs are
		// the installation's own writes. An update without a stored binding
		// cannot be judged and stays a violation.
		return verb == "create" || verb == "delete"
	}
	return state.Fields["roleRef.apiGroup"] == "rbac.authorization.k8s.io" &&
		state.Fields["roleRef.kind"] == "Role" &&
		state.Fields["roleRef.name"] == ControlPlaneName
}

// stateOf reads the object from the later snapshot first, so a created object
// is judged by what it holds, and a deleted one by what it held.
func (j Judgement) stateOf(ref v1beta1.ObjectRef) (ObjectState, bool) {
	key := ObjectKey{Group: ref.Group, Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}
	if state, found := j.After.Lookup(key); found {
		return state, true
	}
	return j.Before.Lookup(key)
}

// changedFields names the paths one write changed, from the before and after
// snapshots of the same object (C-15).
func (j Judgement) changedFields(ref v1beta1.ObjectRef) []string {
	key := ObjectKey{Group: ref.Group, Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}
	before, beforeFound := j.Before.Lookup(key)
	after, afterFound := j.After.Lookup(key)
	switch {
	case !beforeFound && !afterFound:
		return nil
	case !beforeFound:
		return changedFields(map[string]string{}, after.Fields)
	case !afterFound:
		return changedFields(before.Fields, map[string]string{})
	default:
		return changedFields(before.Fields, after.Fields)
	}
}
