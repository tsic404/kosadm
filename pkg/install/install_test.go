package install

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/api/v1beta1"
	"github.com/tsic404/kosd/pkg/markers"
)

// objectState builds one snapshot entry with the given labels and fields.
func objectState(labels map[string]string, fields map[string]string) ObjectState {
	if fields == nil {
		fields = map[string]string{}
	}
	return ObjectState{Labels: labels, Fields: fields}
}

// snapshotOf indexes entries by their key.
func snapshotOf(objects map[ObjectKey]ObjectState) *Snapshot {
	return &Snapshot{At: time.Now(), Objects: objects}
}

// TestVerdictAppliesTheWriterWhitelist pins C-15: the judgment is per writer,
// a KOS subject may write the kernel kinds, its namespace, an installed
// extension class and its own attributed downstream objects, and any other
// KOS write is a violation naming object and requester.
func TestVerdictAppliesTheWriterWhitelist(t *testing.T) {
	installer := "system:serviceaccount:kos-system:kos-installer"
	controller := "system:serviceaccount:kos-system:kos-control-plane"
	extensionKind := schema.GroupKind{Group: "widgets.example.test", Kind: "Widget"}

	downstreamKey := ObjectKey{Group: "apps", Kind: "Deployment", Namespace: "default", Name: "shop"}
	before := snapshotOf(map[ObjectKey]ObjectState{
		downstreamKey: objectState(nil, map[string]string{"spec.replicas": "1"}),
	})
	statefulKey := ObjectKey{Group: "apps", Kind: "StatefulSet", Namespace: "default", Name: "db"}
	roleBindingKey := ObjectKey{Group: "rbac.authorization.k8s.io", Kind: "RoleBinding", Namespace: "default", Name: ControlPlaneName}
	renderedBindingKey := ObjectKey{Group: "rbac.authorization.k8s.io", Kind: "RoleBinding", Namespace: "shop", Name: ControlPlaneName}
	extensionRoleKey := ObjectKey{Group: "rbac.authorization.k8s.io", Kind: "Role", Namespace: "kube-system", Name: "widgets-reader"}
	after := snapshotOf(map[ObjectKey]ObjectState{
		downstreamKey: objectState(
			map[string]string{markers.ManagedBy: "kosd", markers.OwnerUID: "app-uid"},
			map[string]string{"spec.replicas": "2"},
		),
		statefulKey: objectState(map[string]string{markers.ManagedBy: "kosd"}, nil),
		roleBindingKey: objectState(nil, map[string]string{
			"roleRef.apiGroup": "rbac.authorization.k8s.io",
			"roleRef.kind":     "ClusterRole",
			"roleRef.name":     "cluster-admin",
		}),
		renderedBindingKey: objectState(nil, map[string]string{
			"roleRef.apiGroup": "rbac.authorization.k8s.io",
			"roleRef.kind":     "Role",
			"roleRef.name":     ControlPlaneName,
		}),
		extensionRoleKey: objectState(
			map[string]string{markers.ManagedBy: "kosd", markers.OwnerUID: "component-uid"}, nil,
		),
	})

	cases := []struct {
		name   string
		event  AuditEvent
		passed bool
	}{
		{
			name:   "kernel object",
			event:  AuditEvent{Verb: "update", User: installer, Object: v1beta1.ObjectRef{Group: "kos.tsic.top", Kind: "Application", Namespace: "default", Name: "shop"}},
			passed: true,
		},
		{
			name:   "installation namespace",
			event:  AuditEvent{Verb: "create", User: installer, Object: v1beta1.ObjectRef{Kind: "ConfigMap", Namespace: "kos-system", Name: "record"}},
			passed: true,
		},
		{
			name:   "extension class",
			event:  AuditEvent{Verb: "create", User: controller, Object: v1beta1.ObjectRef{Group: "widgets.example.test", Kind: "Widget", Namespace: "default", Name: "w"}},
			passed: true,
		},
		{
			name: "extension class definition",
			event: AuditEvent{Verb: "create", User: controller, Object: v1beta1.ObjectRef{
				Group: "apiextensions.k8s.io", Kind: "CustomResourceDefinition", Name: "widgets.widgets.example.test",
			}},
			passed: true,
		},
		{
			name: "rendered namespaced role",
			event: AuditEvent{Verb: "create", User: installer, Object: v1beta1.ObjectRef{
				Group: "rbac.authorization.k8s.io", Kind: "Role", Namespace: "default", Name: ControlPlaneName,
			}},
			passed: true,
		},
		{
			name: "rendered-name role outside the granted namespaces",
			event: AuditEvent{Verb: "create", User: installer, Object: v1beta1.ObjectRef{
				Group: "rbac.authorization.k8s.io", Kind: "Role", Namespace: "kube-system", Name: ControlPlaneName,
			}},
			passed: false,
		},
		{
			name: "binding that points at a cluster role",
			event: AuditEvent{Verb: "create", User: installer, Object: v1beta1.ObjectRef{
				Group: "rbac.authorization.k8s.io", Kind: "RoleBinding", Namespace: "default", Name: ControlPlaneName,
			}},
			passed: false,
		},
		{
			name: "rendered binding that points at the rendered role",
			event: AuditEvent{Verb: "create", User: installer, Object: v1beta1.ObjectRef{
				Group: "rbac.authorization.k8s.io", Kind: "RoleBinding", Namespace: "shop", Name: ControlPlaneName,
			}},
			passed: true,
		},
		{
			name: "extension role with attribution outside the granted namespaces",
			event: AuditEvent{Verb: "create", User: controller, Object: v1beta1.ObjectRef{
				Group: "rbac.authorization.k8s.io", Kind: "Role", Namespace: "kube-system", Name: "widgets-reader",
			}},
			passed: true,
		},
		{
			name:   "attributed downstream object",
			event:  AuditEvent{Verb: "update", User: controller, Object: v1beta1.ObjectRef{Group: "apps", Kind: "Deployment", Namespace: "default", Name: "shop"}},
			passed: true,
		},
		{
			name:   "unattributed native object",
			event:  AuditEvent{Verb: "create", User: installer, Object: v1beta1.ObjectRef{Kind: "ConfigMap", Namespace: "kube-system", Name: "leak"}},
			passed: false,
		},
		{
			name:   "attributed native object without an owner",
			event:  AuditEvent{Verb: "create", User: controller, Object: v1beta1.ObjectRef{Group: "apps", Kind: "StatefulSet", Namespace: "default", Name: "db"}},
			passed: false,
		},
		{
			name:   "a cluster controller's write is not judged",
			event:  AuditEvent{Verb: "create", User: "system:serviceaccount:kube-system:statefulset-controller", Object: v1beta1.ObjectRef{Kind: "ConfigMap", Namespace: "kube-system", Name: "leak"}},
			passed: true,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			verdict := Judgement{
				Before:          before,
				After:           after,
				Events:          []AuditEvent{testCase.event},
				Subjects:        []string{installer, controller},
				ExtensionKinds:  []schema.GroupKind{extensionKind},
				Namespace:       "kos-system",
				GrantNamespaces: []string{"default", "shop"},
			}.Verdict()
			if verdict.Passed != testCase.passed {
				t.Fatalf("passed = %v, want %v (%+v)", verdict.Passed, testCase.passed, verdict.Violations)
			}
			if !verdict.Measured {
				t.Fatalf("verdict = %+v, want a measured window", verdict)
			}
		})
	}
}

// TestVerdictNamesObjectRequesterAndFields pins the judgment output C-15
// requires.
func TestVerdictNamesObjectRequesterAndFields(t *testing.T) {
	installer := "system:serviceaccount:kos-system:kos-installer"
	key := ObjectKey{Kind: "ConfigMap", Namespace: "kube-system", Name: "leak"}
	verdict := Judgement{
		Before: snapshotOf(map[ObjectKey]ObjectState{key: objectState(nil, map[string]string{"data.value": "old"})}),
		After:  snapshotOf(map[ObjectKey]ObjectState{key: objectState(nil, map[string]string{"data.value": "new"})}),
		Events: []AuditEvent{{
			Verb: "update", User: installer,
			Object: v1beta1.ObjectRef{Kind: "ConfigMap", Namespace: "kube-system", Name: "leak"},
		}},
		Subjects:  []string{installer},
		Namespace: "kos-system",
	}.Verdict()
	if len(verdict.Violations) != 1 {
		t.Fatalf("violations = %+v, want one", verdict.Violations)
	}
	violation := verdict.Violations[0]
	if violation.Object.Name != "leak" || violation.Object.Namespace != "kube-system" ||
		violation.Object.Kind != "ConfigMap" || violation.Requester != installer {
		t.Fatalf("violation = %+v, want the object and requester named", violation)
	}
	if len(violation.Fields) != 1 || violation.Fields[0] != "data.value" {
		t.Fatalf("fields = %v, want the changed path", violation.Fields)
	}
}

// TestVerdictIsUnmeasuredWithoutAudit pins ADR-017: a window that could not be
// measured is not a pass.
func TestVerdictIsUnmeasuredWithoutAudit(t *testing.T) {
	for name, judgement := range map[string]Judgement{
		"no audit":      {AuditError: ErrAuditUnavailable},
		"unknown scope": {ScopeError: errors.New("extension declaration unreadable")},
	} {
		t.Run(name, func(t *testing.T) {
			verdict := judgement.Verdict()
			if verdict.Measured || verdict.Passed {
				t.Fatalf("verdict = %+v, want unmeasured and not passed", verdict)
			}
			if verdict.Reason == "" {
				t.Fatal("an unmeasured verdict carries no reason")
			}
		})
	}
}

// TestQuietWindowIsTheContractValue pins the C-15 observation point while the
// acceptance cases shorten it for run time.
func TestQuietWindowIsTheContractValue(t *testing.T) {
	if DefaultQuietWindow != 60*time.Second {
		t.Fatalf("quiet window = %s, want 60s", DefaultQuietWindow)
	}
}

// TestPlanNormalizeRefusesFormsItCannotHonour pins the installation forms of
// C-54: the external-control-plane form needs the managed cluster's address, an
// existing cluster declares no nodes, and a bootstrap install declares at least
// one.
func TestPlanNormalizeRefusesFormsItCannotHonour(t *testing.T) {
	cases := []struct {
		name    string
		plan    Plan
		wantErr string
	}{
		{name: "external form without address", plan: Plan{Mode: ModeExternal, Owner: "owner"}, wantErr: "managed cluster's address"},
		{name: "external form with nodes", plan: Plan{Mode: ModeExternal, Owner: "owner", Nodes: 2, Managed: ManagedEndpoint{Server: "https://managed.test"}}, wantErr: "declares no nodes"},
		{name: "external form without certificate authority", plan: Plan{Mode: ModeExternal, Owner: "owner", Managed: ManagedEndpoint{Server: "https://managed.test"}}, wantErr: "certificate authority"},
		{name: "missing owner", plan: Plan{Mode: ModeExisting}, wantErr: "first Owner"},
		{name: "existing with nodes", plan: Plan{Mode: ModeExisting, Owner: "owner", Nodes: 3}, wantErr: "declares no nodes"},
		{name: "bootstrap without nodes", plan: Plan{Mode: ModeBootstrap, Owner: "owner"}, wantErr: "nodes"},
		{name: "application namespace equals own", plan: Plan{Mode: ModeExisting, Owner: "owner", Namespace: "kos-system", Applications: []string{"kos-system"}}, wantErr: "application namespace"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := testCase.plan.Normalize()
			if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
				t.Fatalf("Normalize() error = %v, want one naming %q", err, testCase.wantErr)
			}
		})
	}
}

// TestRenderKeepsTheSingleReplicaPromise pins C-54 and F13: the control plane
// has one replica and the single-node notice claims no high availability.
func TestRenderKeepsTheSingleReplicaPromise(t *testing.T) {
	plan := Plan{Mode: ModeBootstrap, Owner: "owner", Nodes: 1}
	documents, err := Render(plan)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	var deployment *unstructured.Unstructured
	for _, document := range documents {
		if document.GVR.Resource == "deployments" {
			deployment = document.Object
		}
		if document.GVR.Resource == "poddisruptionbudgets" {
			t.Fatalf("the render carries a disruption budget, which would promise availability")
		}
	}
	if deployment == nil {
		t.Fatal("the render carries no control-plane deployment")
	}
	replicas, found, err := unstructured.NestedInt64(deployment.Object, "spec", "replicas")
	if err != nil || !found || replicas != 1 {
		t.Fatalf("replicas = %d (found %v, err %v), want one", replicas, found, err)
	}
	single := Plan{Mode: ModeBootstrap, Owner: "owner", Nodes: 1}.Commitment()
	if single.Replicas != 1 || single.AutomaticMigration {
		t.Fatalf("commitment = %+v, want one replica and no migration", single)
	}
	if !strings.Contains(single.Notice, "不承诺高可用") || !strings.Contains(single.Notice, "单节点") {
		t.Fatalf("notice = %q, want the single-node degradation stated", single.Notice)
	}
	multi := Plan{Mode: ModeBootstrap, Owner: "owner", Nodes: 3}.Commitment()
	if !strings.Contains(multi.Notice, "3 个节点") || !strings.Contains(multi.Notice, "不承诺高可用") {
		t.Fatalf("notice = %q, want the declared node count and no availability promise", multi.Notice)
	}
	existing := Plan{Mode: ModeExisting, Owner: "owner"}.Commitment()
	if strings.Contains(existing.Notice, "单节点") || !strings.Contains(existing.Notice, "不承诺高可用") {
		t.Fatalf("notice = %q, want no node-count claim for an existing cluster", existing.Notice)
	}
}

// TestRenderKeepsClusterScopedWritesWithTheInstallPath pins C-04: the rendered
// cluster role grants the kernel objects and read-only cluster facts, while
// namespaced writes are roles in the namespaces the plan declares.
func TestRenderKeepsClusterScopedWritesWithTheInstallPath(t *testing.T) {
	plan := Plan{Mode: ModeExisting, Owner: "owner", Applications: []string{"shop"}}
	documents, err := Render(plan)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	clusterRoles := documentsOfKind(documents, "ClusterRole")
	if len(clusterRoles) != 2 {
		t.Fatalf("rendered %d cluster roles, want one for the control plane and one for the node agent", len(clusterRoles))
	}
	for _, document := range clusterRoles {
		for _, raw := range nestedSlice(t, document.Object, "rules") {
			rule := raw.(map[string]any)
			groups := stringsOf(rule["apiGroups"])
			verbs := stringsOf(rule["verbs"])
			if containsString(groups, api.Group) {
				continue
			}
			for _, verb := range verbs {
				if verb == "*" || verb == "create" || verb == "update" || verb == "patch" || verb == "delete" {
					t.Fatalf("cluster role %s grants %q on %v; cluster-scoped writes belong to the install path",
						document.Object.GetName(), verb, groups)
				}
			}
		}
	}
	roles := documentsOfKind(documents, "Role")
	if len(roles) != 2 {
		t.Fatalf("rendered %d roles, want one for the installation and one for the declared namespace", len(roles))
	}
	namespaces := make(map[string]bool)
	for _, role := range roles {
		namespaces[role.Object.GetNamespace()] = true
	}
	if !namespaces[DefaultNamespace] || !namespaces["shop"] {
		t.Fatalf("roles live in %v, want the installation and the declared namespace", namespaces)
	}
	bindings := documentsOfKind(documents, "RoleBinding")
	if len(bindings) != 2 {
		t.Fatalf("rendered %d role bindings, want one per granted namespace", len(bindings))
	}
	for _, binding := range bindings {
		roleKind, _, _ := unstructured.NestedString(binding.Object.Object, "roleRef", "kind")
		roleName, _, _ := unstructured.NestedString(binding.Object.Object, "roleRef", "name")
		if roleKind != "Role" || roleName != ControlPlaneName {
			t.Fatalf("binding %s/%s points at %s/%s, want Role/%s",
				binding.Object.GetNamespace(), binding.Object.GetName(), roleKind, roleName, ControlPlaneName)
		}
		subjects := nestedSlice(t, binding.Object, "subjects")
		if len(subjects) != 1 {
			t.Fatalf("binding %s/%s has %d subjects, want the control-plane identity only",
				binding.Object.GetNamespace(), binding.Object.GetName(), len(subjects))
		}
		subject, ok := subjects[0].(map[string]any)
		if !ok {
			t.Fatalf("binding %s/%s carries no subject object", binding.Object.GetNamespace(), binding.Object.GetName())
		}
		if subject["kind"] != "ServiceAccount" || subject["name"] != ControlPlaneServiceAccount ||
			subject["namespace"] != DefaultNamespace {
			t.Fatalf("binding %s/%s binds %v, want ServiceAccount %s/%s",
				binding.Object.GetNamespace(), binding.Object.GetName(), subject, DefaultNamespace, ControlPlaneServiceAccount)
		}
	}
}

// TestRenderDeliversTheThreeArtifacts pins C-54: an installation delivers the
// control plane, the per-node agent and the management CLI. The render carries
// the first two; the CLI is the binary this repository builds.
func TestRenderDeliversTheThreeArtifacts(t *testing.T) {
	for _, mode := range []Mode{ModeExisting, ModeBootstrap, ModeExternal} {
		plan := Plan{Mode: mode, Owner: "owner"}
		if mode == ModeBootstrap {
			plan.Nodes = 1
		}
		if mode == ModeExternal {
			plan.Managed = ManagedEndpoint{Server: "https://managed.test:6443", Authority: []byte("authority")}
		}
		documents, err := Render(plan)
		if err != nil {
			t.Fatalf("render %s: %v", mode, err)
		}
		if len(documentsOfKind(documents, "DaemonSet")) != 1 {
			t.Fatalf("%s render carries %d node agents, want one", mode, len(documentsOfKind(documents, "DaemonSet")))
		}
		agent := documentByKind(t, documents, "DaemonSet")
		if agent.GetName() != NodeAgentName || agent.GetNamespace() != DefaultNamespace {
			t.Fatalf("node agent = %s/%s, want %s/%s", agent.GetNamespace(), agent.GetName(), DefaultNamespace, NodeAgentName)
		}
		pod := nestedMap(t, agent, "spec", "template", "spec")
		if pod["serviceAccountName"] != NodeAgentServiceAccount {
			t.Fatalf("node agent runs as %v, want %s", pod["serviceAccountName"], NodeAgentServiceAccount)
		}
		// The agent names its device fact after the host, so the pod has to
		// see the host name; it reads the host device tree read-only.
		if host, _ := pod["hostNetwork"].(bool); !host {
			t.Fatal("the node agent does not share the host network, so it cannot name its own node")
		}
		containers := nestedSlice(t, agent, "spec", "template", "spec", "containers")
		container := containers[0].(map[string]any)
		if container["image"] != DefaultNodeAgentImage {
			t.Fatalf("node agent image = %v, want %s", container["image"], DefaultNodeAgentImage)
		}
		mounts, _ := container["volumeMounts"].([]any)
		if len(mounts) != 1 || mounts[0].(map[string]any)["mountPath"] != "/host/sys" {
			t.Fatalf("node agent mounts %v, want the host device tree at /host/sys", mounts)
		}
		volumes := nestedSlice(t, agent, "spec", "template", "spec", "volumes")
		if len(volumes) != 1 {
			t.Fatalf("node agent has %d volumes, want the host device tree", len(volumes))
		}
		hostPath, _ := volumes[0].(map[string]any)["hostPath"].(map[string]any)
		if hostPath["path"] != "/sys" {
			t.Fatalf("node agent mounts host path %v, want /sys", hostPath["path"])
		}
		// The agent writes its device fact and nothing else.
		role := documentByName(t, documents, "ClusterRole", NodeAgentName)
		for _, raw := range nestedSlice(t, role, "rules") {
			rule := raw.(map[string]any)
			if !containsString(stringsOf(rule["apiGroups"]), api.Group) {
				t.Fatalf("%s node agent role reaches outside the kernel group: %v", mode, rule)
			}
			if strings.Join(stringsOf(rule["resources"]), ",") != "nodedevices" {
				t.Fatalf("%s node agent role writes %v, want the device facts only", mode, rule["resources"])
			}
		}
		if mode != ModeExternal && len(documentsOfKind(documents, "Deployment")) != 1 {
			t.Fatalf("%s render carries no control-plane workload", mode)
		}
	}
}

// documentByName returns the one document of a kind and name.
func documentByName(t *testing.T, documents []Document, kind, name string) *unstructured.Unstructured {
	t.Helper()
	for _, document := range documents {
		if document.Object.GetKind() == kind && document.Object.GetName() == name {
			return document.Object
		}
	}
	t.Fatalf("rendered no %s named %s", kind, name)
	return nil
}

// documentByKind returns the one document of a kind.
func documentByKind(t *testing.T, documents []Document, kind string) *unstructured.Unstructured {
	t.Helper()
	found := documentsOfKind(documents, kind)
	if len(found) != 1 {
		t.Fatalf("rendered %d %s documents, want one", len(found), kind)
	}
	return found[0].Object
}

// documentsOfKind returns every document of a kind.
func documentsOfKind(documents []Document, kind string) []Document {
	found := make([]Document, 0)
	for _, document := range documents {
		if document.Object.GetKind() == kind {
			found = append(found, document)
		}
	}
	return found
}

// nestedSlice reads one slice field.
func nestedSlice(t *testing.T, object *unstructured.Unstructured, path ...string) []any {
	t.Helper()
	value, found, err := unstructured.NestedSlice(object.Object, path...)
	if err != nil || !found {
		t.Fatalf("read %v: found=%v err=%v", path, found, err)
	}
	return value
}

// nestedMap reads one map field.
func nestedMap(t *testing.T, object *unstructured.Unstructured, path ...string) map[string]any {
	t.Helper()
	value, found, err := unstructured.NestedMap(object.Object, path...)
	if err != nil || !found {
		t.Fatalf("read %v: found=%v err=%v", path, found, err)
	}
	return value
}

// stringsOf converts one unstructured string slice.
func stringsOf(value any) []string {
	raw, _ := value.([]any)
	converted := make([]string, 0, len(raw))
	for _, entry := range raw {
		if text, ok := entry.(string); ok {
			converted = append(converted, text)
		}
	}
	return converted
}

// containsString reports one element in a string slice.
func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// TestInjectionListStopsTwiceAtTheSameStep pins C-58: the same list fails at
// the same step, and a list naming no step or the same step twice is refused.
func TestInjectionListStopsTwiceAtTheSameStep(t *testing.T) {
	injections, err := ParseInjections("control-plane=1,definitions=2")
	if err != nil {
		t.Fatalf("parse injections: %v", err)
	}
	stop := func() string {
		injector, err := NewInjector(injections)
		if err != nil {
			t.Fatalf("build injector: %v", err)
		}
		for _, step := range Steps() {
			if err := injector.At(step); err != nil {
				var injected *InjectedFailure
				if !errors.As(err, &injected) {
					t.Fatalf("step %s failed with %v, want an injected failure", step, err)
				}
				return injected.Point
			}
		}
		return ""
	}
	first, second := stop(), stop()
	if first != StepControlPlane || second != first {
		t.Fatalf("stops = %q and %q, want %q twice", first, second, StepControlPlane)
	}
	for _, text := range []string{"nowhere=1", "control-plane=0", "control-plane=1,control-plane=2"} {
		if _, err := ParseInjections(text); err == nil {
			t.Fatalf("injections %q were accepted", text)
		}
	}
}

// TestClaimInstructionFollowsTheWindow pins the result contract of the external
// form: a run that never opened the claim window prints no claim instruction, so
// a failed installation cannot hand out an address that answers bootstrap_closed
// (C-68).
func TestClaimInstructionFollowsTheWindow(t *testing.T) {
	installer := &Installer{plan: Plan{Mode: ModeExternal, Owner: "owner"}}
	if claim := installer.partial(Result{}).OwnerClaim; claim != (OwnerClaim{}) {
		t.Fatalf("claim of a run without a window = %+v, want none", claim)
	}
	installer.claimOpened = true
	claim := installer.partial(Result{}).OwnerClaim
	if claim.URL == "" || claim.Instruction == "" {
		t.Fatalf("claim of a run that opened the window = %+v, want an address and an instruction", claim)
	}
}

// TestDefinitionsAdmitOnlyObjectClasses pins F-2: the install path writes the
// release's object definitions and nothing else, so a manifest that carries a
// webhook never reaches the cluster through it.
func TestDefinitionsAdmitOnlyObjectClasses(t *testing.T) {
	directory := t.TempDir()
	manifest := `apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: must-not-be-installed
webhooks:
  - name: example.test
    clientConfig:
      url: https://example.test/mutate
    sideEffects: None
    admissionReviewVersions: ["v1"]
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.test
spec:
  group: example.test
  scope: Namespaced
  names:
    plural: widgets
    singular: widget
    kind: Widget
    listKind: WidgetList
  versions:
    - name: v1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
`
	if err := os.WriteFile(filepath.Join(directory, "definitions.yaml"), []byte(manifest), 0o600); err != nil {
		t.Fatalf("write the definition manifests: %v", err)
	}
	definitions, err := CRDDirectory{FS: os.DirFS(directory)}.Definitions(context.Background())
	if err != nil {
		t.Fatalf("read the definitions: %v", err)
	}
	if len(definitions) != 1 || definitions[0].GetKind() != "CustomResourceDefinition" {
		t.Fatalf("definitions = %v, want the object class only", definitions)
	}
}
