// Package harness boots a throwaway API server with the KOS object model
// installed and shapes the objects the acceptance cases run on.
package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/api/v1beta1"
	"github.com/tsic404/kosd/pkg/markers"
	"github.com/tsic404/kosd/pkg/write"
)

// Namespace is where namespaced kernel objects of the acceptance cases live.
const Namespace = "default"

// requestTimeout bounds every API call so a wedged server fails the case.
const requestTimeout = 30 * time.Second

// Cluster is a running API server with the generated CRDs installed.
type Cluster struct {
	// Dynamic reads and writes the kernel objects in either served version.
	Dynamic dynamic.Interface
	// Kube inspects cluster-wide admission configuration.
	Kube kubernetes.Interface
	// APIExtensions reads the installed CRDs.
	APIExtensions apiextensionsclientset.Interface
	// Plurals maps a kind to the resource name the API server serves.
	Plurals map[string]string
	// Config is the admin rest configuration the API server handed out.
	Config *rest.Config
	// AuditLogPath is where the API server writes its audit log; empty when
	// the cluster was started without audit.
	AuditLogPath string
}

// auditPolicy records object references and requesters: the install invariant
// reads who wrote which object, never the object bodies (C-15).
const auditPolicy = `apiVersion: audit.k8s.io/v1
kind: Policy
omitStages:
  - RequestReceived
rules:
  - level: Metadata
`

// Start boots etcd and kube-apiserver, installs the generated CRDs and returns
// clients pointed at the server. KUBEBUILDER_ASSETS must name the envtest
// binaries; `make test` downloads and exports them.
func Start(t *testing.T) *Cluster {
	t.Helper()
	return start(t, false, false)
}

// StartAudited is Start with the API server audit log enabled, which the
// install invariant needs as its measurement condition (C-15).
func StartAudited(t *testing.T) *Cluster {
	t.Helper()
	return start(t, true, false)
}

// StartBare boots an API server without the KOS object model, which is what a
// cluster looks like before an installation reaches it (C-54).
func StartBare(t *testing.T) *Cluster {
	t.Helper()
	return start(t, false, true)
}

// StartBareAudited is StartBare with the API server audit log enabled, so the
// installation that fills the cluster can be judged (C-15).
func StartBareAudited(t *testing.T) *Cluster {
	t.Helper()
	return start(t, true, true)
}

// start boots the control plane, optionally with audit logging, optionally
// without the KOS object model.
func start(t *testing.T, audited, bare bool) *Cluster {
	t.Helper()
	environment := &envtest.Environment{}
	if !bare {
		environment.CRDDirectoryPaths = []string{CRDDirectory(t)}
		environment.ErrorIfCRDPathMissing = true
	}
	auditLogPath := ""
	if audited {
		directory := t.TempDir()
		policyPath := filepath.Join(directory, "audit-policy.yaml")
		if err := os.WriteFile(policyPath, []byte(auditPolicy), 0o600); err != nil {
			t.Fatalf("write audit policy: %v", err)
		}
		auditLogPath = filepath.Join(directory, "audit.log")
		apiServer := environment.ControlPlane.GetAPIServer()
		apiServer.Configure().Set("audit-policy-file", policyPath)
		apiServer.Configure().Set("audit-log-path", auditLogPath)
		apiServer.Configure().Set("audit-log-format", "json")
		apiServer.Configure().Set("audit-log-mode", "blocking")
	}
	config, err := environment.Start()
	if err != nil {
		t.Fatalf("start envtest (KUBEBUILDER_ASSETS=%q): %v", os.Getenv("KUBEBUILDER_ASSETS"), err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatalf("build dynamic client: %v", err)
	}
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("build kubernetes client: %v", err)
	}
	extensionsClient, err := apiextensionsclientset.NewForConfig(config)
	if err != nil {
		t.Fatalf("build apiextensions client: %v", err)
	}

	cluster := &Cluster{
		Dynamic:       dynamicClient,
		Kube:          kubeClient,
		APIExtensions: extensionsClient,
		Config:        config,
		AuditLogPath:  auditLogPath,
	}
	cluster.Plurals = cluster.discoverPlurals(t)
	return cluster
}

// RepoRoot returns the repository root the tests run from.
func RepoRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the harness source file")
	}
	return filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(source))))
}

// CRDDirectory is where the release's object definition manifests live. The
// definitions belong to kosd, the API they describe; kosadm reads them from the
// kosd module the release pins instead of keeping a second copy that drifts.
func CRDDirectory(t *testing.T) string {
	t.Helper()
	output, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/tsic404/kosd").Output()
	if err != nil {
		t.Fatalf("locate the kosd module: %v", err)
	}
	directory := filepath.Join(strings.TrimSpace(string(output)), "config", "crd")
	if info, err := os.Stat(directory); err != nil || !info.IsDir() {
		t.Fatalf("the kosd module carries no object definition directory at %s", directory)
	}
	return directory
}

// KernelObjects returns one object per kernel kind, shaped identically for
// every acceptance case: the same labels, annotations, spec and status.
func KernelObjects(t *testing.T) []*unstructured.Unstructured {
	t.Helper()
	objects := make([]*unstructured.Unstructured, 0, len(api.Kinds()))
	for _, kind := range api.Kinds() {
		object := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": api.Group + "/" + v1beta1.GroupVersion.Version,
			"kind":       kind,
			"metadata": map[string]any{
				"name":        strings.ToLower(kind),
				"labels":      markerLabels(kind),
				"annotations": markerAnnotations(kind),
			},
			"spec": KernelSpec(kind),
		}}
		if hasStatus, _ := api.HasStatus(kind); hasStatus {
			object.Object["status"] = ObjectStatus()
		}
		objects = append(objects, object)
	}
	return objects
}

// KernelSpec returns a spec the kernel accepts for one kind. Only Endpoint has
// a shape the schema requires (C-43); every other kind carries an empty spec.
func KernelSpec(kind string) map[string]any {
	if kind != "Endpoint" {
		return map[string]any{}
	}
	return map[string]any{
		"kind":      string(v1beta1.EndpointKindHTTPHost),
		"targetApp": "application",
		"host":      "endpoint.test",
	}
}

// ObjectStatus is the status value the acceptance objects carry (C-07, C-08).
func ObjectStatus() map[string]any {
	return map[string]any{
		"phase":  "Running",
		"health": map[string]any{"state": string(v1beta1.HealthStateHealthy)},
		"conditions": []any{map[string]any{
			"type":               api.ConditionReconciled,
			"status":             "True",
			"reason":             "Reconciled",
			"message":            "acceptance",
			"lastTransitionTime": "2026-10-25T00:00:00Z",
		}},
	}
}

// markerLabels returns the attribution labels for one kind: an adopted
// definition, a discovered device and managed objects otherwise.
func markerLabels(kind string) map[string]any {
	switch kind {
	case "ApplicationDefinition":
		return map[string]any{
			markers.AdoptedBy:      "acceptance",
			markers.AdoptionReason: "pre-existing-catalogue",
		}
	case "NodeDevice":
		return map[string]any{markers.ManagedBy: "discovery"}
	default:
		return map[string]any{
			markers.ManagedBy: "kosd",
			markers.OwnerUID:  "00000000-0000-0000-0000-000000000001",
		}
	}
}

// markerAnnotations returns the annotation carrier: adopted-at for the adopted
// definition plus an unrelated annotation that must not change behaviour.
func markerAnnotations(kind string) map[string]any {
	annotations := map[string]any{"example.test/note": "acceptance"}
	if kind == "ApplicationDefinition" {
		annotations[markers.AdoptedAt] = "2026-10-25T00:00:00Z"
	}
	return annotations
}

// GVR resolves the resource of a kind in one served version.
func (c *Cluster) GVR(t *testing.T, version, kind string) schema.GroupVersionResource {
	t.Helper()
	plural, ok := c.Plurals[kind]
	if !ok {
		t.Fatalf("no plural discovered for kind %q", kind)
	}
	return schema.GroupVersionResource{Group: api.Group, Version: version, Resource: plural}
}

// Create writes an object and, for kinds with a status writer, its status.
// The returned object is what the server stored.
func (c *Cluster) Create(t *testing.T, object *unstructured.Unstructured) *unstructured.Unstructured {
	t.Helper()
	return c.CreateIn(t, Namespace, object)
}

// CreateIn writes an object into an explicit namespace, so a case can install
// one outside the acceptance namespace.
func (c *Cluster) CreateIn(t *testing.T, namespace string, object *unstructured.Unstructured) *unstructured.Unstructured {
	t.Helper()
	kind := object.GetKind()
	resource := c.resourceIn(t, v1beta1.GroupVersion.Version, kind, namespace)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	created, err := resource.Create(ctx, object, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create %s: %v", kind, err)
	}
	status, hasStatus := object.Object["status"]
	if !hasStatus {
		return created
	}
	created.Object["status"] = status
	updated, err := resource.UpdateStatus(ctx, created, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("write status of %s: %v", kind, err)
	}
	return updated
}

// Get reads an existing object in one served version.
func (c *Cluster) Get(t *testing.T, version, kind, name string) *unstructured.Unstructured {
	t.Helper()
	return c.GetIn(t, Namespace, version, kind, name)
}

// CreateClusterObject writes one object of a class the kernel does not model,
// so a case can shape a workload, a pod or an event the API server already
// holds. The returned object is what the server stored.
func (c *Cluster) CreateClusterObject(t *testing.T, group, version, resource, namespace string, object map[string]any) *unstructured.Unstructured {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	client := c.Dynamic.Resource(schema.GroupVersionResource{Group: group, Version: version, Resource: resource})
	var writable dynamic.ResourceInterface = client
	if namespace != "" {
		writable = client.Namespace(namespace)
	}
	created, err := writable.Create(ctx, &unstructured.Unstructured{Object: object}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create %s %s: %v", resource, nestedName(object), err)
	}
	return created
}

// nestedName reads the name of one object manifest for a failure message.
func nestedName(object map[string]any) string {
	metadata, _ := object["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	return name
}

// GetIn reads an existing object in an explicit namespace.
func (c *Cluster) GetIn(t *testing.T, namespace, version, kind, name string) *unstructured.Unstructured {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	object, err := c.resourceIn(t, version, kind, namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s %s/%s in %s: %v", kind, namespace, name, version, err)
	}
	return object
}

// List reads every object of a kind in the acceptance namespace.
func (c *Cluster) List(t *testing.T, version, kind string) []*unstructured.Unstructured {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	list, err := c.resource(t, version, kind).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list %s in %s: %v", kind, version, err)
	}
	objects := make([]*unstructured.Unstructured, 0, len(list.Items))
	for index := range list.Items {
		objects = append(objects, &list.Items[index])
	}
	return objects
}

// Resource resolves the client of one kind for code that runs off the test
// goroutine. The namespace is ignored for cluster-scoped kinds.
func (c *Cluster) Resource(version, kind, namespace string) (dynamic.ResourceInterface, error) {
	plural, ok := c.Plurals[kind]
	if !ok {
		return nil, fmt.Errorf("no plural discovered for kind %q", kind)
	}
	resource := c.Dynamic.Resource(schema.GroupVersionResource{Group: api.Group, Version: version, Resource: plural})
	if scope, _ := api.ScopeOf(kind); scope == api.ScopeNamespaced {
		return resource.Namespace(namespace), nil
	}
	return resource.Namespace(""), nil
}

// GetObject reads one kernel object for code that runs off the test goroutine.
func (c *Cluster) GetObject(ctx context.Context, version, kind, namespace, name string) (*unstructured.Unstructured, error) {
	resource, err := c.Resource(version, kind, namespace)
	if err != nil {
		return nil, err
	}
	return resource.Get(ctx, name, metav1.GetOptions{})
}

// ApplyStatus writes a status only when the stored value differs, so an
// unchanged observed state produces no write. It reports whether it wrote.
func (c *Cluster) ApplyStatus(ctx context.Context, version, kind, namespace, name string, status map[string]any) (bool, error) {
	resource, err := c.Resource(version, kind, namespace)
	if err != nil {
		return false, err
	}
	object, err := resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	current, found, err := unstructured.NestedMap(object.Object, "status")
	if err != nil {
		return false, err
	}
	if found {
		equal, err := sameJSON(current, status)
		if err != nil {
			return false, err
		}
		if equal {
			return false, nil
		}
	}
	object.Object["status"] = status
	_, err = resource.UpdateStatus(ctx, object, metav1.UpdateOptions{})
	return err == nil, err
}

// sameJSON compares two decoded JSON values by their encoding: the dynamic
// client decodes integers as int64 and encoding/json as float64, so a direct
// deep comparison would always report a difference.
func sameJSON(left, right any) (bool, error) {
	leftEncoded, err := json.Marshal(left)
	if err != nil {
		return false, err
	}
	rightEncoded, err := json.Marshal(right)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftEncoded, rightEncoded), nil
}

// Application returns a namespaced Application with an empty status, so the
// case's reconciler is the only status writer.
func Application(name string, annotations map[string]string) *unstructured.Unstructured {
	return ApplicationIn(Namespace, name, annotations)
}

// ApplicationIn is Application in an explicit namespace.
func ApplicationIn(namespace, name string, annotations map[string]string) *unstructured.Unstructured {
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": api.Group + "/" + v1beta1.GroupVersion.Version,
		"kind":       "Application",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"labels":    map[string]any{markers.ManagedBy: "acceptance"},
		},
		"spec": map[string]any{},
	}}
	if len(annotations) > 0 {
		object.SetAnnotations(annotations)
	}
	return object
}

// ConditionsOf reads status.conditions of a stored object; a missing list is
// nil, not an error.
func ConditionsOf(object *unstructured.Unstructured) ([]metav1.Condition, error) {
	raw, found, err := unstructured.NestedSlice(object.Object, "status", "conditions")
	if err != nil || !found {
		return nil, err
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var conditions []metav1.Condition
	if err := json.Unmarshal(encoded, &conditions); err != nil {
		return nil, err
	}
	return conditions, nil
}

// Conditions is ConditionsOf for the test goroutine.
func Conditions(t *testing.T, object *unstructured.Unstructured) []metav1.Condition {
	t.Helper()
	conditions, err := ConditionsOf(object)
	if err != nil {
		t.Fatalf("read conditions of %s %s: %v", object.GetKind(), object.GetName(), err)
	}
	return conditions
}

// StatusValue renders a status struct as the map the dynamic client writes.
func StatusValue(status v1beta1.ObjectStatus) (map[string]any, error) {
	encoded, err := json.Marshal(status)
	if err != nil {
		return nil, err
	}
	var value map[string]any
	if err := json.Unmarshal(encoded, &value); err != nil {
		return nil, err
	}
	return value, nil
}

// resource returns the client for a kind, namespace-scoped when the kind is.
func (c *Cluster) resource(t *testing.T, version, kind string) dynamic.ResourceInterface {
	t.Helper()
	return c.resourceIn(t, version, kind, Namespace)
}

// resourceIn resolves the client of one kind in an explicit namespace.
func (c *Cluster) resourceIn(t *testing.T, version, kind, namespace string) dynamic.ResourceInterface {
	t.Helper()
	resource := c.Dynamic.Resource(c.GVR(t, version, kind))
	if scope, _ := api.ScopeOf(kind); scope == api.ScopeNamespaced {
		return resource.Namespace(namespace)
	}
	return resource.Namespace("")
}

// discoverPlurals reads the resource names the API server serves for the KOS
// group and for core, so a case can name any object it writes.
func (c *Cluster) discoverPlurals(t *testing.T) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	list, err := c.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list CRDs: %v", err)
	}
	plurals := make(map[string]string, len(list.Items))
	for _, crd := range list.Items {
		plurals[crd.Spec.Names.Kind] = crd.Spec.Names.Plural
	}
	resources, err := c.Kube.Discovery().ServerResourcesForGroupVersion("v1")
	if err != nil {
		t.Fatalf("discover core v1: %v", err)
	}
	for _, resource := range resources.APIResources {
		if _, exists := plurals[resource.Kind]; !exists {
			plurals[resource.Kind] = resource.Name
		}
	}
	return plurals
}

// Resources maps every kernel kind to the resource the API server serves.
func (c *Cluster) Resources() map[schema.GroupKind]string {
	resources := make(map[schema.GroupKind]string, len(api.Kinds())+16)
	for _, kind := range api.Kinds() {
		if plural, known := c.Plurals[kind]; known {
			resources[schema.GroupKind{Group: api.Group, Kind: kind}] = plural
		}
	}
	// Rendered application objects are native ones: the write path resolves
	// their resources from discovery here, as an installed controller would
	// (C-04, C-32); batch carries the migration Jobs (C-40) and the backup
	// schedule CronJob (C-41).
	for _, groupVersion := range []schema.GroupVersion{
		{Version: "v1"}, {Group: "apps", Version: "v1"}, {Group: "batch", Version: "v1"},
	} {
		list, err := c.Kube.Discovery().ServerResourcesForGroupVersion(groupVersion.String())
		if err != nil {
			continue
		}
		for _, resource := range list.APIResources {
			if strings.Contains(resource.Name, "/") {
				continue
			}
			key := schema.GroupKind{Group: groupVersion.Group, Kind: resource.Kind}
			if _, known := resources[key]; !known {
				resources[key] = resource.Name
			}
		}
	}
	return resources
}

// Writer returns the write path over this cluster.
func (c *Cluster) Writer() *write.Writer {
	return &write.Writer{Dynamic: c.Dynamic, Resources: c.Resources()}
}

// Refresh rediscovers the resource names this cluster serves. An installation
// fills a bare cluster with the object model, so a case reads the names again
// afterwards.
func (c *Cluster) Refresh(t *testing.T) {
	t.Helper()
	c.Plurals = c.discoverPlurals(t)
}

// EnsureNamespace creates name unless the cluster already has it.
func (c *Cluster) EnsureNamespace(t *testing.T, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	_, err := c.Kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace %s: %v", name, err)
	}
}

// InstallCRD installs one extension object class definition and waits until the
// class is served. An extension package carries its own definitions; a case
// installs them when the component is provisioned.
func (c *Cluster) InstallCRD(t *testing.T, group, version, kind, plural string, namespaced bool) {
	t.Helper()
	c.installCRD(t, c.APIExtensions, group, version, kind, plural, namespaced)
}

// InstallCRDAs installs one extension object class definition as username, so
// the install window sees the definition written by a KOS subject (C-15).
func (c *Cluster) InstallCRDAs(t *testing.T, username, group, version, kind, plural string, namespaced bool) {
	t.Helper()
	c.installCRD(t, c.ExtensionsAs(t, username), group, version, kind, plural, namespaced)
}

// installCRD writes one definition and waits until it is served.
func (c *Cluster) installCRD(t *testing.T, client apiextensionsclientset.Interface, group, version, kind, plural string, namespaced bool) {
	t.Helper()
	scope := apiextensionsv1.ClusterScoped
	if namespaced {
		scope = apiextensionsv1.NamespaceScoped
	}
	preserve := true
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: plural + "." + group},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: group,
			Scope: scope,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   plural,
				Singular: strings.ToLower(kind),
				Kind:     kind,
				ListKind: kind + "List",
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:    version,
				Served:  true,
				Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type:                   "object",
						XPreserveUnknownFields: &preserve,
					},
				},
			}},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	if _, err := client.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{}); err != nil {
		t.Fatalf("install CRD %s: %v", crd.Name, err)
	}
	Eventually(t, 15*time.Second, "CRD "+crd.Name+" to be established", func() bool {
		stored, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crd.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		for _, condition := range stored.Status.Conditions {
			if condition.Type == apiextensionsv1.Established && condition.Status == apiextensionsv1.ConditionTrue {
				return true
			}
		}
		return false
	})
}

// ExtensionResource resolves the client of one extension object class.
func (c *Cluster) ExtensionResource(group, version, plural, namespace string) dynamic.ResourceInterface {
	resource := c.Dynamic.Resource(schema.GroupVersionResource{Group: group, Version: version, Resource: plural})
	if namespace == "" {
		return resource
	}
	return resource.Namespace(namespace)
}

// Direct returns the client of any object class by group, version and kind.
// A case uses it to write exactly the object it needs, without admission.
func (c *Cluster) Direct(t *testing.T, group, version, kind, namespace string) dynamic.ResourceInterface {
	t.Helper()
	plural, ok := c.Resources()[schema.GroupKind{Group: group, Kind: kind}]
	if !ok {
		t.Fatalf("no plural discovered for kind %q in group %q", kind, group)
	}
	resource := c.Dynamic.Resource(schema.GroupVersionResource{Group: group, Version: version, Resource: plural})
	if namespace == "" {
		return resource
	}
	return resource.Namespace(namespace)
}

// ListByLabels returns the kernel objects of kind matching selector.
func (c *Cluster) ListByLabels(t *testing.T, version, kind, selector string) []unstructured.Unstructured {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	list, err := c.resource(t, version, kind).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		t.Fatalf("list %s by %q: %v", kind, selector, err)
	}
	return list.Items
}

// Flatten turns an object into path to typed-value entries so two reads can be
// compared field by field. The version identity and the server's write history
// are left out: they name the version rather than the object.
func Flatten(object map[string]any) map[string]string {
	fields := make(map[string]string)
	flattenInto("", object, fields)
	return fields
}

// isVersionIdentity reports paths that differ between two reads of one object
// by construction and carry no object data.
func isVersionIdentity(path string) bool {
	return path == "apiVersion" || path == "kind" || strings.HasPrefix(path, "metadata.managedFields")
}

// flattenInto records every leaf and container of a JSON tree.
func flattenInto(path string, node any, fields map[string]string) {
	if isVersionIdentity(path) {
		return
	}
	switch typed := node.(type) {
	case map[string]any:
		if path != "" {
			fields[path] = "object"
		}
		for key, value := range typed {
			flattenInto(join(path, key), value, fields)
		}
	case []any:
		fields[path] = fmt.Sprintf("array[%d]", len(typed))
		for index, value := range typed {
			flattenInto(fmt.Sprintf("%s[%d]", path, index), value, fields)
		}
	default:
		fields[path] = fmt.Sprintf("%T=%v", typed, typed)
	}
}

// join appends a JSON key to a path.
func join(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

// ShapeOf flattens an object without its status and its server-written
// revision, so a case can require the spec and identity to be unchanged.
func ShapeOf(object *unstructured.Unstructured) map[string]string {
	shape := make(map[string]string)
	for path, value := range Flatten(object.Object) {
		if strings.HasPrefix(path, "status.") || path == "metadata.resourceVersion" {
			continue
		}
		shape[path] = value
	}
	return shape
}
