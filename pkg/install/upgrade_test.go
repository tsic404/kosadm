package install

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	upgradeNamespace = "kos-system"
	upgradeImage     = "ghcr.io/tsic404/kos-control-plane:v0.2.0"
	servedClusterIP  = "10.96.0.12"
)

// crdResource is where the API server serves the object definitions.
var crdResource = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}

// serviceResource is where the control plane publishes its API.
var serviceResource = schema.GroupVersionResource{Version: "v1", Resource: "services"}

// deploymentResource is where the control-plane workload runs.
var deploymentResource = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}

// definitionOf builds one definition of a kernel kind serving the given
// versions at the given schema description.
func definitionOf(name, kind, plural, description string, versions ...string) *apiextensionsv1.CustomResourceDefinition {
	defined := make([]apiextensionsv1.CustomResourceDefinitionVersion, 0, len(versions))
	for _, version := range versions {
		defined = append(defined, apiextensionsv1.CustomResourceDefinitionVersion{
			Name:    version,
			Served:  true,
			Storage: version == versions[len(versions)-1],
			Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
				Type:        "object",
				Description: description,
			}},
		})
	}
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group:    "kos.tsic.top",
			Names:    apiextensionsv1.CustomResourceDefinitionNames{Kind: kind, Plural: plural},
			Scope:    apiextensionsv1.NamespaceScoped,
			Versions: defined,
		},
	}
}

// establishedCondition is the condition the API server publishes for a
// definition it serves. Its observedGeneration is left unset because the pinned
// apiextensions-apiserver never fills it: the establishing controller writes
// only type, status, reason and message, and the registry zeroes the field.
func establishedCondition() []apiextensionsv1.CustomResourceDefinitionCondition {
	return []apiextensionsv1.CustomResourceDefinitionCondition{{
		Type:    apiextensionsv1.Established,
		Status:  apiextensionsv1.ConditionTrue,
		Reason:  "InitialNamesAccepted",
		Message: "the initial names have been accepted",
	}}
}

// servedDefinitions is what the cluster serves before the upgrade: both kinds
// carry the shape the previous release shipped and are established.
func servedDefinitions() []*apiextensionsv1.CustomResourceDefinition {
	applications := definitionOf("applications.kos.tsic.top", "Application", "applications", "the served shape", "v1alpha1", "v1beta1")
	volumes := definitionOf("volumes.kos.tsic.top", "Volume", "volumes", "the served shape", "v1alpha1", "v1beta1")
	for _, definition := range []*apiextensionsv1.CustomResourceDefinition{applications, volumes} {
		definition.ObjectMeta.Generation = 1
		definition.Status.Conditions = establishedCondition()
	}
	return []*apiextensionsv1.CustomResourceDefinition{applications, volumes}
}

// releaseDefinitions is the definition set the release ships, in the order the
// release lists it: one kind moved to a new shape and one is served as it was.
func releaseDefinitions() []*apiextensionsv1.CustomResourceDefinition {
	return []*apiextensionsv1.CustomResourceDefinition{
		definitionOf("volumes.kos.tsic.top", "Volume", "volumes", "the served shape", "v1alpha1", "v1beta1"),
		definitionOf("applications.kos.tsic.top", "Application", "applications", "the release's shape", "v1alpha1", "v1beta1"),
	}
}

// definitionSource answers the release's definitions as manifests.
type definitionSource []*apiextensionsv1.CustomResourceDefinition

// Definitions implements DefinitionSource.
func (s definitionSource) Definitions(context.Context) ([]*unstructured.Unstructured, error) {
	manifests := make([]*unstructured.Unstructured, 0, len(s))
	for _, definition := range s {
		encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(definition)
		if err != nil {
			return nil, err
		}
		manifests = append(manifests, &unstructured.Unstructured{Object: encoded})
	}
	return manifests, nil
}

// servedService is the control-plane Service an installation left behind: the
// shape the release renders, with the addresses and families the API server
// allocated for it. It still carries the selector of the release that installed
// it.
func servedService() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   map[string]any{"name": ControlPlaneName, "namespace": upgradeNamespace, "resourceVersion": "7"},
		"spec": map[string]any{
			"selector": map[string]any{"app": "installed-release"},
			"ports": []any{map[string]any{
				"name": "http", "port": int64(8080), "targetPort": int64(8080),
			}},
			"clusterIP":      servedClusterIP,
			"clusterIPs":     []any{servedClusterIP},
			"ipFamilies":     []any{"IPv4"},
			"ipFamilyPolicy": "SingleStack",
		},
	}}
}

// servedDeployment is the control-plane workload the installation left behind,
// running the image of the release that installed it.
func servedDeployment() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": ControlPlaneName, "namespace": upgradeNamespace, "resourceVersion": "9"},
		"spec": map[string]any{
			"replicas": int64(1),
			"selector": map[string]any{"matchLabels": map[string]any{"app": ControlPlaneName}},
			"strategy": map[string]any{"type": "Recreate"},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": ControlPlaneName}},
				"spec": map[string]any{
					"serviceAccountName": ControlPlaneServiceAccount,
					"containers": []any{map[string]any{
						"name": ControlPlaneName, "image": "ghcr.io/tsic404/kos-control-plane:v0.1.0",
						"ports": []any{map[string]any{"containerPort": int64(8080)}},
					}},
				},
			},
		},
	}}
}

// writeOrder records the writes an upgrade made, in order, so a case can assert
// which step preceded which.
type writeOrder struct {
	steps []string
}

// record appends one step.
func (o *writeOrder) record(step string) {
	o.steps = append(o.steps, step)
}

// first returns the index of the first step carrying one prefix, or -1.
func (o *writeOrder) first(prefix string) int {
	for index, step := range o.steps {
		if strings.HasPrefix(step, prefix) {
			return index
		}
	}
	return -1
}

// last returns the index of the last step carrying one prefix, or -1.
func (o *writeOrder) last(prefix string) int {
	for index := len(o.steps) - 1; index >= 0; index-- {
		if strings.HasPrefix(o.steps[index], prefix) {
			return index
		}
	}
	return -1
}

// count returns the number of steps carrying one prefix.
func (o *writeOrder) count(prefix string) int {
	count := 0
	for _, step := range o.steps {
		if strings.HasPrefix(step, prefix) {
			count++
		}
	}
	return count
}

// definitionServer plays the API server and the extension controller behind it.
// A write bumps the definition's generation the way the API server does; the
// controller publishes the Established condition afterwards, and this fake
// publishes it on the second read after a create, so a definition the upgrade
// creates is served asynchronously exactly as it is in a cluster.
type definitionServer struct {
	client  *apiextensionsfake.Clientset
	tracker k8stesting.ObjectTracker
	order   *writeOrder
	reads   map[string]int
	pending map[string]bool
}

// newDefinitionServer seeds the served definitions and the reactors that model
// the API server.
func newDefinitionServer(order *writeOrder, served []*apiextensionsv1.CustomResourceDefinition) *definitionServer {
	objects := make([]runtime.Object, 0, len(served))
	for _, definition := range served {
		objects = append(objects, definition.DeepCopy())
	}
	client := apiextensionsfake.NewSimpleClientset(objects...)
	server := &definitionServer{
		client:  client,
		tracker: client.Tracker(),
		order:   order,
		reads:   map[string]int{},
		pending: map[string]bool{},
	}
	client.PrependReactor("create", "customresourcedefinitions", server.onCreate)
	client.PrependReactor("update", "customresourcedefinitions", server.onUpdate)
	client.PrependReactor("get", "customresourcedefinitions", server.onGet)
	return server
}

// onCreate stores one new definition with no condition yet: the controller that
// publishes Established has not seen it.
func (s *definitionServer) onCreate(action k8stesting.Action) (bool, runtime.Object, error) {
	created, err := decodeDefinition(action.(k8stesting.CreateAction).GetObject())
	if err != nil {
		return true, nil, err
	}
	created.Generation = 1
	created.Status = apiextensionsv1.CustomResourceDefinitionStatus{}
	if err := s.tracker.Add(created); err != nil {
		return true, nil, err
	}
	s.pending[created.Name] = true
	s.reads[created.Name] = 0
	s.order.record("definition/" + created.Name)
	return true, created, nil
}

// onUpdate stores one replaced definition and bumps its generation, the way the
// API server does for a spec change. The status is the server's own and is left
// as it stands.
func (s *definitionServer) onUpdate(action k8stesting.Action) (bool, runtime.Object, error) {
	updated, err := decodeDefinition(action.(k8stesting.UpdateAction).GetObject())
	if err != nil {
		return true, nil, err
	}
	stored, err := s.tracker.Get(crdResource, "", updated.Name)
	if err != nil {
		return true, nil, err
	}
	previous, ok := stored.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		return false, nil, nil
	}
	updated.Generation = previous.Generation + 1
	updated.Status = previous.Status
	if err := s.tracker.Update(crdResource, updated, ""); err != nil {
		return true, nil, err
	}
	s.order.record("definition/" + updated.Name)
	return true, updated, nil
}

// onGet answers one definition. A definition the upgrade created gains the
// Established condition from the second read on, which is where the controller's
// own pass lands.
func (s *definitionServer) onGet(action k8stesting.Action) (bool, runtime.Object, error) {
	name := action.(k8stesting.GetAction).GetName()
	stored, err := s.tracker.Get(crdResource, "", name)
	if err != nil {
		return true, nil, err
	}
	definition, ok := stored.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		return false, nil, nil
	}
	s.reads[name]++
	served := definition.DeepCopy()
	if s.pending[name] && s.reads[name] > 1 {
		served.Status.Conditions = establishedCondition()
		established := served.DeepCopy()
		if err := s.tracker.Update(crdResource, established, ""); err != nil {
			return true, nil, err
		}
		delete(s.pending, name)
		s.order.record("established/" + name)
	}
	return true, served, nil
}

// decodeDefinition reads one definition the fake client hands over.
func decodeDefinition(object runtime.Object) (*apiextensionsv1.CustomResourceDefinition, error) {
	definition, ok := object.(*apiextensionsv1.CustomResourceDefinition)
	if !ok {
		return nil, errors.New("the fake client handed over an object that is not a definition")
	}
	return definition.DeepCopy(), nil
}

// upgradeWorld wires one upgrade over fakes that record the order of the writes.
type upgradeWorld struct {
	config       UpgradeConfig
	order        *writeOrder
	definitions  *definitionServer
	controlPlane *dynamicfake.FakeDynamicClient
}

// newUpgradeWorld builds the world one case upgrades, with the control-plane
// Service an earlier in-cluster installation left behind.
func newUpgradeWorld(t *testing.T) *upgradeWorld {
	t.Helper()
	return newUpgradeWorldWith(t, []runtime.Object{servedService()}, nil)
}

// newUpgradeWorldWith builds the world over the objects each cluster starts
// from: the managed side and, when a case needs one, the control-plane side.
func newUpgradeWorldWith(t *testing.T, managed, controlPlaneObjects []runtime.Object) *upgradeWorld {
	t.Helper()
	order := &writeOrder{}
	definitions := newDefinitionServer(order, servedDefinitions())
	controlPlane := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), managed...)
	recordControlPlane(order, controlPlane)
	return &upgradeWorld{
		config: UpgradeConfig{
			Plan: Plan{
				Mode:      ModeExisting,
				Namespace: upgradeNamespace,
				Owner:     "owner",
				Image:     upgradeImage,
			},
			APIExtensions: definitions.client,
			Definitions:   definitionSource(releaseDefinitions()),
			Dynamic:       controlPlane,
			Poll:          time.Millisecond,
			Wait:          time.Second,
		},
		order:        order,
		definitions:  definitions,
		controlPlane: controlPlane,
	}
}

// recordControlPlane records every write the upgrade makes to the cluster.
func recordControlPlane(order *writeOrder, client dynamic.Interface) {
	fake, ok := client.(*dynamicfake.FakeDynamicClient)
	if !ok {
		panic("the recording client is the fake dynamic client")
	}
	record := func(action k8stesting.Action) {
		order.record("control-plane/" + action.GetResource().Resource)
	}
	fake.PrependReactor("create", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		record(action)
		return false, nil, nil
	})
	fake.PrependReactor("update", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		record(action)
		return false, nil, nil
	})
}

// TestUpgradeAppliesDefinitionsBeforeTheControlPlane covers C-55's fixed order:
// the release's object definitions are written and served before any
// control-plane object is replaced, a definition the release ships unchanged
// takes no write, and the report names what took effect in that order.
func TestUpgradeAppliesDefinitionsBeforeTheControlPlane(t *testing.T) {
	world := newUpgradeWorld(t)

	result, err := Upgrade(context.Background(), world.config)
	if err != nil {
		t.Fatalf("upgrade: %v", err)
	}

	lastDefinition := world.order.last("definition/")
	firstControlPlane := world.order.first("control-plane/")
	if lastDefinition < 0 || firstControlPlane < 0 {
		t.Fatalf("the upgrade made an incomplete set of writes: %v", world.order.steps)
	}
	if lastDefinition > firstControlPlane {
		t.Fatalf("the upgrade replaced a control-plane object before the definitions were served: %v", world.order.steps)
	}
	// Only the definition whose shape moved takes a write; the one served as the
	// release ships it is left alone.
	if writes := world.order.count("definition/"); writes != 1 {
		t.Fatalf("the upgrade wrote %d definitions, want only the one whose shape moved: %v", writes, world.order.steps)
	}
	// The report follows the release's order, which is not the name order.
	want := []string{"volumes.kos.tsic.top", "applications.kos.tsic.top"}
	if len(result.Definitions) != len(want) || result.Definitions[0] != want[0] || result.Definitions[1] != want[1] {
		t.Fatalf("result definitions = %v, want %v", result.Definitions, want)
	}
	if !contains(result.Objects, clusterManaged+"/deployments/"+ControlPlaneName) {
		t.Fatalf("result objects = %v, want the workload the release names", result.Objects)
	}
	// The namespace is the installation's own object: an upgrade never rewrites
	// it, so labels another tool put there survive.
	if step := world.order.first("control-plane/namespaces"); step >= 0 {
		t.Fatalf("the upgrade rewrote the installation namespace: %v", world.order.steps)
	}
	if image := workloadImage(t, world.controlPlane); image != upgradeImage {
		t.Fatalf("the workload image = %q, want the release's %q", image, upgradeImage)
	}
	if shape := definitionShape(t, world.config, "applications.kos.tsic.top"); shape != "the release's shape" {
		t.Fatalf("served definition shape = %q, want the release's", shape)
	}
}

// TestUpgradeWaitsForACreatedDefinitionToBeServed covers the wait on the state
// an API server really reports: a definition the upgrade creates is served but
// not established until the controller publishes the condition, and no
// control-plane object moves before that. The condition carries no
// observedGeneration on the pinned server, so the wait reads the condition and
// the stored spec, not a generation.
func TestUpgradeWaitsForACreatedDefinitionToBeServed(t *testing.T) {
	world := newUpgradeWorld(t)
	world.config.Definitions = definitionSource(append(releaseDefinitions(),
		definitionOf("providers.kos.tsic.top", "Provider", "providers", "the release's shape", "v1alpha1", "v1beta1")))

	if _, err := Upgrade(context.Background(), world.config); err != nil {
		t.Fatalf("upgrade: %v", err)
	}

	established := world.order.first("established/providers.kos.tsic.top")
	if established < 0 {
		t.Fatalf("the upgrade never observed the created definition being established: %v", world.order.steps)
	}
	if first := world.order.first("control-plane/"); first < established {
		t.Fatalf("the upgrade replaced the control plane before the created definition was served: %v", world.order.steps)
	}
	if reads := world.definitions.reads["providers.kos.tsic.top"]; reads < 2 {
		t.Fatalf("the upgrade read the created definition %d times, want it to wait for the controller", reads)
	}
}

// TestUpgradeFailsWhenTheApiServerDoesNotServeWhatItWrote covers the read-back
// half of the wait: a definition the API server stored in another shape than the
// release wrote is not served as written, so the control plane is not replaced
// on top of it.
func TestUpgradeFailsWhenTheApiServerDoesNotServeWhatItWrote(t *testing.T) {
	world := newUpgradeWorld(t)
	world.config.Wait = 50 * time.Millisecond
	// The server keeps one version of the definition it was handed, the way a
	// server that prunes a field it does not know would.
	world.config.APIExtensions.(*apiextensionsfake.Clientset).PrependReactor("update", "customresourcedefinitions",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			updated := action.(k8stesting.UpdateAction).GetObject().(*apiextensionsv1.CustomResourceDefinition).DeepCopy()
			if len(updated.Spec.Versions) > 1 {
				updated.Spec.Versions = updated.Spec.Versions[:1]
			}
			return true, updated, nil
		})

	if _, err := Upgrade(context.Background(), world.config); err == nil {
		t.Fatal("the upgrade replaced the control plane over a definition the server did not serve as written")
	} else if !strings.Contains(err.Error(), "served as written") {
		t.Fatalf("failure %v does not say the definition was not served as written", err)
	}
	if step := world.order.first("control-plane/"); step >= 0 {
		t.Fatalf("the upgrade replaced a control-plane object after a definition it could not serve: %v", world.order.steps)
	}
}

// TestUpgradeKeepsWhatTheApiServerAllocatedOnTheService covers the replacement
// of a served Service: the release names the service port, not the addresses or
// the node ports the API server allocated, and a replacement that dropped them
// would be refused by a real cluster.
func TestUpgradeKeepsWhatTheApiServerAllocatedOnTheService(t *testing.T) {
	world := newUpgradeWorld(t)

	if _, err := Upgrade(context.Background(), world.config); err != nil {
		t.Fatalf("upgrade: %v", err)
	}

	served, err := world.controlPlane.Resource(serviceResource).Namespace(upgradeNamespace).
		Get(context.Background(), ControlPlaneName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the replaced service: %v", err)
	}
	// Every field the API server allocated is named here, not read from the
	// production list: a field that stopped being carried would leave the
	// replacement without it, and this case would say so.
	for _, field := range []string{"clusterIP", "clusterIPs", "ipFamilies", "ipFamilyPolicy"} {
		if _, found, err := unstructured.NestedFieldNoCopy(served.Object, "spec", field); err != nil || !found {
			t.Fatalf("the replacement lost spec.%s: %v", field, served.Object["spec"])
		}
	}
	address, _, _ := unstructured.NestedString(served.Object, "spec", "clusterIP")
	if address != servedClusterIP {
		t.Fatalf("served cluster IP = %q, want %q", address, servedClusterIP)
	}
	if policy, _, _ := unstructured.NestedString(served.Object, "spec", "ipFamilyPolicy"); policy != "SingleStack" {
		t.Fatalf("served IP family policy = %q, want SingleStack", policy)
	}

	// The rest of the served spec is the release's: the selector of the previous
	// release is gone.
	if selector, _, _ := unstructured.NestedString(served.Object, "spec", "selector", "app"); selector != ControlPlaneName {
		t.Fatalf("served selector = %q, want %q", selector, ControlPlaneName)
	}
}

// TestUpgradeRefusesAReleaseThatStopsServingAServedVersion covers the guard on
// an upgrade that would strand the control plane it is about to replace: the
// definitions move first, so a version the served definition carries is one that
// control plane talks to.
func TestUpgradeRefusesAReleaseThatStopsServingAServedVersion(t *testing.T) {
	world := newUpgradeWorld(t)
	world.config.Definitions = definitionSource([]*apiextensionsv1.CustomResourceDefinition{
		definitionOf("volumes.kos.tsic.top", "Volume", "volumes", "the served shape", "v1alpha1", "v1beta1"),
		definitionOf("applications.kos.tsic.top", "Application", "applications", "the release's shape", "v1beta1"),
	})

	_, err := Upgrade(context.Background(), world.config)
	if err == nil {
		t.Fatal("the upgrade stopped serving a version the running control plane talks to")
	}
	if !strings.Contains(err.Error(), "v1alpha1") {
		t.Fatalf("failure %v does not name the version it would stop serving", err)
	}
	if step := world.order.first("control-plane/"); step >= 0 {
		t.Fatalf("the upgrade replaced a control-plane object after refusing a definition: %v", world.order.steps)
	}
}

// TestUpgradeLeavesTheControlPlaneAloneWhenADefinitionFails covers the failure
// path on the other side: a definition the upgrade cannot write stops the run
// before any control-plane object moves, and the report names what did take
// effect before the failure.
func TestUpgradeLeavesTheControlPlaneAloneWhenADefinitionFails(t *testing.T) {
	world := newUpgradeWorld(t)
	world.config.APIExtensions.(*apiextensionsfake.Clientset).PrependReactor("update", "customresourcedefinitions",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			if action.(k8stesting.UpdateAction).GetObject().(*apiextensionsv1.CustomResourceDefinition).Name != "applications.kos.tsic.top" {
				return false, nil, nil
			}
			return true, nil, errors.New("the API server refused the definition")
		})

	result, err := Upgrade(context.Background(), world.config)
	if err == nil {
		t.Fatal("the upgrade replaced the control plane after a definition it could not write")
	}
	if !strings.Contains(err.Error(), "applications.kos.tsic.top") {
		t.Fatalf("failure %v does not name the definition it could not write", err)
	}
	if step := world.order.first("control-plane/"); step >= 0 {
		t.Fatalf("the upgrade replaced a control-plane object after a failed definition: %v", world.order.steps)
	}
	if len(result.Objects) != 0 {
		t.Fatalf("result objects = %v, want nothing replaced", result.Objects)
	}
	if len(result.Definitions) != 1 || result.Definitions[0] != "volumes.kos.tsic.top" {
		t.Fatalf("result definitions = %v, want the definition that took effect before the failure", result.Definitions)
	}
}

// TestUpgradeWritesTheExternalFormsControlPlaneToItsOwnCluster covers C-54's
// external form: the definitions and the identity go to the managed cluster, the
// workload and its service to the cluster that runs the control plane — where
// the Service replaces the one the previous release left — and the definitions
// still move before either.
func TestUpgradeWritesTheExternalFormsControlPlaneToItsOwnCluster(t *testing.T) {
	// The managed cluster of this form never ran the control plane; the one that
	// does holds the workload and its service already.
	world := newUpgradeWorldWith(t, nil, []runtime.Object{servedDeployment(), servedService()})
	controlPlaneCluster := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), servedDeployment(), servedService())
	recordControlPlane(world.order, controlPlaneCluster)
	world.config.Plan.Mode = ModeExternal
	world.config.Plan.Managed = ManagedEndpoint{Server: "https://managed.example:6443", Authority: []byte("authority")}
	world.config.ControlPlaneCluster = &Cluster{Dynamic: controlPlaneCluster}

	result, err := Upgrade(context.Background(), world.config)
	if err != nil {
		t.Fatalf("upgrade: %v", err)
	}

	// The managed cluster keeps the identity the control plane acts as and its
	// permissions, and no workload: that is what makes the form external.
	if _, err := world.controlPlane.Resource(deploymentResource).Namespace(upgradeNamespace).
		Get(context.Background(), ControlPlaneName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the managed cluster holds the control-plane workload: %v", err)
	}
	if _, err := world.controlPlane.Resource(serviceResource).Namespace(upgradeNamespace).
		Get(context.Background(), ControlPlaneName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the managed cluster holds the control-plane service: %v", err)
	}
	for _, object := range []struct {
		resource string
		name     string
	}{
		{"serviceaccounts", ControlPlaneServiceAccount},
		{"clusterrolebindings", ControlPlaneName},
	} {
		gvr := schema.GroupVersionResource{Version: "v1", Resource: object.resource}
		if object.resource == "clusterrolebindings" {
			gvr = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: object.resource}
		}
		if _, err := world.controlPlane.Resource(gvr).Namespace(namespaceOf(gvr, upgradeNamespace)).
			Get(context.Background(), object.name, metav1.GetOptions{}); err != nil {
			t.Fatalf("the managed cluster does not hold %s %s: %v", object.resource, object.name, err)
		}
	}

	// The control-plane cluster holds the workload the release brings and still
	// holds its service, which this run replaced.
	if image := workloadImageIn(t, controlPlaneCluster); image != upgradeImage {
		t.Fatalf("the control-plane workload runs %q, want the release's %q", image, upgradeImage)
	}
	if _, err := controlPlaneCluster.Resource(serviceResource).Namespace(upgradeNamespace).
		Get(context.Background(), ControlPlaneName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the control-plane cluster lost its service: %v", err)
	}
	if !contains(result.Objects, clusterControlPlane+"/services/"+ControlPlaneName) {
		t.Fatalf("result objects = %v, want the control-plane service named with its cluster", result.Objects)
	}
	if !contains(result.Objects, clusterManaged+"/serviceaccounts/"+ControlPlaneServiceAccount) {
		t.Fatalf("result objects = %v, want the managed identity named with its cluster", result.Objects)
	}

	// The definitions the managed cluster serves move before the workload is
	// replaced anywhere (C-55).
	lastDefinition := world.order.last("definition/")
	firstObject := world.order.first("control-plane/")
	if lastDefinition < 0 || firstObject < 0 || lastDefinition > firstObject {
		t.Fatalf("the upgrade did not write the definitions first: %v", world.order.steps)
	}
}

// namespaceOf is where one rendered resource lives: a cluster-scoped resource
// takes no namespace.
func namespaceOf(gvr schema.GroupVersionResource, namespace string) string {
	if gvr.Resource == "clusterrolebindings" || gvr.Resource == "clusterroles" {
		return ""
	}
	return namespace
}

// TestUpgradeRefusesAConfigurationItCannotHonour covers the wiring the flow
// needs: without the release's definitions, a client for them or the
// control-plane client, an upgrade cannot honour C-55's order and is refused
// before it writes.
func TestUpgradeRefusesAConfigurationItCannotHonour(t *testing.T) {
	cases := []struct {
		name   string
		break_ func(*UpgradeConfig)
	}{
		{"no definitions", func(config *UpgradeConfig) { config.Definitions = nil }},
		{"no definition client", func(config *UpgradeConfig) { config.APIExtensions = nil }},
		{"no control-plane client", func(config *UpgradeConfig) { config.Dynamic = nil }},
		// The two forms disagree about where the control plane runs, and the
		// upgrade refuses the pairing neither form can honour (C-54).
		{"external form without the control-plane cluster", func(config *UpgradeConfig) {
			config.Plan.Mode = ModeExternal
			config.Plan.Managed = ManagedEndpoint{Server: "https://managed.example:6443"}
			config.ControlPlaneCluster = nil
		}},
		{"external cluster on a form that runs its own control plane", func(config *UpgradeConfig) {
			config.ControlPlaneCluster = &Cluster{Dynamic: config.Dynamic}
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			world := newUpgradeWorld(t)
			testCase.break_(&world.config)
			if _, err := Upgrade(context.Background(), world.config); err == nil {
				t.Fatal("an upgrade missing a required wiring was accepted")
			}
			if len(world.order.steps) != 0 {
				t.Fatalf("the refused upgrade wrote %v", world.order.steps)
			}
		})
	}
}

// workloadImage reads the image the control-plane workload runs in the cluster
// the case upgrades.
func workloadImage(t *testing.T, client dynamic.Interface) string {
	t.Helper()
	return workloadImageIn(t, client)
}

// workloadImageIn reads the image one cluster's control-plane workload runs.
func workloadImageIn(t *testing.T, client dynamic.Interface) string {
	t.Helper()
	served, err := client.Resource(deploymentResource).Namespace(upgradeNamespace).
		Get(context.Background(), ControlPlaneName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the replaced workload: %v", err)
	}
	containers, _, _ := unstructured.NestedSlice(served.Object, "spec", "template", "spec", "containers")
	if len(containers) == 0 {
		t.Fatalf("the replaced workload carries no container: %v", served.Object)
	}
	container, _ := containers[0].(map[string]any)
	image, _ := container["image"].(string)
	return image
}

// definitionShape reads the schema description one served definition carries.
func definitionShape(t *testing.T, config UpgradeConfig, name string) string {
	t.Helper()
	served, err := config.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the replaced definition %s: %v", name, err)
	}
	return served.Spec.Versions[0].Schema.OpenAPIV3Schema.Description
}

// contains reports whether values holds want.
func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
