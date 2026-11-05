package install

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"

	"github.com/tsic404/kosd/pkg/extension"
)

// Defaults that bound one upgrade run.
const (
	// defaultUpgradePoll is how long the upgrade waits between two observations
	// of the served definitions.
	defaultUpgradePoll = 50 * time.Millisecond
	// defaultUpgradeWait bounds the wait for one definition to be served.
	defaultUpgradeWait = 2 * time.Minute
)

// UpgradeConfig wires one upgrade of a running installation (C-55).
type UpgradeConfig struct {
	// Plan is the release the installation moves to. Its image is the control
	// plane the upgrade rolls, and its namespace is where the release's objects
	// live.
	Plan Plan
	// APIExtensions writes the object definitions of the cluster this upgrade
	// reconciles — the managed cluster in the external form, where the control
	// plane reads and writes through its cross-cluster credential; required and
	// paired with Dynamic, which addresses the same cluster.
	APIExtensions apiextensionsclientset.Interface
	// Definitions supplies the release's object definition manifests; required.
	Definitions DefinitionSource
	// Dynamic writes the release's manifests into the cluster it reconciles: the
	// identity the control plane acts as and its permissions; required.
	Dynamic dynamic.Interface
	// ControlPlaneCluster runs the control-plane workload when that is not the
	// cluster the installation reconciles; the external form requires it and
	// every other form leaves it nil (C-54).
	ControlPlaneCluster *Cluster
	// Ready observes the upgraded control plane; nil skips the readiness step.
	Ready Ready
	// Frameworks judges the installed extensions that depend on a framework API
	// version this release stops serving (C-16). Its Frameworks field names the
	// versions the installed kernel serves and its Artifacts resolves the
	// installed packages; nil reports the judgement unmeasured and replaces the
	// definitions without it.
	Frameworks *extension.Installer
	// Poll is how long the upgrade waits between two observations of the
	// definitions; zero selects defaultUpgradePoll.
	Poll time.Duration
	// Wait bounds the wait for one definition to be served; zero selects
	// defaultUpgradeWait.
	Wait time.Duration
}

// UpgradeResult is what one upgrade did, in the order it did it (C-55).
type UpgradeResult struct {
	// Frameworks is the framework dependency preflight the run was accepted
	// with; nil when the configuration leaves the judgement unmeasured (C-16).
	Frameworks *extension.FrameworkPreflight `json:"frameworks,omitempty"`
	// Definitions are the object definitions the release carries, applied
	// before anything else, in the order the release lists them.
	Definitions []string `json:"definitions"`
	// Objects are the manifests the upgrade wrote once the definitions were
	// served, in write order, each prefixed with the cluster it landed in
	// ("managed" or "control-plane"): the external form writes into two, and a
	// caller recovering from a half-done run has to know which.
	Objects []string `json:"objects"`
}

// FrameworkBlockedError reports the extensions a release would strand: their
// declarations only run on a framework API version this release stops serving,
// so the upgrade writes nothing and names them (C-16, C-55).
type FrameworkBlockedError struct {
	// Preflight is the judgement that refused the upgrade.
	Preflight extension.FrameworkPreflight
}

// Error implements error.
func (e FrameworkBlockedError) Error() string {
	dependents := make([]string, 0, len(e.Preflight.Dependents))
	for _, dependent := range e.Preflight.Dependents {
		dependents = append(dependents, dependent.Component)
	}
	return "框架版本 " + strings.Join(e.Preflight.Removed, "、") + " 被移除，仍有扩展依赖：" + strings.Join(dependents, "、")
}

// Upgrade brings a running installation to the release the configuration names
// (C-55). The order is fixed: every object definition is applied and served
// before any control-plane object is replaced, so a control plane of either
// version meets the structure it expects while the changeover runs.
//
// The two versions of the release share one structure on every served version
// (C-01, C-16), which is what lets the running control plane and the one this
// upgrade starts read and write the same objects in between.
func Upgrade(ctx context.Context, config UpgradeConfig) (UpgradeResult, error) {
	plan, err := config.normalize()
	if err != nil {
		return UpgradeResult{}, err
	}
	definitions, err := config.Definitions.Definitions(ctx)
	if err != nil {
		return UpgradeResult{}, fmt.Errorf("read the release's object definitions: %w", err)
	}
	result := UpgradeResult{Definitions: make([]string, 0, len(definitions))}
	// The dependency judgement runs before the first definition write: an
	// extension that only runs on a version this release stops serving strands
	// the upgrade, and the cluster is still as it was when the run is refused
	// (C-16, C-55).
	if config.Frameworks != nil {
		preflight, err := config.Frameworks.Preflight(ctx, extension.SupportedFrameworks())
		if err != nil {
			return result, fmt.Errorf("read the framework dependencies: %w", err)
		}
		result.Frameworks = &preflight
		if preflight.IsBlocked {
			return result, FrameworkBlockedError{Preflight: preflight}
		}
	}
	if err := applyDefinitions(ctx, config, definitions, &result); err != nil {
		return result, err
	}
	documents, err := Render(plan)
	if err != nil {
		return result, err
	}
	if err := applyDocuments(ctx, clusterManaged, config.Dynamic, documents, &result); err != nil {
		return result, err
	}
	if config.ControlPlaneCluster != nil {
		controlPlane, err := RenderControlPlane(plan)
		if err != nil {
			return result, err
		}
		if err := applyDocuments(ctx, clusterControlPlane, config.ControlPlaneCluster.Dynamic, controlPlane, &result); err != nil {
			return result, err
		}
	}
	if config.Ready != nil {
		if err := config.Ready(ctx); err != nil {
			return result, fmt.Errorf("the upgraded control plane does not answer: %w", err)
		}
	}
	return result, nil
}

// normalize fills the defaults and refuses a configuration the upgrade cannot
// honour.
func (c UpgradeConfig) normalize() (Plan, error) {
	switch {
	case c.Definitions == nil:
		return Plan{}, errors.New("an upgrade needs the release's object definitions")
	case c.APIExtensions == nil:
		return Plan{}, errors.New("an upgrade needs the definition client")
	case c.Dynamic == nil:
		return Plan{}, errors.New("an upgrade needs the client of the cluster it reconciles")
	}
	plan, err := c.Plan.Normalize()
	if err != nil {
		return Plan{}, err
	}
	switch {
	case plan.Mode == ModeExternal && c.ControlPlaneCluster == nil:
		return Plan{}, errors.New("an external-control-plane upgrade needs the control-plane cluster's clients")
	case plan.Mode != ModeExternal && c.ControlPlaneCluster != nil:
		return Plan{}, fmt.Errorf("upgrade form %q runs its control plane in the cluster it reconciles", plan.Mode)
	}
	return plan, nil
}

// clusterManaged and clusterControlPlane name the two clusters an upgrade may
// write into, so its report says where each manifest landed (C-54).
const (
	clusterManaged      = "managed"
	clusterControlPlane = "control-plane"
)

// applyDocuments writes one cluster's rendered manifests. The namespace is the
// installation's own object: an upgrade writes into it and never rewrites it, so
// the labels other tools put on that namespace survive.
func applyDocuments(ctx context.Context, cluster string, client dynamic.Interface, documents []Document, result *UpgradeResult) error {
	for _, document := range documents {
		if document.GVR.Resource == "namespaces" {
			continue
		}
		if err := applyDocument(ctx, client, document); err != nil {
			return err
		}
		result.Objects = append(result.Objects, cluster+"/"+document.GVR.Resource+"/"+document.Object.GetName())
	}
	return nil
}

// applyDefinitions writes the release's object definitions over the served ones
// and waits until the API server serves them. A definition the release ships
// unchanged takes no write, so a repeated upgrade is not a write storm. The
// result names each definition as it takes effect, so a run that fails part way
// reports the writes behind it instead of none.
func applyDefinitions(ctx context.Context, config UpgradeConfig, definitions []*unstructured.Unstructured, result *UpgradeResult) error {
	applied := make([]*apiextensionsv1.CustomResourceDefinition, 0, len(definitions))
	for _, definition := range definitions {
		desired := &apiextensionsv1.CustomResourceDefinition{}
		if err := decodeInto(definition, desired); err != nil {
			return err
		}
		if err := applyDefinition(ctx, config.APIExtensions, desired); err != nil {
			return err
		}
		result.Definitions = append(result.Definitions, desired.Name)
		applied = append(applied, desired)
	}
	for _, desired := range applied {
		if err := waitServed(ctx, config, desired); err != nil {
			return err
		}
	}
	return nil
}

// applyDefinition writes one definition: it creates one the cluster does not
// serve yet and replaces the served spec of one it does. Replacement is what an
// upgrade is; installation refuses it because an install rolls back instead.
func applyDefinition(ctx context.Context, extensions apiextensionsclientset.Interface, desired *apiextensionsv1.CustomResourceDefinition) error {
	served, err := extensions.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := extensions.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create object definition %s: %w", desired.Name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("read object definition %s: %w", desired.Name, err)
	}
	if dropped := droppedVersions(served, desired); len(dropped) > 0 {
		return fmt.Errorf("object definition %s stops serving %s, which the running control plane was built against",
			desired.Name, strings.Join(dropped, ", "))
	}
	if sameDefinition(served, desired) {
		return nil
	}
	updated := served.DeepCopy()
	updated.Spec = *desired.Spec.DeepCopy()
	if _, err := extensions.ApiextensionsV1().CustomResourceDefinitions().Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("replace object definition %s: %w", desired.Name, err)
	}
	return nil
}

// droppedVersions lists the versions the served definition serves and the
// release no longer does. The definitions move before the control plane, so the
// one still running in front of this upgrade talks to the served set: a release
// that stopped serving one of them would strand it mid-changeover (C-55).
func droppedVersions(served, desired *apiextensionsv1.CustomResourceDefinition) []string {
	kept := make(map[string]bool, len(desired.Spec.Versions))
	for _, version := range desired.Spec.Versions {
		if version.Served {
			kept[version.Name] = true
		}
	}
	dropped := make([]string, 0, len(served.Spec.Versions))
	for _, version := range served.Spec.Versions {
		if version.Served && !kept[version.Name] {
			dropped = append(dropped, version.Name)
		}
	}
	sort.Strings(dropped)
	return dropped
}

// waitServed waits until the API server serves one definition the way the release
// wrote it: it is established, and the spec it holds is the one the upgrade
// applied. A definition the upgrade created only gains the condition after the
// API server has taken it up; one it replaced keeps the condition, so the
// read-back is what says the write landed as written (C-55).
//
// The condition's observedGeneration is not a signal here: the pinned
// apiextensions-apiserver (v0.37.1) never fills it — the field is behind a
// feature gate that is off, the establishing controller does not set it and the
// registry zeroes it — so a comparison against it would wait for a value that
// never arrives. A read that fails says nothing about the definition either, so
// it is retried until the wait runs out instead of failing the upgrade on one
// blip.
func waitServed(ctx context.Context, config UpgradeConfig, desired *apiextensionsv1.CustomResourceDefinition) error {
	deadline := time.Now().Add(config.wait())
	for {
		served, err := config.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, desired.Name, metav1.GetOptions{})
		switch {
		case err == nil && established(served) && sameDefinition(served, desired):
			return nil
		case time.Now().After(deadline):
			if err != nil {
				return fmt.Errorf("read object definition %s: %w", desired.Name, err)
			}
			return fmt.Errorf("object definition %s was not served as written within %s", desired.Name, config.wait())
		}
		if err := sleepContext(ctx, config.poll()); err != nil {
			return err
		}
	}
}

// applyDocument writes one rendered control-plane object: it creates a missing
// one and replaces the served spec of one it can read. The fields the API server
// allocates on a Service are carried over from the served object: the release
// does not name them, and a replacement that dropped them would be refused.
func applyDocument(ctx context.Context, client dynamic.Interface, document Document) error {
	var writable dynamic.ResourceInterface = client.Resource(document.GVR)
	if namespace := document.Object.GetNamespace(); namespace != "" {
		writable = client.Resource(document.GVR).Namespace(namespace)
	}
	served, err := writable.Get(ctx, document.Object.GetName(), metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, err := writable.Create(ctx, document.Object, metav1.CreateOptions{FieldManager: InstallerServiceAccount}); err != nil {
			return fmt.Errorf("create %s %s: %w", document.GVR.Resource, document.Object.GetName(), err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("read %s %s: %w", document.GVR.Resource, document.Object.GetName(), err)
	}
	updated := document.Object.DeepCopy()
	updated.SetResourceVersion(served.GetResourceVersion())
	carryAllocated(served, updated)
	if _, err := writable.Update(ctx, updated, metav1.UpdateOptions{FieldManager: InstallerServiceAccount}); err != nil {
		return fmt.Errorf("replace %s %s: %w", document.GVR.Resource, document.Object.GetName(), err)
	}
	return nil
}

// allocatedServiceFields are the spec fields the API server fills in on the
// Service a release names and rejects a replacement for changing. They are the
// fields of the shape the release renders — a ClusterIP service — so a node port
// or a load balancer's health-check port is not among them: those only exist for
// a type the release does not render, and carrying them onto a ClusterIP
// replacement would be refused.
var allocatedServiceFields = []string{"clusterIP", "clusterIPs", "ipFamilies", "ipFamilyPolicy"}

// carryAllocated copies what the API server allocated on one served object onto
// its replacement, for the fields the release does not name.
func carryAllocated(served, updated *unstructured.Unstructured) {
	if updated.GetKind() != "Service" {
		return
	}
	for _, field := range allocatedServiceFields {
		if _, found, err := unstructured.NestedFieldNoCopy(updated.Object, "spec", field); err != nil || found {
			continue
		}
		value, found, err := unstructured.NestedFieldNoCopy(served.Object, "spec", field)
		if err != nil || !found {
			continue
		}
		_ = unstructured.SetNestedField(updated.Object, value, "spec", field)
	}
}

// poll returns the observation interval.
func (c UpgradeConfig) poll() time.Duration {
	if c.Poll > 0 {
		return c.Poll
	}
	return defaultUpgradePoll
}

// wait returns how long one definition may take to be served.
func (c UpgradeConfig) wait() time.Duration {
	if c.Wait > 0 {
		return c.Wait
	}
	return defaultUpgradeWait
}
