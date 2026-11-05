package install

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/tsic404/kosd/pkg/extension"
	"github.com/tsic404/kosd/pkg/identity"
	"github.com/tsic404/kosd/pkg/markers"
	"github.com/tsic404/kosd/pkg/write"
)

// Defaults that bound one installation run. The quiet window is the C-15
// observation point; a case shortens it to keep the suite fast, the contract
// value is the default here.
const (
	// DefaultQuietWindow is how long no KOS subject may write after the
	// installation settled (C-15).
	DefaultQuietWindow = 60 * time.Second
	// DefaultSettleTimeout bounds the wait for every reconciled object.
	DefaultSettleTimeout = 2 * time.Minute
	// DefaultNodeTimeout bounds the bootstrap wait for the declared node count.
	DefaultNodeTimeout = 5 * time.Minute
	// DefaultNodeAgentTimeout bounds the wait for the per-node agent's rollout;
	// an image the hosts cannot pull never becomes ready, and a cold pull of a
	// large image has to fit inside it.
	DefaultNodeAgentTimeout = 5 * time.Minute
	// operationTimeout bounds one create or delete in the install path.
	operationTimeout = 30 * time.Second
)

// Accounts creates the first Owner before the installation ends (C-54, C-63).
type Accounts interface {
	BootstrapOwner(ctx context.Context, username string) (identity.Bootstrap, error)
}

// ExternalAccounts is the account service the external-control-plane form
// needs: the first Owner is created without a password, and the claim window is
// what delivers it once the installation finished (C-54, C-68).
type ExternalAccounts interface {
	// PrepareFirstOwner creates the first Owner, or reuses the one the database
	// already holds, without revealing a password.
	PrepareFirstOwner(ctx context.Context, username string) (identity.Bootstrap, error)
	// OpenBootstrapWindow opens the claim window an installation gets exactly
	// once.
	OpenBootstrapWindow(ctx context.Context) error
}

// Ready reports whether the control plane answers. A nil check means the
// installation does not observe readiness.
type Ready func(ctx context.Context) error

// Provisioner brings the run kernel up on bare hosts (C-54 bootstrap form).
type Provisioner interface {
	Provision(ctx context.Context, nodes int) error
}

// NodeSource reports the run-kernel nodes that joined the bootstrap.
type NodeSource interface {
	ReadyNodes(ctx context.Context) ([]string, error)
}

// Config wires one installation.
type Config struct {
	// Plan is the installation.
	Plan Plan
	// Dynamic executes the install writes as the installer identity.
	Dynamic dynamic.Interface
	// Kube reads namespaces and writes the installation's own namespace.
	Kube kubernetes.Interface
	// APIExtensions writes the object definitions.
	APIExtensions apiextensionsclientset.Interface
	// Definitions supplies the release's object definition manifests.
	Definitions DefinitionSource
	// Accounts creates the first Owner.
	Accounts Accounts
	// ExternalAccounts prepares the first Owner and opens the claim window; the
	// external form requires it (C-68).
	ExternalAccounts ExternalAccounts
	// ControlPlaneCluster runs the control-plane workload when that is not the
	// cluster the installation reconciles; nil means the installation's own
	// cluster, which is the case for every other form (C-54).
	ControlPlaneCluster *Cluster
	// Audit reads the API server audit log; nil makes the window unmeasured.
	Audit AuditSource
	// Subjects are the KOS identities the invariant judges; empty selects
	// SubjectUsers of the plan's namespace.
	Subjects []string
	// ExtensionClasses are the object classes installed extensions contribute.
	// They are merged with the classes Artifacts derives at judgment time.
	ExtensionClasses []schema.GroupKind
	// Artifacts reads installed components' declarations, so the window
	// whitelists the classes those components contribute (C-15); nil uses only
	// ExtensionClasses.
	Artifacts extension.ArtifactSource
	// Injections is the C-58 injection list.
	Injections []Injection
	// Ready observes the control plane; nil skips the readiness step.
	Ready Ready
	// Provisioner brings up the run kernel (bootstrap form only).
	Provisioner Provisioner
	// Nodes reports the joined nodes (bootstrap form only).
	Nodes NodeSource
	// QuietWindow is the C-15 quiet period; zero selects DefaultQuietWindow.
	QuietWindow time.Duration
	// SettleTimeout bounds the wait for the observation points; zero selects
	// DefaultSettleTimeout.
	SettleTimeout time.Duration
	// NodeTimeout bounds the bootstrap wait; zero selects DefaultNodeTimeout.
	NodeTimeout time.Duration
	// NodeAgentTimeout bounds the wait for the per-node agent; zero selects
	// DefaultNodeAgentTimeout.
	NodeAgentTimeout time.Duration
	// NoNodeAgentController skips the node-agent rollout wait for a cluster
	// that runs no controller scheduling DaemonSets, such as an API server on
	// its own: the status can never report a rollout there. It is false in
	// every delivery, where the wait holds the installation to the third
	// artifact.
	NoNodeAgentController bool
	// AfterStep observes one completed step; a case uses it to act inside the
	// install window.
	AfterStep func(ctx context.Context, step string) error
	// Snapshotter overrides the default snapshot reader.
	Snapshotter SnapshotSource
	// Clock supplies the window start; nil means the wall clock.
	Clock func() time.Time
}

// Result is what one installation run reports.
type Result struct {
	// Mode is the installation form.
	Mode Mode
	// Namespace is the installation's own namespace.
	Namespace string
	// Owner is the first Owner's login name.
	Owner string
	// OneTimePassword is the first-login password; it is set only when this
	// run created the Owner.
	OneTimePassword string
	// Nodes is the declared node count the run observed (bootstrap only).
	Nodes int
	// ControlPlaneReady reports whether the readiness check passed.
	ControlPlaneReady bool
	// Steps are the steps that completed, in execution order.
	Steps []string
	// StoppedAt names the step that failed; empty when no step failed.
	StoppedAt string
	// RolledBack reports whether the failed run removed everything it created.
	RolledBack bool
	// Completed reports whether the installation finished: every step ran, the
	// install window passed and, for the external form, the claim window
	// opened. A run that failed inside or after its steps is not completed.
	Completed bool
	// Window is the install-window measurement (C-15).
	Window Verdict
	// OwnerClaim is where the first Owner's one-time password is claimed. Only
	// the external form sets it: that form never puts the password in the
	// install output (C-68).
	OwnerClaim OwnerClaim
}

// OwnerClaim is the instruction an external installation prints instead of a
// password (C-68): the claim endpoint and what the operator does there.
type OwnerClaim struct {
	// URL is the claim endpoint on the control-plane host.
	URL string
	// Instruction tells the operator what to run.
	Instruction string
}

// Installer runs one installation.
type Installer struct {
	config     Config
	plan       Plan
	injections []Injection
	// caller is the API server identity this run writes as. C-15 judges the
	// writes of the installation itself, which a kubeconfig install never
	// names through a service account (ADR-017).
	caller string
	// injector counts the attempts of the current run; each run starts from
	// the same list, so a repeated run stops at the same step (C-58).
	injector *Injector
	created  []createdObject
	steps    []string
	stopped  string
	owner    string
	oneTime  string
	nodes    int
	ready    bool
	// claimOpened reports that this run opened the first Owner's claim window,
	// so only a completed installation prints a claim instruction (C-68).
	claimOpened bool
}

// createdObject is one object this run created and would remove on rollback.
type createdObject struct {
	// cluster is the cluster the object lives in; nil means the managed one.
	cluster     *Cluster
	gvr         schema.GroupVersionResource
	namespace   string
	name        string
	isNamespace bool
	definition  bool
}

// managed returns the cluster the installation reconciles: the configured
// clients, which the external form points at the managed cluster.
func (i *Installer) managed() *Cluster {
	return &Cluster{Dynamic: i.config.Dynamic, Kube: i.config.Kube, APIExtensions: i.config.APIExtensions}
}

// controlPlane returns the cluster that runs the control-plane workload.
func (i *Installer) controlPlane() *Cluster {
	if i.config.ControlPlaneCluster != nil {
		return i.config.ControlPlaneCluster
	}
	return i.managed()
}

// clusterOf resolves the cluster a created object lives in.
func (i *Installer) clusterOf(created createdObject) *Cluster {
	if created.cluster != nil {
		return created.cluster
	}
	return i.managed()
}

// New validates the wiring of one installation.
func New(config Config) (*Installer, error) {
	plan, err := config.Plan.Normalize()
	if err != nil {
		return nil, err
	}
	switch {
	case config.Dynamic == nil:
		return nil, errors.New("installation needs a dynamic client")
	case config.Kube == nil:
		return nil, errors.New("installation needs a kubernetes client")
	case config.Definitions == nil:
		return nil, errors.New("installation needs the release's object definitions")
	case config.APIExtensions == nil:
		return nil, errors.New("installation needs an apiextensions client")
	case config.Accounts == nil && plan.Mode != ModeExternal:
		return nil, errors.New("installation needs the account service for the first Owner")
	case plan.Mode == ModeBootstrap && config.Provisioner == nil:
		return nil, errors.New("bootstrap installation needs a provisioner")
	case plan.Mode == ModeBootstrap && config.Nodes == nil:
		return nil, errors.New("bootstrap installation needs a node source")
	case plan.Mode == ModeExternal && config.ExternalAccounts == nil:
		return nil, errors.New("external-control-plane installation needs the claim-window account service")
	case plan.Mode == ModeExternal && config.ControlPlaneCluster == nil:
		return nil, errors.New("external-control-plane installation needs the control-plane cluster's clients")
	case plan.Mode != ModeExternal && config.ControlPlaneCluster != nil:
		return nil, fmt.Errorf("install form %q runs its control plane in the cluster it reconciles", plan.Mode)
	}
	if _, err := NewInjector(config.Injections); err != nil {
		return nil, err
	}
	return &Installer{config: config, plan: plan, injections: config.Injections}, nil
}

// Plan returns the normalized plan of this installation.
func (i *Installer) Plan() Plan { return i.plan }

// SnapshotSource reads one observation point of the install window. The default
// is *Snapshotter; a caller may wrap it, for example to refuse a snapshot while
// the run kernel is not up yet.
type SnapshotSource interface {
	Take(ctx context.Context) (*Snapshot, error)
}

// Install runs the existing-cluster form (C-54).
func (i *Installer) Install(ctx context.Context) (Result, error) {
	if i.plan.Mode != ModeExisting {
		return Result{}, fmt.Errorf("install form %q is not an existing-cluster installation", i.plan.Mode)
	}
	return i.run(ctx)
}

// Bootstrap runs the bootstrap form: provision the declared nodes, then install
// into the cluster they form (C-54).
func (i *Installer) Bootstrap(ctx context.Context) (Result, error) {
	if i.plan.Mode != ModeBootstrap {
		return Result{}, fmt.Errorf("install form %q is not a bootstrap installation", i.plan.Mode)
	}
	return i.run(ctx)
}

// InstallExternal runs the external-control-plane form (C-54): the managed
// cluster receives the object definitions and the identity the control plane
// acts as, while the control-plane cluster receives the workload, its service
// and the credential to the managed cluster.
func (i *Installer) InstallExternal(ctx context.Context) (Result, error) {
	if i.plan.Mode != ModeExternal {
		return Result{}, fmt.Errorf("install form %q is not an external-control-plane installation", i.plan.Mode)
	}
	return i.run(ctx)
}

// run is the shared window of the three forms. A bootstrap run has to reach an
// API server before it can observe one: the provision and node steps run first,
// so the window opens on a running run kernel instead of failing on a bare host.
// The window always measures the managed cluster, whichever cluster runs the
// control plane.
func (i *Installer) run(ctx context.Context) (Result, error) {
	result := Result{Mode: i.plan.Mode, Namespace: i.plan.Namespace}
	i.reset()
	if i.plan.Mode == ModeBootstrap {
		if err := i.runStep(ctx, StepProvision, func(ctx context.Context) error {
			return i.config.Provisioner.Provision(ctx, i.plan.Nodes)
		}); err != nil {
			return i.partial(result), err
		}
		if err := i.runStep(ctx, StepNodes, i.waitNodes); err != nil {
			return i.partial(result), err
		}
	}
	caller, err := CallerIdentity(ctx, i.config.Kube)
	if err != nil {
		result.Window = Verdict{Reason: err.Error()}
		return i.partial(result), err
	}
	i.caller = caller
	started := i.now()
	before, err := i.snapshot(ctx)
	if err != nil {
		result.Window = Verdict{Reason: fmt.Sprintf("snapshot before install: %v", err)}
		return i.partial(result), fmt.Errorf("snapshot before install: %w", err)
	}

	runErr := i.runInstall(ctx)
	// The installed components' classes must be known before the window waits:
	// the observation point covers extension objects too (C-15). The same list
	// then judges the window.
	classes := append([]schema.GroupKind{}, i.config.ExtensionClasses...)
	classErr := error(nil)
	if runErr == nil {
		derived, err := i.extensionClasses(ctx)
		if err != nil {
			classErr = err
		} else {
			classes = derived
		}
		runErr = i.waitWindow(ctx, classes)
	}
	// The first Owner's claim window opens when the installation is complete,
	// not when its last step ran: the C-15 quiet window and the settle wait
	// above belong to the installation, and a failed run must not leave a
	// window behind (C-68).
	if runErr == nil && i.plan.Mode == ModeExternal {
		if err := i.config.ExternalAccounts.OpenBootstrapWindow(ctx); err != nil {
			runErr = fmt.Errorf("open the first-Owner claim window: %w", err)
		} else {
			i.claimOpened = true
		}
	}
	var rollbackErr error
	if runErr != nil {
		rollbackErr = i.rollback(ctx)
		result.RolledBack = rollbackErr == nil
	}

	after, snapshotErr := i.snapshot(ctx)
	quitErr := error(nil)
	if snapshotErr != nil {
		quitErr = fmt.Errorf("snapshot after install: %w", snapshotErr)
	}
	events, auditErr := i.auditEvents(ctx, started, i.now())
	result.Window = Judgement{
		Before:          before,
		After:           after,
		Events:          events,
		Subjects:        i.subjects(),
		ExtensionKinds:  classes,
		Namespace:       i.plan.Namespace,
		GrantNamespaces: grantNamespaces(i.plan),
		AuditError:      auditErr,
		ScopeError:      classErr,
	}.Verdict()
	if quitErr != nil && result.Window.Measured {
		result.Window.Passed = false
		result.Window.Reason = quitErr.Error()
	}
	result.Completed = runErr == nil
	return i.partial(result), errors.Join(runErr, rollbackErr, quitErr)
}

// partial copies the run's progress into the result.
func (i *Installer) partial(result Result) Result {
	result.StoppedAt = i.stopped
	result.Steps = append([]string{}, i.steps...)
	if i.owner != "" {
		result.Owner = i.owner
	}
	result.OneTimePassword = i.oneTime
	result.Nodes = i.nodes
	result.ControlPlaneReady = i.ready
	// A failed run has no window to point at: printing an address that answers
	// bootstrap_closed would only mislead the operator (C-68).
	if i.claimOpened {
		result.OwnerClaim = OwnerClaim{
			URL: i.plan.Address + identity.BootstrapOwnerPath,
			Instruction: "在控制面主机上向该地址发一次 POST 领取首个 Owner 的一次性口令；" +
				"口令只返回一次，首次登录必须修改。",
		}
	}
	return result
}

// reset starts one run from an empty creation list and a fresh injection
// counter.
func (i *Installer) reset() {
	i.injector, _ = NewInjector(i.injections)
	i.caller = ""
	i.created = nil
	i.steps = nil
	i.stopped = ""
	i.owner = ""
	i.oneTime = ""
	i.nodes = 0
	i.ready = false
	i.claimOpened = false
}

// runInstall executes the installation steps in order.
func (i *Installer) runInstall(ctx context.Context) error {
	if err := i.runStep(ctx, StepPreflight, i.preflight); err != nil {
		return err
	}
	if err := i.runStep(ctx, StepNamespace, i.createNamespace); err != nil {
		return err
	}
	if err := i.runStep(ctx, StepDefinitions, i.createDefinitions); err != nil {
		return err
	}
	if err := i.runStep(ctx, StepPermissions, i.createPermissions); err != nil {
		return err
	}
	if err := i.runStep(ctx, StepNodeAgent, i.createNodeAgent); err != nil {
		return err
	}
	if i.plan.Mode == ModeExternal {
		if err := i.runStep(ctx, StepCredential, i.createCredential); err != nil {
			return err
		}
	}
	if err := i.runStep(ctx, StepControlPlane, i.createControlPlane); err != nil {
		return err
	}
	if i.config.Ready != nil {
		if err := i.runStep(ctx, StepReady, func(ctx context.Context) error {
			if err := i.config.Ready(ctx); err != nil {
				return err
			}
			i.ready = true
			return nil
		}); err != nil {
			return err
		}
	}
	return i.runStep(ctx, StepFirstOwner, i.createFirstOwner)
}

// runStep injects the configured failure, runs one step and remembers where the
// run stopped.
func (i *Installer) runStep(ctx context.Context, name string, run func(context.Context) error) error {
	if err := i.injector.At(name); err != nil {
		i.stopped = name
		return err
	}
	if err := run(ctx); err != nil {
		i.stopped = name
		return fmt.Errorf("install step %q: %w", name, err)
	}
	i.steps = append(i.steps, name)
	if i.config.AfterStep != nil {
		if err := i.config.AfterStep(ctx, name); err != nil {
			i.stopped = name
			return fmt.Errorf("install step %q: %w", name, err)
		}
	}
	return nil
}

// preflight verifies the cluster is usable and that every namespace an
// application declared already exists; installation never creates one (C-54).
func (i *Installer) preflight(ctx context.Context) error {
	for _, namespace := range i.plan.Applications {
		_, err := i.config.Kube.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			return fmt.Errorf("declared namespace %q does not exist; installation creates only its own namespace", namespace)
		case err != nil:
			return fmt.Errorf("read declared namespace %q: %w", namespace, err)
		}
	}
	return nil
}

// createNamespace creates the installation's own namespace when it is missing,
// in every cluster the installation writes to: the managed cluster keeps the
// identity and the object definitions, the control-plane cluster keeps the
// workload (C-54).
func (i *Installer) createNamespace(ctx context.Context) error {
	clusters := []*Cluster{i.managed()}
	if i.plan.Mode == ModeExternal {
		clusters = append(clusters, i.controlPlane())
	}
	seen := map[*Cluster]bool{}
	for _, cluster := range clusters {
		if seen[cluster] {
			continue
		}
		seen[cluster] = true
		if err := i.ensureNamespaceIn(ctx, cluster); err != nil {
			return err
		}
	}
	return nil
}

// ensureNamespaceIn creates the installation's namespace in one cluster.
func (i *Installer) ensureNamespaceIn(ctx context.Context, cluster *Cluster) error {
	_, err := cluster.Kube.CoreV1().Namespaces().Get(ctx, i.plan.Namespace, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("read namespace %q: %w", i.plan.Namespace, err)
	}
	namespace := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]any{
			"name":   i.plan.Namespace,
			"labels": map[string]any{markers.ManagedBy: write.ManagedByValue},
		},
	}}
	created, err := i.ensureDynamic(ctx, cluster, schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}, namespace)
	if err != nil {
		return err
	}
	if created {
		i.created = append(i.created, createdObject{
			cluster:     cluster,
			gvr:         schema.GroupVersionResource{Version: "v1", Resource: "namespaces"},
			name:        i.plan.Namespace,
			isNamespace: true,
		})
	}
	return nil
}

// createDefinitions writes the release's object definitions and waits until the
// API server serves them.
func (i *Installer) createDefinitions(ctx context.Context) error {
	definitions, err := i.config.Definitions.Definitions(ctx)
	if err != nil {
		return err
	}
	for _, definition := range definitions {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := decodeInto(definition, crd); err != nil {
			return err
		}
		_, err := i.config.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{})
		switch {
		case apierrors.IsAlreadyExists(err):
			stored, getErr := i.config.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crd.Name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("read object definition %s: %w", crd.Name, getErr)
			}
			if !sameDefinition(stored, crd) {
				return fmt.Errorf("object definition %s differs from the release; installation does not overwrite an existing schema", crd.Name)
			}
			continue
		case err != nil:
			return fmt.Errorf("create object definition %s: %w", crd.Name, err)
		}
		i.created = append(i.created, createdObject{
			gvr:        schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"},
			name:       crd.Name,
			definition: true,
		})
	}
	for _, definition := range definitions {
		name := definition.GetName()
		if err := i.waitEstablished(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

// sameDefinition compares the parts of a definition that decide the served
// schema. Server-filled defaults outside these fields do not make an otherwise
// identical definition a different one.
func sameDefinition(stored, desired *apiextensionsv1.CustomResourceDefinition) bool {
	return stored.Spec.Group == desired.Spec.Group &&
		equality.Semantic.DeepEqual(stored.Spec.Names, desired.Spec.Names) &&
		stored.Spec.Scope == desired.Spec.Scope &&
		equality.Semantic.DeepEqual(stored.Spec.Versions, desired.Spec.Versions) &&
		stored.Spec.PreserveUnknownFields == desired.Spec.PreserveUnknownFields &&
		equality.Semantic.DeepEqual(conversionOf(stored), conversionOf(desired))
}

// conversionOf reads the conversion strategy, treating an absent block as the
// server's None default. A webhook converter is never reused silently (C-16).
func conversionOf(crd *apiextensionsv1.CustomResourceDefinition) *apiextensionsv1.CustomResourceConversion {
	if crd.Spec.Conversion != nil {
		return crd.Spec.Conversion
	}
	return &apiextensionsv1.CustomResourceConversion{Strategy: apiextensionsv1.NoneConverter}
}

// waitEstablished waits until one definition is served.
func (i *Installer) waitEstablished(ctx context.Context, name string) error {
	deadline := time.Now().Add(operationTimeout)
	for {
		crd, err := i.config.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("read object definition %s: %w", name, err)
		}
		if established(crd) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("object definition %s was not established within %s", name, operationTimeout)
		}
		if err := sleepContext(ctx, 50*time.Millisecond); err != nil {
			return err
		}
	}
}

// established reports the Established condition of a definition.
func established(crd *apiextensionsv1.CustomResourceDefinition) bool {
	for _, condition := range crd.Status.Conditions {
		if condition.Type == apiextensionsv1.Established && condition.Status == apiextensionsv1.ConditionTrue {
			return true
		}
	}
	return false
}

// createPermissions writes the control-plane service account, its cluster-scoped
// role and binding, and the namespaced role and binding of the installation's
// own namespace (C-04). They live in the cluster the control plane acts on,
// which is the managed cluster for every form.
func (i *Installer) createPermissions(ctx context.Context) error {
	documents, err := Render(i.plan)
	if err != nil {
		return err
	}
	for _, document := range documents {
		switch document.GVR.Resource {
		case "serviceaccounts", "clusterroles", "clusterrolebindings", "roles", "rolebindings":
			if err := i.ensureDocument(ctx, i.managed(), document); err != nil {
				return err
			}
		}
	}
	return nil
}

// createNodeAgent writes the per-node agent into the managed cluster (C-54).
// The agent runs where the nodes are, so the external form writes it into the
// cluster it reconciles while its control plane runs elsewhere.
func (i *Installer) createNodeAgent(ctx context.Context) error {
	documents, err := Render(i.plan)
	if err != nil {
		return err
	}
	for _, document := range documents {
		if document.GVR.Resource != "daemonsets" {
			continue
		}
		if err := i.ensureDocument(ctx, i.managed(), document); err != nil {
			return err
		}
	}
	return i.waitNodeAgent(ctx)
}

// waitNodeAgent waits until the controller has observed the workload and every
// node it selected runs an agent. A fresh DaemonSet reports no status at all,
// so an unobserved object's zeros must not read as a finished rollout: an agent
// whose image the host cannot pull would otherwise pass as delivered (C-54).
func (i *Installer) waitNodeAgent(ctx context.Context) error {
	if i.config.NoNodeAgentController {
		return nil
	}
	timeout := i.config.NodeAgentTimeout
	if timeout <= 0 {
		timeout = DefaultNodeAgentTimeout
	}
	deadline := time.Now().Add(timeout)
	for {
		agent, err := i.config.Kube.AppsV1().DaemonSets(i.plan.Namespace).Get(ctx, NodeAgentName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("read the node agent: %w", err)
		}
		observed := agent.Status.ObservedGeneration >= agent.Generation
		if observed && agent.Status.DesiredNumberScheduled > 0 &&
			agent.Status.NumberReady == agent.Status.DesiredNumberScheduled {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the node agent is ready on %d of %d nodes after %s (observed: %t): check its image %q",
				agent.Status.NumberReady, agent.Status.DesiredNumberScheduled, timeout, observed, i.plan.NodeAgentImage)
		}
		if err := sleepContext(ctx, 200*time.Millisecond); err != nil {
			return err
		}
	}
}

// createCredential mints the managed cluster's credential for the control-plane
// identity and stores it in the control-plane cluster (C-14, C-54).
func (i *Installer) createCredential(ctx context.Context) error {
	token, expiresAt, err := ManagedToken(ctx, i.config.Kube, i.plan.Namespace)
	if err != nil {
		return err
	}
	raw, err := managedKubeconfig(i.plan, token)
	if err != nil {
		return err
	}
	created, err := i.writeCredential(ctx, raw, expiresAt)
	if err != nil {
		return err
	}
	if created {
		i.created = append(i.created, createdObject{
			cluster:   i.controlPlane(),
			gvr:       schema.GroupVersionResource{Version: "v1", Resource: "secrets"},
			namespace: i.plan.Namespace,
			name:      ManagedCredentialSecret,
		})
	}
	return nil
}

// createControlPlane writes the control-plane workload and service (C-04). The
// external form writes them into the control-plane cluster, where the
// credential to the managed cluster already sits.
func (i *Installer) createControlPlane(ctx context.Context) error {
	cluster := i.managed()
	render := Render
	if i.plan.Mode == ModeExternal {
		cluster = i.controlPlane()
		render = RenderControlPlane
	}
	documents, err := render(i.plan)
	if err != nil {
		return err
	}
	for _, document := range documents {
		switch document.GVR.Resource {
		case "deployments", "services":
			if err := i.ensureDocument(ctx, cluster, document); err != nil {
				return err
			}
		case "serviceaccounts":
			// The workload names this identity; ServiceAccount admission
			// refuses a pod whose account the cluster does not hold.
			if err := i.ensureDocument(ctx, cluster, document); err != nil {
				return err
			}
		}
	}
	return nil
}

// ensureDocument creates one rendered object in cluster when it is missing.
func (i *Installer) ensureDocument(ctx context.Context, cluster *Cluster, document Document) error {
	created, err := i.ensureDynamic(ctx, cluster, document.GVR, document.Object)
	if err != nil {
		return err
	}
	if created {
		i.created = append(i.created, createdObject{
			cluster:   cluster,
			gvr:       document.GVR,
			namespace: document.Object.GetNamespace(),
			name:      document.Object.GetName(),
		})
	}
	return nil
}

// ensureDynamic creates one object in cluster; an existing object is left as it
// is, so a repeated installation does not roll back someone else's object.
func (i *Installer) ensureDynamic(ctx context.Context, cluster *Cluster, gvr schema.GroupVersionResource, object *unstructured.Unstructured) (bool, error) {
	var writable dynamic.ResourceInterface = cluster.Dynamic.Resource(gvr)
	if namespace := object.GetNamespace(); namespace != "" {
		writable = cluster.Dynamic.Resource(gvr).Namespace(namespace)
	}
	_, err := writable.Create(ctx, object, metav1.CreateOptions{FieldManager: "kos-installer"})
	if apierrors.IsAlreadyExists(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("create %s %s: %w", gvr.Resource, object.GetName(), err)
	}
	return true, nil
}

// createFirstOwner creates the first Owner and keeps its one-time password for
// the run's output (C-54, C-63). The external form hands out no password: it
// prepares the account here and opens the claim window once the installation is
// complete (C-68), which run does after the install window passed.
func (i *Installer) createFirstOwner(ctx context.Context) error {
	if i.plan.Mode == ModeExternal {
		bootstrap, err := i.config.ExternalAccounts.PrepareFirstOwner(ctx, i.plan.Owner)
		if err != nil {
			return fmt.Errorf("prepare the first Owner: %w", err)
		}
		i.owner = bootstrap.Username
		return nil
	}
	bootstrap, err := i.config.Accounts.BootstrapOwner(ctx, i.plan.Owner)
	if err != nil {
		return fmt.Errorf("create the first Owner: %w", err)
	}
	i.owner = bootstrap.Username
	if bootstrap.Created {
		i.oneTime = bootstrap.OneTimePassword
	}
	return nil
}

// waitNodes waits until the run kernel reports the declared node count.
func (i *Installer) waitNodes(ctx context.Context) error {
	timeout := i.config.NodeTimeout
	if timeout <= 0 {
		timeout = DefaultNodeTimeout
	}
	deadline := time.Now().Add(timeout)
	observed := 0
	for {
		nodes, err := i.config.Nodes.ReadyNodes(ctx)
		if err != nil {
			return fmt.Errorf("read the bootstrap nodes: %w", err)
		}
		observed = len(nodes)
		if observed == i.plan.Nodes {
			i.nodes = observed
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("bootstrap declares %d nodes, the run kernel reports %d ready after %s",
				i.plan.Nodes, observed, timeout)
		}
		if err := sleepContext(ctx, 50*time.Millisecond); err != nil {
			return err
		}
	}
}

// waitWindow waits for the observation points and the quiet period of C-15. A
// window whose audit cannot be read is left to the judgment, which reports it
// unmeasured rather than failing the installation.
func (i *Installer) waitWindow(ctx context.Context, classes []schema.GroupKind) error {
	discoveryClient := i.config.Kube.Discovery()
	if err := WaitSettled(ctx, i.config.Dynamic, discoveryClient, classes, i.config.SettleTimeout, nil); err != nil {
		return err
	}
	if i.config.Audit == nil {
		return nil
	}
	quiet := i.config.QuietWindow
	if quiet <= 0 {
		quiet = DefaultQuietWindow
	}
	if err := WaitQuiet(ctx, i.config.Audit, i.subjects(), quiet); err != nil {
		if IsUnavailable(err) {
			return nil
		}
		return err
	}
	return nil
}

// rollback removes what this run created, newest first. It runs after any
// failed step, including an injected one (C-54, C-58).
func (i *Installer) rollback(ctx context.Context) error {
	problems := make([]error, 0)
	for index := len(i.created) - 1; index >= 0; index-- {
		if err := i.remove(ctx, i.created[index]); err != nil {
			problems = append(problems, err)
		}
	}
	return errors.Join(problems...)
}

// remove deletes one created object and waits until it is gone.
func (i *Installer) remove(ctx context.Context, created createdObject) error {
	cluster := i.clusterOf(created)
	switch {
	case created.definition:
		err := cluster.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().Delete(ctx, created.name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete object definition %s: %w", created.name, err)
		}
		return i.waitGone(ctx, created, func(ctx context.Context) error {
			_, err := cluster.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, created.name, metav1.GetOptions{})
			return err
		})
	case created.isNamespace:
		return i.deleteNamespace(ctx, cluster, created.name)
	default:
		var writable dynamic.ResourceInterface = cluster.Dynamic.Resource(created.gvr)
		if created.namespace != "" {
			writable = cluster.Dynamic.Resource(created.gvr).Namespace(created.namespace)
		}
		err := writable.Delete(ctx, created.name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete %s %s: %w", created.gvr.Resource, created.name, err)
		}
		return i.waitGone(ctx, created, func(ctx context.Context) error {
			_, err := writable.Get(ctx, created.name, metav1.GetOptions{})
			return err
		})
	}
}

// deleteNamespace removes the installation's own namespace. The namespace
// controller that would clear its finalizer does not run in every cluster, so
// the installer finalizes the namespace it created itself.
func (i *Installer) deleteNamespace(ctx context.Context, cluster *Cluster, name string) error {
	client := cluster.Kube.CoreV1().Namespaces()
	if err := client.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete namespace %s: %w", name, err)
	}
	deadline := time.Now().Add(operationTimeout)
	for {
		namespace, err := client.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read namespace %s: %w", name, err)
		}
		if namespace.DeletionTimestamp != nil && len(namespace.Spec.Finalizers) > 0 {
			namespace.Spec.Finalizers = nil
			if _, err := client.Finalize(ctx, namespace, metav1.UpdateOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("finalize namespace %s: %w", name, err)
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("namespace %s did not disappear within %s", name, operationTimeout)
		}
		if err := sleepContext(ctx, 50*time.Millisecond); err != nil {
			return err
		}
	}
}

// waitGone polls one deletion until the object is missing.
func (i *Installer) waitGone(ctx context.Context, created createdObject, read func(context.Context) error) error {
	deadline := time.Now().Add(operationTimeout)
	for {
		err := read(ctx)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read %s %s after deletion: %w", created.gvr.Resource, created.name, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s %s did not disappear within %s", created.gvr.Resource, created.name, operationTimeout)
		}
		if err := sleepContext(ctx, 50*time.Millisecond); err != nil {
			return err
		}
	}
}

// snapshot reads one install observation point.
func (i *Installer) snapshot(ctx context.Context) (*Snapshot, error) {
	snapshotter := i.config.Snapshotter
	if snapshotter == nil {
		snapshotter = &Snapshotter{Dynamic: i.config.Dynamic, Discovery: i.config.Kube.Discovery()}
	}
	return snapshotter.Take(ctx)
}

// auditEvents reads the writes the API server recorded inside the window.
func (i *Installer) auditEvents(ctx context.Context, from, to time.Time) ([]AuditEvent, error) {
	if i.config.Audit == nil {
		return nil, ErrAuditUnavailable
	}
	return i.config.Audit.Events(ctx, from, to.Add(time.Second))
}

// subjects returns the KOS identities the invariant judges: the service
// accounts the components run as, the caller this installation runs as, and
// anything the caller configured. The caller is read from the API server, so an
// install driven by a kubeconfig is judged like one driven by a service
// account (C-15, ADR-017).
func (i *Installer) subjects() []string {
	seen := map[string]bool{}
	subjects := make([]string, 0, len(i.config.Subjects)+3)
	for _, subject := range append(append([]string{}, i.config.Subjects...), SubjectUsers(i.plan.Namespace)...) {
		if subject == "" || seen[subject] {
			continue
		}
		seen[subject] = true
		subjects = append(subjects, subject)
	}
	if i.caller != "" && !seen[i.caller] {
		subjects = append(subjects, i.caller)
	}
	sort.Strings(subjects)
	return subjects
}

// extensionClasses merges the configured classes with what the installed
// components declare, so an extension installed inside the window is
// whitelisted (C-15).
func (i *Installer) extensionClasses(ctx context.Context) ([]schema.GroupKind, error) {
	classes := append([]schema.GroupKind{}, i.config.ExtensionClasses...)
	if i.config.Artifacts == nil {
		return classes, nil
	}
	discovered, err := ComponentClasses(ctx, i.config.Dynamic, i.config.Artifacts)
	if err != nil {
		return nil, fmt.Errorf("read installed extension classes: %w", err)
	}
	return append(classes, discovered...), nil
}

// now returns the clock of this run.
func (i *Installer) now() time.Time {
	if i.config.Clock == nil {
		return time.Now().UTC()
	}
	return i.config.Clock().UTC()
}

// decodeInto converts one unstructured manifest to a typed object.
func decodeInto(object *unstructured.Unstructured, target any) error {
	encoded, err := object.MarshalJSON()
	if err != nil {
		return fmt.Errorf("encode %s %s: %w", object.GetKind(), object.GetName(), err)
	}
	if err := json.Unmarshal(encoded, target); err != nil {
		return fmt.Errorf("decode %s %s: %w", object.GetKind(), object.GetName(), err)
	}
	return nil
}
