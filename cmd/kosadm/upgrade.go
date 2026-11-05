package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/api/v1beta1"
	"github.com/tsic404/kosd/pkg/controlplane"
	"github.com/tsic404/kosd/pkg/extension"
	"github.com/tsic404/kosd/pkg/identity"
	"github.com/tsic404/kosd/pkg/markers"
	"github.com/tsic404/kosd/pkg/write"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/pkg/maintenance"
	"github.com/tsic404/kosadm/pkg/upgrade"
)

// upgradeUsage prints the upgrade surface: the platform form moves KOS to the
// release and rolls the declared nodes, the extension form judges a replacement
// package before it is applied.
func upgradeUsage(w io.Writer) {
	fmt.Fprint(w, `kosadm upgrade moves a running KOS to a newer release.

Usage:
  kosadm upgrade platform  [flags]   replace the object definitions, then the control plane
  kosadm upgrade extension [flags]   judge and apply a replacement extension package

The platform form reads the release the same way an installation does:
  --definitions <dir>        directory of the release's object definition manifests
  --image <image>            control-plane image of the release
  --node <name>              node the upgrade takes out; repeatable
  --version <version>        version the nodes report when the operator finished them
  --downtime <duration>      declared per-node downtime estimate, for example 15m
  --identity-database <path> control-plane database the maintenance records go to;
                             required with --node

A release that stops serving a framework API version is judged against the
installed extensions before any definition is replaced; both inputs are required:
  --installed-framework <v>  framework version the installed kernel serves; repeatable
  --package <reference>=<file>  the package of an installed extension the judgement
                             reads; required for every installed component; repeatable

The extension form takes local copies of every package the evaluation reads:
each installed component's current package, so the preflight can diff it, and
the replacement.
  --package <reference>=<file>  artifact a package reference resolves to; repeatable
  --target <component>=<reference>  component that moves to a package; repeatable
`)
}

// runUpgrade dispatches the upgrade forms.
func runUpgrade(args []string, out io.Writer) error {
	if len(args) == 0 {
		upgradeUsage(os.Stderr)
		return exitError{message: "upgrade names no form: platform or extension", code: 2}
	}
	switch args[0] {
	case "platform":
		return runUpgradePlatform(args[1:], out)
	case "extension":
		return runUpgradeExtension(args[1:], out)
	case "help", "-h", "--help":
		upgradeUsage(out)
		return nil
	default:
		upgradeUsage(os.Stderr)
		return exitError{message: fmt.Sprintf("unknown upgrade form %q", args[0]), code: 2}
	}
}

// platformFlags are the flags the platform upgrade names.
type platformFlags struct {
	kubeconfig             string
	definitions            string
	namespace              string
	owner                  string
	image                  string
	nodeAgentImage         string
	identityDatabase       string
	nodes                  repeated
	version                string
	downtime               string
	poll                   time.Duration
	nodeWait               time.Duration
	installedFrameworks    repeated
	packages               assignments
	external               bool
	controlPlaneKubeconfig string
	managedServer          string
	managedAuthority       string
	address                string
}

// register adds the platform form's flags to one command.
func (f *platformFlags) register(set *flag.FlagSet) {
	set.StringVar(&f.kubeconfig, "kubeconfig", "",
		"kubeconfig of the cluster the upgrade reconciles; empty reads KUBECONFIG and ~/.kube/config")
	set.StringVar(&f.definitions, "definitions", "",
		"directory of the release's object definition manifests (required)")
	set.StringVar(&f.namespace, "namespace", install.DefaultNamespace,
		"namespace the installation created for itself")
	set.StringVar(&f.owner, "owner", "owner", "login name of the first Owner")
	set.StringVar(&f.image, "image", "", "control-plane image of the release; empty selects the pinned default")
	set.StringVar(&f.nodeAgentImage, "node-agent-image", "",
		"node-agent image of the release; empty selects the pinned default")
	f.packages = assignments{}
	set.StringVar(&f.identityDatabase, "identity-database", "",
		"control-plane database the maintenance records are written to; required with --node")
	set.Var(&f.nodes, "node", "node the upgrade takes out of scheduling; repeatable")
	set.StringVar(&f.version, "version", "",
		"version the upgraded node reports; required with --node")
	set.StringVar(&f.downtime, "downtime", "",
		"declared per-node downtime estimate, for example 15m; the preflight marks it undetermined when empty")
	set.Var(&f.installedFrameworks, "installed-framework",
		"framework API version the installed kernel serves; repeatable, required: the judgement reads every installed extension (C-16)")
	set.Var(f.packages, "package",
		"a package reference and the local artifact it resolves to, as <reference>=<file>; repeatable, required for every installed component the judgement reads")
	set.DurationVar(&f.poll, "poll", 0, "how long the upgrade waits between two observations; zero selects the flow's default")
	set.DurationVar(&f.nodeWait, "node-wait", 0,
		"how long the upgrade waits for one node to stop its applications and report its version; zero selects the flow's default")
	set.BoolVar(&f.external, "external", false,
		"upgrade the external-control-plane form (C-54)")
	set.StringVar(&f.controlPlaneKubeconfig, "control-plane-kubeconfig", "",
		"kubeconfig of the cluster that runs the control plane (external form)")
	set.StringVar(&f.managedServer, "managed-server", "",
		"address of the managed cluster as the control plane reaches it; empty reads it from --kubeconfig")
	set.StringVar(&f.managedAuthority, "managed-authority", "",
		"PEM bundle the control plane verifies the managed cluster with; empty reads it from --kubeconfig")
	set.StringVar(&f.address, "address", "", "address the control plane answers on its own host (external form)")
}

// validate refuses a platform upgrade the flow cannot honour.
func (f *platformFlags) validate() error {
	if f.definitions == "" {
		return exitError{message: "--definitions is required: name the directory of the release's object definition manifests", code: 2}
	}
	if len(f.installedFrameworks) == 0 {
		return exitError{message: "--installed-framework is required: name the framework API versions the installed kernel serves, so a release that stops serving one is judged before any definition is replaced (C-16)", code: 2}
	}
	if len(f.nodes) == 0 && f.version != "" {
		return exitError{message: "--version names the node upgrade; it needs --node", code: 2}
	}
	if len(f.nodes) > 0 && f.version == "" {
		return exitError{message: "--version is required with --node: a node ends its maintenance when it reports that version", code: 2}
	}
	if len(f.nodes) > 0 && f.identityDatabase == "" {
		return exitError{message: "--identity-database is required with --node: the maintenance records go to the control-plane database", code: 2}
	}
	if !f.external && (f.controlPlaneKubeconfig != "" || f.managedServer != "" || f.managedAuthority != "" || f.address != "") {
		return exitError{message: "--control-plane-kubeconfig, --managed-server, --managed-authority and --address belong to --external", code: 2}
	}
	if f.external && f.controlPlaneKubeconfig == "" {
		return exitError{message: "--external needs --control-plane-kubeconfig: the cluster that runs the control plane", code: 2}
	}
	return nil
}

// plan builds the release plan of the upgrade.
func (f *platformFlags) plan(endpoint func() (install.ManagedEndpoint, error)) (install.Plan, error) {
	plan := install.Plan{
		Mode:           install.ModeExisting,
		Namespace:      f.namespace,
		Owner:          f.owner,
		Image:          f.image,
		NodeAgentImage: f.nodeAgentImage,
	}
	if !f.external {
		return plan.Normalize()
	}
	managed, err := endpoint()
	if err != nil {
		return install.Plan{}, err
	}
	plan.Mode = install.ModeExternal
	plan.Managed = managed
	plan.Address = f.address
	return plan.Normalize()
}

// runUpgradePlatform moves KOS to this release and rolls the declared nodes
// through the node maintenance flow (C-55).
func runUpgradePlatform(args []string, out io.Writer) error {
	set := flag.NewFlagSet("upgrade platform", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	var flags platformFlags
	flags.register(set)
	if err := set.Parse(args); err != nil {
		return exitError{message: err.Error(), code: 2}
	}
	if err := flags.validate(); err != nil {
		return err
	}

	ctx, stop := signalContext()
	defer stop()

	managedConfig, err := loadConfig(flags.kubeconfig)
	if err != nil {
		return err
	}
	managedClients, err := newClients(managedConfig)
	if err != nil {
		return err
	}
	source, err := definitions(flags.definitions)
	if err != nil {
		return err
	}
	plan, err := flags.plan(func() (install.ManagedEndpoint, error) {
		return managedEndpoint(flags.managedServer, flags.managedAuthority, managedConfig)
	})
	if err != nil {
		return exitError{message: err.Error(), code: 2}
	}
	var auditor controlplane.Auditor
	if len(flags.nodes) > 0 {
		store, closeStore, err := openAudit(ctx, flags.identityDatabase)
		if err != nil {
			return err
		}
		defer closeStore()
		auditor = identity.AuditTrail{Store: store}
	}
	flow, request, err := upgradeFlow(managedClients, flags, auditor)
	if err != nil {
		return err
	}
	frameworks, err := frameworkInstaller(ctx, managedClients, flags)
	if err != nil {
		return err
	}
	actor := ""
	// The preflight runs before any write: an item the flow cannot judge stops
	// the upgrade while the cluster is still as it was (C-55).
	if flow != nil {
		actor, err = install.CallerIdentity(ctx, managedClients.kube)
		if err != nil {
			return err
		}
		report, failure := flow.Preflight(ctx, request)
		if failure != nil {
			return upgradeFailure(failure)
		}
		printUpgradeReport(out, report)
		if report.Blocked {
			return exitError{message: "预检有不可判定项，升级不发起：" + strings.Join(report.Undetermined, "、"), code: 1}
		}
	}

	config := install.UpgradeConfig{
		Plan:          plan,
		APIExtensions: managedClients.extended,
		Definitions:   source,
		Dynamic:       managedClients.dynamic,
		Frameworks:    frameworks,
	}
	if flags.external {
		controlConfig, err := loadConfig(flags.controlPlaneKubeconfig)
		if err != nil {
			return err
		}
		controlPlane, err := install.NewCluster(ctx, controlConfig)
		if err != nil {
			return fmt.Errorf("build the control-plane cluster clients: %w", err)
		}
		config.ControlPlaneCluster = controlPlane
	}
	result, upgradeErr := install.Upgrade(ctx, config)
	printUpgradeResult(out, result)
	if upgradeErr != nil {
		return upgradeErr
	}
	if flow == nil {
		return nil
	}
	run, failure := flow.Run(ctx, request, actor)
	if failure != nil {
		return upgradeFailure(failure)
	}
	printNodeResults(out, run)
	return nil
}

// upgradeFlow builds the node maintenance sequence the platform upgrade drives.
// No node named means the upgrade only replaces the release's objects.
func upgradeFlow(clients clients, flags platformFlags, auditor controlplane.Auditor) (*upgrade.Flow, upgrade.Request, error) {
	if len(flags.nodes) == 0 {
		return nil, upgrade.Request{}, nil
	}
	resources, err := resourceNames(clients.kube)
	if err != nil {
		return nil, upgrade.Request{}, err
	}
	flow := &upgrade.Flow{
		Maintenance: &maintenance.Service{
			Dynamic: clients.dynamic,
			Writer:  &write.Writer{Dynamic: clients.dynamic, Resources: resources},
			Pods:    maintenance.Cluster{Kube: clients.kube},
			Auditor: auditor,
		},
		Applications: upgrade.ClusterApplications{Dynamic: clients.dynamic},
		Nodes:        upgrade.ClusterNodes{Dynamic: clients.dynamic},
		Poll:         flags.poll,
		Wait:         flags.nodeWait,
	}
	return flow, upgrade.Request{Nodes: flags.nodes, Version: flags.version, Downtime: flags.downtime}, nil
}

// openAudit opens the control-plane database the maintenance records go to, so
// the node setting the trail feeds shows the upgrade as planned maintenance
// (C-14, C-65). The database must already exist: opening creates and migrates
// one, and a mistyped path would take the records into a database the control
// plane never serves.
func openAudit(ctx context.Context, path string) (*identity.Store, func(), error) {
	if err := requireDatabaseFile(path); err != nil {
		return nil, nil, err
	}
	store, err := identity.Open(ctx, identity.OpenOptions{Path: path})
	if err != nil {
		return nil, nil, fmt.Errorf("open the control-plane database: %w", err)
	}
	return store, func() { _ = store.Close() }, nil
}

// requireDatabaseFile refuses a path that does not name an existing regular
// file, before anything opens it for writing.
func requireDatabaseFile(path string) error {
	info, err := os.Stat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return exitError{message: fmt.Sprintf("--identity-database %s does not exist: the maintenance records go to the control plane's database, and an upgrade never creates one", path), code: 2}
	case err != nil:
		return fmt.Errorf("read the control-plane database %s: %w", path, err)
	case !info.Mode().IsRegular():
		return exitError{message: fmt.Sprintf("--identity-database %s is not a regular file", path), code: 2}
	}
	return nil
}

// frameworkInstaller reads the installed extensions the release's framework
// versions must not strand (C-16). The judgement's inputs are required: every
// installed component's package must be supplied, because the preflight skips
// a component it cannot read and would then report it as not dependent.
func frameworkInstaller(ctx context.Context, clients clients, flags platformFlags) (*extension.Installer, error) {
	resources, err := resourceNames(clients.kube)
	if err != nil {
		return nil, err
	}
	artifacts, err := readPackages(flags.packages)
	if err != nil {
		return nil, exitError{message: err.Error(), code: 2}
	}
	if err := requireExtensionPackages(ctx, clients.dynamic, artifacts); err != nil {
		return nil, err
	}
	return &extension.Installer{
		Dynamic:    clients.dynamic,
		Resources:  resources,
		Artifacts:  artifacts,
		Frameworks: flags.installedFrameworks,
	}, nil
}

// requireExtensionPackages refuses a judgement that cannot read every installed
// extension: the framework dependency preflight skips a component whose package
// the operator did not supply, and that extension's dependency on a removed
// framework version would go unlisted (C-16, C-55).
func requireExtensionPackages(ctx context.Context, client dynamic.Interface, artifacts extension.MemoryArtifacts) error {
	plural, ok := api.ResourceName("SystemComponent")
	if !ok {
		return fmt.Errorf("the kernel defines no SystemComponent resource")
	}
	resource := client.Resource(schema.GroupVersionResource{
		Group: api.Group, Version: v1beta1.GroupVersion.Version, Resource: plural,
	})
	list, err := resource.List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list the installed components: %w", err)
	}
	missing := make([]string, 0, len(list.Items))
	for index := range list.Items {
		object := &list.Items[index]
		if object.GetDeletionTimestamp() != nil {
			continue
		}
		attribution, err := markers.AttributionOf(object.GetLabels())
		if err != nil {
			return fmt.Errorf("component %s: %w", object.GetName(), err)
		}
		if attribution == markers.AttributionUnmanaged || attribution == markers.AttributionAdopted {
			continue
		}
		reference, _, err := unstructured.NestedString(object.Object, "spec", "package", "reference")
		if err != nil || reference == "" {
			continue
		}
		if _, supplied := artifacts[reference]; !supplied {
			missing = append(missing, object.GetName()+" ("+reference+")")
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return exitError{message: "--package must supply the package of every installed extension the framework judgement reads; missing: " + strings.Join(missing, ", "), code: 2}
	}
	return nil
}

// upgradeFailure renders one refused step of the flow.
func upgradeFailure(failure *controlplane.Error) error {
	return exitError{message: failure.Message, code: 1}
}

// printUpgradeReport writes the preflight: what the upgrade stops, for how
// long, how many applications are unhealthy and what it cannot judge (C-55).
func printUpgradeReport(out io.Writer, report upgrade.Report) {
	fmt.Fprintf(out, "Unhealthy applications: %d\n", report.UnhealthyApplications)
	for _, plan := range report.Nodes {
		fmt.Fprintf(out, "Node %s (%s)\n", plan.Node, plan.KubeletVersion)
		fmt.Fprintf(out, "  Applications: %s\n", listOrNone(plan.Applications))
		fmt.Fprintf(out, "  Volumes: %s\n", listOrNone(plan.Volumes))
		fmt.Fprintf(out, "  Downtime: %s\n", textOrUnspecified(plan.Downtime))
	}
	if len(report.Undetermined) > 0 {
		fmt.Fprintf(out, "Undetermined: %s\n", strings.Join(report.Undetermined, "; "))
	}
	fmt.Fprintf(out, "Blocked: %t\n", report.Blocked)
}

// printUpgradeResult writes what the release's replacement did, definitions
// first (C-55).
func printUpgradeResult(out io.Writer, result install.UpgradeResult) {
	fmt.Fprintf(out, "Definitions applied: %s\n", listOrNone(result.Definitions))
	fmt.Fprintf(out, "Objects replaced: %s\n", listOrNone(result.Objects))
	printFrameworkReport(out, result.Frameworks)
}

// printFrameworkReport writes the framework dependency preflight: the versions
// the release stops serving and the extensions that still depend on them
// (C-16).
func printFrameworkReport(out io.Writer, preflight *extension.FrameworkPreflight) {
	if preflight == nil {
		fmt.Fprintln(out, "Framework dependencies: not judged (the upgrade stopped before the preflight)")
		return
	}
	fmt.Fprintf(out, "Framework target: %s\n", listOrNone(preflight.Target))
	fmt.Fprintf(out, "Framework versions removed: %s\n", listOrNone(preflight.Removed))
	dependents := make([]string, 0, len(preflight.Dependents))
	for _, dependent := range preflight.Dependents {
		dependents = append(dependents, dependent.Component+" ("+dependent.FrameworkVersion+")")
	}
	fmt.Fprintf(out, "Framework dependents: %s\n", listOrNone(dependents))
	fmt.Fprintf(out, "Framework blocked: %t\n", preflight.IsBlocked)
}

// printNodeResults writes what each node's maintenance did.
func printNodeResults(out io.Writer, result upgrade.Result) {
	for _, node := range result.Nodes {
		fmt.Fprintf(out, "Node %s\n", node.Node)
		fmt.Fprintf(out, "  Evicted: %s\n", listOrNone(node.Drain.Evicted))
		retained := make([]string, 0, len(node.Drain.Retained))
		for _, entry := range node.Drain.Retained {
			retained = append(retained, entry.Pod+" ("+entry.Reason+")")
		}
		fmt.Fprintf(out, "  Retained: %s\n", listOrNone(retained))
	}
	fmt.Fprintf(out, "Restored: %s\n", listOrNone(result.Cordoned))
}

// runUpgradeExtension judges a replacement extension package and applies it
// only when the checks pass (C-55).
func runUpgradeExtension(args []string, out io.Writer) error {
	set := flag.NewFlagSet("upgrade extension", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	var flags extensionFlags
	flags.register(set)
	if err := set.Parse(args); err != nil {
		return exitError{message: err.Error(), code: 2}
	}
	if len(flags.targets) == 0 {
		return exitError{message: "--target is required: name the component and the package it moves to", code: 2}
	}

	ctx, stop := signalContext()
	defer stop()

	config, err := loadConfig(flags.kubeconfig)
	if err != nil {
		return err
	}
	clients, err := newClients(config)
	if err != nil {
		return err
	}
	resources, err := resourceNames(clients.kube)
	if err != nil {
		return err
	}

	artifacts, err := readPackages(flags.packages)
	if err != nil {
		return exitError{message: err.Error(), code: 2}
	}
	targets, err := upgradeTargets(flags.targets, artifacts)
	if err != nil {
		return exitError{message: err.Error(), code: 2}
	}

	installer := &extension.Installer{
		Dynamic:   clients.dynamic,
		Resources: resources,
		Artifacts: artifacts,
		Status:    &write.Writer{Dynamic: clients.dynamic, Resources: resources},
	}
	preflight, err := installer.UpgradePreflight(ctx, targets)
	if err != nil {
		return fmt.Errorf("judge the extension upgrade: %w", err)
	}
	printExtensionPreflight(out, preflight)
	if preflight.IsBlocked {
		return exitError{message: "扩展升级被拒绝：" + blockedReasons(preflight), code: 1}
	}

	if err := repointComponents(ctx, clients.dynamic, targets); err != nil {
		return err
	}
	installation, err := installer.Install(ctx)
	if err != nil {
		return fmt.Errorf("apply the extension upgrade: %w", err)
	}
	for _, target := range targets {
		component, found := installation.Component(target.Component)
		if !found {
			fmt.Fprintf(out, "Component %s left the installation\n", target.Component)
			continue
		}
		fmt.Fprintf(out, "Component %s: %s\n", target.Component, component.Phase)
	}
	return nil
}

// extensionFlags are the flags the extension upgrade names.
type extensionFlags struct {
	kubeconfig string
	packages   assignments
	targets    assignments
}

// register adds the extension form's flags to one command.
func (f *extensionFlags) register(set *flag.FlagSet) {
	f.packages = assignments{}
	f.targets = assignments{}
	set.StringVar(&f.kubeconfig, "kubeconfig", "",
		"kubeconfig of the cluster the upgrade reconciles; empty reads KUBECONFIG and ~/.kube/config")
	set.Var(f.packages, "package",
		"a package reference and the local artifact it resolves to, as <reference>=<file>; repeatable")
	set.Var(f.targets, "target",
		"a component and the package reference it moves to, as <component>=<reference>; repeatable")
}

// assignments collects repeated name=value flags.
type assignments map[string]string

// String implements flag.Value.
func (a assignments) String() string {
	entries := make([]string, 0, len(a))
	for name, value := range a {
		entries = append(entries, name+"="+value)
	}
	sort.Strings(entries)
	return strings.Join(entries, ",")
}

// Set implements flag.Value.
func (a assignments) Set(value string) error {
	name, target, found := strings.Cut(value, "=")
	name = strings.TrimSpace(name)
	target = strings.TrimSpace(target)
	if !found || name == "" || target == "" {
		return fmt.Errorf("%q is not <name>=<value>", value)
	}
	if _, duplicate := a[name]; duplicate {
		return fmt.Errorf("%q is named more than once", name)
	}
	a[name] = target
	return nil
}

// readPackages reads the local artifacts the operator named.
func readPackages(packages assignments) (extension.MemoryArtifacts, error) {
	artifacts := extension.MemoryArtifacts{}
	references := make([]string, 0, len(packages))
	for reference := range packages {
		references = append(references, reference)
	}
	sort.Strings(references)
	for _, reference := range references {
		content, err := os.ReadFile(packages[reference])
		if err != nil {
			return nil, fmt.Errorf("read the package of %s: %w", reference, err)
		}
		artifacts[reference] = content
	}
	return artifacts, nil
}

// upgradeTargets pairs every component with the digest of the artifact its
// target reference names. A target the operator did not supply is refused
// rather than fetched from nowhere.
func upgradeTargets(targets assignments, artifacts extension.MemoryArtifacts) ([]extension.UpgradeTarget, error) {
	components := make([]string, 0, len(targets))
	for component := range targets {
		components = append(components, component)
	}
	sort.Strings(components)
	resolved := make([]extension.UpgradeTarget, 0, len(components))
	for _, component := range components {
		reference := targets[component]
		content, ok := artifacts[reference]
		if !ok {
			return nil, fmt.Errorf("target %s names package %q, which --package does not supply", component, reference)
		}
		resolved = append(resolved, extension.UpgradeTarget{
			Component: component,
			Reference: reference,
			Digest:    extension.DigestOf(content),
		})
	}
	return resolved, nil
}

// printExtensionPreflight writes the contribution difference item by item and
// the problems that block the upgrade (C-55).
func printExtensionPreflight(out io.Writer, preflight extension.UpgradePreflight) {
	for _, component := range preflight.Components {
		fmt.Fprintf(out, "Component %s: %s -> %s\n", component.Component, component.From, component.To)
		for _, change := range component.Changes {
			fmt.Fprintf(out, "  %s: -%s +%s\n", change.Class,
				listOrNone(change.Removed), listOrNone(change.Added))
		}
		for _, problem := range component.Problems {
			fmt.Fprintf(out, "  Problem: %s\n", problem)
		}
	}
	fmt.Fprintf(out, "Blocked: %t\n", preflight.IsBlocked)
}

// blockedReasons names every problem that refuses the upgrade.
func blockedReasons(preflight extension.UpgradePreflight) string {
	reasons := make([]string, 0, len(preflight.Components))
	for _, component := range preflight.Components {
		if !component.IsBlocked {
			continue
		}
		for _, problem := range component.Problems {
			reasons = append(reasons, component.Component+": "+problem)
		}
	}
	if len(reasons) == 0 {
		return "没有可应用的目标"
	}
	return strings.Join(reasons, "；")
}

// repointComponents writes the replacement declaration onto each component, as
// a package upgrade does before the install pass that applies it (C-17, C-55).
func repointComponents(ctx context.Context, client dynamic.Interface, targets []extension.UpgradeTarget) error {
	plural, ok := api.ResourceName("SystemComponent")
	if !ok {
		return fmt.Errorf("the kernel defines no SystemComponent resource")
	}
	resource := client.Resource(schema.GroupVersionResource{
		Group: api.Group, Version: v1beta1.GroupVersion.Version, Resource: plural,
	})
	for _, target := range targets {
		object, err := resource.Get(ctx, target.Component, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("component %s is not installed", target.Component)
		}
		if err != nil {
			return fmt.Errorf("read component %s: %w", target.Component, err)
		}
		if err := unstructured.SetNestedField(object.Object, target.Reference, "spec", "package", "reference"); err != nil {
			return fmt.Errorf("set the package reference of %s: %w", target.Component, err)
		}
		if err := unstructured.SetNestedField(object.Object, target.Digest, "spec", "package", "digest"); err != nil {
			return fmt.Errorf("set the package digest of %s: %w", target.Component, err)
		}
		if _, err := resource.Update(ctx, object, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("write the replacement declaration of %s: %w", target.Component, err)
		}
	}
	return nil
}

// resourceNames maps every served kind to its resource name, so the write path
// resolves a rendered object without guessing.
func resourceNames(client kubernetes.Interface) (map[schema.GroupKind]string, error) {
	_, lists, err := client.Discovery().ServerGroupsAndResources()
	if err != nil {
		var failed *discovery.ErrGroupDiscoveryFailed
		if !errors.As(err, &failed) {
			return nil, fmt.Errorf("discover the served resources: %w", err)
		}
		// One unavailable API group does not stop the write path resolving the
		// rest; a kind from that group fails where it is used.
	}
	names := make(map[schema.GroupKind]string, len(lists))
	for _, list := range lists {
		groupVersion, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			continue
		}
		for _, resource := range list.APIResources {
			if strings.Contains(resource.Name, "/") {
				continue
			}
			key := schema.GroupKind{Group: groupVersion.Group, Kind: resource.Kind}
			if _, known := names[key]; !known {
				names[key] = resource.Name
			}
		}
	}
	return names, nil
}

// listOrNone renders one list for the operator.
func listOrNone(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	return strings.Join(values, ", ")
}

// textOrUnspecified renders one optional text.
func textOrUnspecified(value string) string {
	if value == "" {
		return "unspecified"
	}
	return value
}
