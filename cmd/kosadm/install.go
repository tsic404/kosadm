package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/tsic404/kosd/pkg/identity"

	"github.com/tsic404/kosadm/pkg/install"
)

// runInstall executes the existing-cluster form, or the external-control-plane
// form when --external is set (C-54).
func runInstall(args []string, out io.Writer) error {
	set := flag.NewFlagSet("install", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	var flags installFlags
	flags.register(set)
	external := set.Bool("external", false,
		"run the control plane outside the managed cluster (C-54 external-control-plane form)")
	controlPlaneKubeconfig := set.String("control-plane-kubeconfig", "",
		"kubeconfig of the cluster that runs the control plane (external form)")
	managedServer := set.String("managed-server", "",
		"address of the managed cluster as the control plane reaches it; empty reads it from --kubeconfig")
	managedAuthority := set.String("managed-authority", "",
		"PEM bundle the control plane verifies the managed cluster with; empty reads it from --kubeconfig")
	address := set.String("address", "",
		"address the control plane answers on its own host (external form); empty selects the default")
	if err := set.Parse(args); err != nil {
		return exitError{message: err.Error(), code: 2}
	}
	if err := flags.validate(); err != nil {
		return err
	}
	if !*external && (*controlPlaneKubeconfig != "" || *managedServer != "" || *managedAuthority != "" || *address != "") {
		return exitError{message: "--control-plane-kubeconfig, --managed-server, --managed-authority and --address belong to --external", code: 2}
	}
	if *external && *controlPlaneKubeconfig == "" {
		return exitError{message: "--external needs --control-plane-kubeconfig: the cluster that runs the control plane", code: 2}
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
	accounts, closeAccounts, err := openAccounts(ctx, flags.identityDatabase)
	if err != nil {
		return err
	}
	defer closeAccounts()

	plan := install.Plan{
		Mode:           install.ModeExisting,
		Namespace:      flags.namespace,
		Owner:          flags.owner,
		Applications:   flags.applications,
		Image:          flags.image,
		NodeAgentImage: flags.nodeAgentImage,
	}
	if *external {
		endpoint, err := managedEndpoint(*managedServer, *managedAuthority, managedConfig)
		if err != nil {
			return exitError{message: err.Error(), code: 2}
		}
		plan.Mode = install.ModeExternal
		plan.Managed = endpoint
		plan.Address = *address
	}
	normalized, err := plan.Normalize()
	if err != nil {
		return exitError{message: err.Error(), code: 2}
	}
	config, err := flags.config(normalized, managedClients, source, accounts)
	if err != nil {
		return err
	}
	if *external {
		controlConfig, err := loadConfig(*controlPlaneKubeconfig)
		if err != nil {
			return err
		}
		controlPlane, err := install.NewCluster(ctx, controlConfig)
		if err != nil {
			return fmt.Errorf("build the control-plane cluster clients: %w", err)
		}
		// The external form prepares the account without revealing a password
		// and delivers it through the claim window instead (C-68).
		config.Accounts = nil
		config.ExternalAccounts = accounts
		config.ControlPlaneCluster = controlPlane
	}
	runner, err := install.New(config)
	if err != nil {
		return err
	}
	var run install.Result
	var runErr error
	if *external {
		run, runErr = runner.InstallExternal(ctx)
	} else {
		run, runErr = runner.Install(ctx)
	}
	printResult(out, normalized, run)
	return runErr
}

// runBootstrap executes the bootstrap form: the operator's provisioning command
// brings the run kernel up on bare hosts, then the installation fills it
// (C-54).
func runBootstrap(args []string, out io.Writer) error {
	set := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	var flags installFlags
	flags.register(set)
	nodes := set.Int("nodes", 0, "declared node count of the bootstrap; must be at least one")
	provisionCommand := set.String("provision-command", "",
		"command that brings the declared nodes up and writes --kubeconfig (required); KOS_NODES carries the count")
	nodeTimeout := set.Duration("node-timeout", install.DefaultNodeTimeout,
		"how long the installation waits for the declared nodes to join")
	if err := set.Parse(args); err != nil {
		return exitError{message: err.Error(), code: 2}
	}
	if err := flags.validate(); err != nil {
		return err
	}
	if *provisionCommand == "" {
		return exitError{message: "--provision-command is required: KOS does not provision machines, it installs onto a run kernel that command brings up", code: 2}
	}
	if *nodes < 1 {
		return exitError{message: fmt.Sprintf("--nodes must be at least one, got %d", *nodes), code: 2}
	}

	ctx, stop := signalContext()
	defer stop()

	// The command owns machine provisioning and writes the kubeconfig; the
	// installation can only observe the run kernel once it answers.
	if err := provision(ctx, *provisionCommand, *nodes, out); err != nil {
		return err
	}
	clusterConfig, err := awaitCluster(ctx, flags.kubeconfig, *nodeTimeout)
	if err != nil {
		return err
	}
	clusterClients, err := newClients(clusterConfig)
	if err != nil {
		return err
	}
	source, err := definitions(flags.definitions)
	if err != nil {
		return err
	}
	accounts, closeAccounts, err := openAccounts(ctx, flags.identityDatabase)
	if err != nil {
		return err
	}
	defer closeAccounts()

	plan, err := install.Plan{
		Mode:           install.ModeBootstrap,
		Namespace:      flags.namespace,
		Owner:          flags.owner,
		Nodes:          *nodes,
		Applications:   flags.applications,
		Image:          flags.image,
		NodeAgentImage: flags.nodeAgentImage,
	}.Normalize()
	if err != nil {
		return exitError{message: err.Error(), code: 2}
	}
	installConfig, err := flags.config(plan, clusterClients, source, accounts)
	if err != nil {
		return err
	}
	installConfig.Provisioner = completedProvisioner{}
	installConfig.Nodes = clusterNodes{kube: clusterClients.kube}
	installConfig.NodeTimeout = *nodeTimeout
	runner, err := install.New(installConfig)
	if err != nil {
		return err
	}
	run, runErr := runner.Bootstrap(ctx)
	printResult(out, plan, run)
	return runErr
}

// provision runs the operator's command with the declared node count in
// KOS_NODES. Its output is the installation's output.
func provision(ctx context.Context, command string, nodes int, out io.Writer) error {
	runner := exec.CommandContext(ctx, "sh", "-c", command)
	runner.Env = append(os.Environ(), fmt.Sprintf("KOS_NODES=%d", nodes))
	runner.Stdout = out
	runner.Stderr = os.Stderr
	if err := runner.Run(); err != nil {
		return fmt.Errorf("provision the run kernel: %w", err)
	}
	return nil
}

// completedProvisioner reports the provisioning the command already ran: the
// CLI loads the kubeconfig before it builds the clients, so the run kernel
// exists by the time the installation starts.
type completedProvisioner struct{}

// Provision implements install.Provisioner.
func (completedProvisioner) Provision(context.Context, int) error { return nil }

// clusterNodes reads the nodes the run kernel registered.
type clusterNodes struct {
	kube kubernetes.Interface
}

// ReadyNodes implements install.NodeSource.
func (n clusterNodes) ReadyNodes(ctx context.Context) ([]string, error) {
	list, err := n.kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(list.Items))
	for index := range list.Items {
		names = append(names, list.Items[index].Name)
	}
	return names, nil
}

// validate refuses a command that names no definitions or no database.
func (f installFlags) validate() error {
	if f.definitions == "" {
		return exitError{message: "--definitions is required: name the directory of the release's object definition manifests", code: 2}
	}
	if f.identityDatabase == "" {
		return exitError{message: "--identity-database is required: name the database the control plane serves", code: 2}
	}
	return nil
}

// config wires the installation both forms share.
func (f installFlags) config(plan install.Plan, clusterClients clients, source install.DefinitionSource, accounts *identity.Service) (install.Config, error) {
	injections, err := install.ParseInjections(f.injections)
	if err != nil {
		return install.Config{}, exitError{message: err.Error(), code: 2}
	}
	config := install.Config{
		Plan:             plan,
		Dynamic:          clusterClients.dynamic,
		Kube:             clusterClients.kube,
		APIExtensions:    clusterClients.extended,
		Definitions:      source,
		Accounts:         accounts,
		Injections:       injections,
		QuietWindow:      f.quietWindow,
		SettleTimeout:    f.settleTimeout,
		NodeAgentTimeout: f.nodeAgentTimeout,
	}
	if f.auditLog != "" {
		kinds, err := install.DiscoveryKinds(clusterClients.kube.Discovery())
		if err != nil {
			return install.Config{}, fmt.Errorf("resolve the resources the audit log names: %w", err)
		}
		config.Audit = &install.AuditLog{Path: f.auditLog, Kinds: kinds}
	}
	return config, nil
}

// openAccounts opens the control-plane database the first Owner is created in.
func openAccounts(ctx context.Context, path string) (*identity.Service, func(), error) {
	store, err := identity.Open(ctx, identity.OpenOptions{Path: path})
	if err != nil {
		return nil, nil, fmt.Errorf("open the control-plane database: %w", err)
	}
	service := identity.New(store, identity.Config{})
	return service, func() { _ = store.Close() }, nil
}

// managedEndpoint fills the managed cluster's address from flags, falling back
// to the kubeconfig the installation reconciles. A kubeconfig may carry its
// certificate authority as data or as a file path; both reach the credential
// the control plane gets, and a kubeconfig that verifies nothing is refused
// rather than turned into a credential that fails on first use (C-54).
func managedEndpoint(server, authority string, config *rest.Config) (install.ManagedEndpoint, error) {
	endpoint := install.ManagedEndpoint{Server: server}
	if endpoint.Server == "" {
		endpoint.Server = config.Host
	}
	switch {
	case authority != "":
		pem, err := authorityPEM(authority)
		if err != nil {
			return install.ManagedEndpoint{}, err
		}
		endpoint.Authority = pem
	case len(config.CAData) > 0:
		endpoint.Authority = config.CAData
	case config.CAFile != "":
		pem, err := os.ReadFile(config.CAFile)
		if err != nil {
			return install.ManagedEndpoint{}, fmt.Errorf("read the managed cluster's certificate authority %s: %w", config.CAFile, err)
		}
		endpoint.Authority = pem
	}
	if len(endpoint.Authority) == 0 {
		return install.ManagedEndpoint{}, fmt.Errorf("the managed cluster's certificate authority is unknown: name one with --managed-authority or use a kubeconfig that carries it")
	}
	return endpoint, nil
}

// authorityPEM reads the managed cluster's certificate authority from an inline
// PEM bundle or from the file the operator names.
func authorityPEM(value string) ([]byte, error) {
	if strings.Contains(value, "-----BEGIN") {
		return []byte(value), nil
	}
	pem, err := os.ReadFile(value)
	if err != nil {
		return nil, fmt.Errorf("read the managed cluster's certificate authority %s: %w", value, err)
	}
	return pem, nil
}

// printResult writes what the installation delivered (C-54, C-68).
func printResult(out io.Writer, plan install.Plan, result install.Result) {
	fmt.Fprintf(out, "Mode: %s\n", result.Mode)
	fmt.Fprintf(out, "Namespace: %s\n", result.Namespace)
	fmt.Fprintf(out, "Steps: %s\n", strings.Join(result.Steps, ", "))
	if !result.Completed {
		if result.StoppedAt != "" {
			fmt.Fprintf(out, "Stopped at: %s (rolled back: %t)\n", result.StoppedAt, result.RolledBack)
		} else {
			fmt.Fprintf(out, "Failed after the installation steps (rolled back: %t)\n", result.RolledBack)
		}
	}
	fmt.Fprintf(out, "Install window: %s\n", verdictText(result.Window))
	if plan.Mode == install.ModeBootstrap {
		fmt.Fprintf(out, "Nodes: %d\n", result.Nodes)
	}
	// Only a completed installation delivered a credential: a run that failed
	// inside a step, in the settle wait, in the quiet window or while opening
	// the claim window was rolled back and delivered nothing (C-54, C-68).
	if !result.Completed {
		if result.Owner != "" {
			// Rollback removes cluster objects, not the account: the first
			// Owner exists, but no password reached the operator.
			fmt.Fprintf(out, "The first Owner %q exists in the control-plane database, but the installation was rolled back and delivered no credential.\n", result.Owner)
		}
		return
	}
	if result.Owner != "" {
		fmt.Fprintf(out, "First Owner: %s\n", result.Owner)
	}
	if result.OneTimePassword != "" {
		fmt.Fprintf(out, "One-time password: %s\n", result.OneTimePassword)
		fmt.Fprintln(out, "The first sign-in must replace it.")
	} else if plan.Mode != install.ModeExternal && result.Owner != "" {
		fmt.Fprintln(out, "The first Owner already existed; the installation hands out no new password.")
	}
	if result.OwnerClaim.URL != "" {
		fmt.Fprintf(out, "Claim endpoint: %s\n", result.OwnerClaim.URL)
		fmt.Fprintf(out, "  %s\n", result.OwnerClaim.Instruction)
	}
	fmt.Fprintf(out, "Availability: %s\n", plan.Commitment().Notice)
}

// verdictText renders the install window measurement (C-15).
func verdictText(verdict install.Verdict) string {
	switch {
	case !verdict.Measured && verdict.Reason == "":
		return "unmeasured (no audit source)"
	case !verdict.Measured:
		return "unmeasured: " + verdict.Reason
	case verdict.Passed:
		return "passed"
	}
	return fmt.Sprintf("failed: %d violation(s): %s", len(verdict.Violations), violationsText(verdict.Violations))
}

// violationsText names each violation in one line.
func violationsText(violations []install.Violation) string {
	parts := make([]string, 0, len(violations))
	for _, violation := range violations {
		parts = append(parts, violation.String())
	}
	return strings.Join(parts, "; ")
}
