// Command kosadm installs KOS: the existing-cluster form and the bootstrap
// form in this command, the external-control-plane form under install
// --external. It prints what the installation delivered, which form the
// first-Owner credential takes (C-54): the one-time password in the output, or
// the claim endpoint for the external form (C-68).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/tsic404/kosadm/pkg/install"
)

// exitError is a failure the command reports without a stack.
type exitError struct {
	message string
	code    int
}

// Error implements error.
func (e exitError) Error() string { return e.message }

// usage prints the command surface.
func usage(w io.Writer) {
	fmt.Fprint(w, `kosadm installs and runs the lifecycle of KOS.

Usage:
  kosadm install   [flags]   install into a cluster that already runs
  kosadm bootstrap [flags]   bring the run kernel up on a bare host, then install
  kosadm uninstall [flags]   remove an installation and print the leftover manifest
  kosadm rescue    [flags]   read native resources without writing them
  kosadm status    [flags]   report the installation and its components
  kosadm help                print this text

Every install form needs the release's object definition manifests:
  --definitions <dir>        directory of the release's CRD manifests

Every install form creates the first Owner in the control-plane database:
  --identity-database <path> the database the control plane serves

The install window (C-15) is measured from the API server audit log:
  --audit-log <path>         the API server's audit log; without it the
                             installation reports the window as unmeasured
`)
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "kosadm:", err)
		code := 1
		var coded exitError
		if errors.As(err, &coded) {
			code = coded.code
		}
		os.Exit(code)
	}
}

// run dispatches one subcommand.
func run(args []string, out io.Writer) error {
	if len(args) == 0 {
		usage(os.Stderr)
		return exitError{message: "no subcommand", code: 2}
	}
	switch args[0] {
	case "install":
		return runInstall(args[1:], out)
	case "bootstrap":
		return runBootstrap(args[1:], out)
	case "uninstall":
		return runUninstall(args[1:], out)
	case "rescue":
		return runRescue(args[1:], out)
	case "status":
		return runStatus(args[1:], out)
	case "help", "-h", "--help":
		usage(out)
		return nil
	default:
		usage(os.Stderr)
		return exitError{message: fmt.Sprintf("unknown subcommand %q", args[0]), code: 2}
	}
}

// installFlags are the flags every form shares.
type installFlags struct {
	kubeconfig       string
	definitions      string
	identityDatabase string
	namespace        string
	owner            string
	applications     repeated
	image            string
	nodeAgentImage   string
	auditLog         string
	injections       string
	quietWindow      time.Duration
	settleTimeout    time.Duration
	nodeAgentTimeout time.Duration
}

// registerInstall adds the shared flags to one command.
func (f *installFlags) register(set *flag.FlagSet) {
	set.StringVar(&f.kubeconfig, "kubeconfig", "",
		"kubeconfig of the cluster the installation reconciles; empty reads KUBECONFIG and ~/.kube/config")
	set.StringVar(&f.definitions, "definitions", "",
		"directory of the release's object definition manifests (required)")
	set.StringVar(&f.identityDatabase, "identity-database", "",
		"control-plane database the first Owner is created in (required)")
	set.StringVar(&f.namespace, "namespace", install.DefaultNamespace,
		"namespace the installation creates for itself")
	set.StringVar(&f.owner, "owner", "owner", "login name of the first Owner")
	set.Var(&f.applications, "application",
		"namespace of an application installed with KOS; repeatable, must already exist")
	set.StringVar(&f.image, "image", "", "control-plane image; empty selects the pinned default")
	set.StringVar(&f.nodeAgentImage, "node-agent-image", "",
		"node-agent image; empty selects the pinned default")
	set.StringVar(&f.auditLog, "audit-log", "",
		"API server audit log; without it the install window is reported unmeasured")
	set.StringVar(&f.injections, "inject", "",
		"failure injection list, for example control-plane=1 (C-58)")
	set.DurationVar(&f.quietWindow, "quiet-window", install.DefaultQuietWindow,
		"how long no KOS subject may write after the installation settles (C-15)")
	set.DurationVar(&f.settleTimeout, "settle-timeout", install.DefaultSettleTimeout,
		"how long the installation waits for every reconciled object")
	set.DurationVar(&f.nodeAgentTimeout, "node-agent-timeout", install.DefaultNodeAgentTimeout,
		"how long the installation waits for the node agent to run on every node")
}

// repeated collects a flag that may appear more than once.
type repeated []string

// String implements flag.Value.
func (r *repeated) String() string { return strings.Join(*r, ",") }

// Set implements flag.Value.
func (r *repeated) Set(value string) error {
	*r = append(*r, value)
	return nil
}

// signalContext returns a context cancelled on SIGINT or SIGTERM, so an
// interrupted installation still runs its rollback.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
