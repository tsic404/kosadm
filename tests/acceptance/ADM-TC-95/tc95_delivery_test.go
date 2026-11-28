package admtc95

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
)

// otherNode is a second node of the cluster that holds no database. An upgrade
// that names it without naming the database has no file for it: the running
// control plane serves the file on its own node, and this command cannot show
// that file lives on otherNode.
const otherNode = "node-b"

// TestTC95PlatformUpgradeChecksTheDeliveryItRenders drives the real
// `kosadm upgrade platform` entry against a running installation whose control
// plane is pinned to the node holding its database. Every case names its
// inputs the way an operator can and judges what the upgrade writes, not what a
// helper would have computed: the delivery the replacement renders is the one
// that has to be checked, because the flags and the served workload differ
// exactly where the pin is inherited (ADM-C-54, ADM-C-55, ADM-TC-95).
func TestTC95PlatformUpgradeChecksTheDeliveryItRenders(t *testing.T) {
	ctx := context.Background()
	cluster := harness.Start(t)
	installerIdentity := harness.InstallerIdentity()
	cluster.GrantClusterAdmin(t, installerIdentity)
	accounts := harness.OpenIdentity(t, nil, nil)
	cluster.RegisterControlPlaneNode(t)
	database, err := install.SeededDatabase(accounts.DatabasePath, harness.ControlPlaneNode)
	if err != nil {
		t.Fatalf("resolve the database the installation seeds: %v", err)
	}
	clients := cluster.InstallClients(t, installerIdentity)
	runner, err := install.New(install.Config{
		NoNodeAgentController: true,
		Plan: harness.InstallerPlan(func(plan *install.Plan) {
			plan.Database = &database
		}),
		Dynamic:       clients.Dynamic,
		Kube:          clients.Kube,
		APIExtensions: clients.APIExtensions,
		Definitions:   harness.Definitions(t),
		Accounts:      accounts.Service,
		QuietWindow:   200 * time.Millisecond,
		SettleTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("build installer: %v", err)
	}
	installed, err := runner.Install(ctx)
	if err != nil {
		t.Fatalf("install: %v", err)
	}
	if !installed.Completed || installed.OneTimePassword == "" {
		t.Fatalf("the installation completed=%t and printed password=%q, want a completed run with its credential",
			installed.Completed, installed.OneTimePassword)
	}
	cluster.CreateNode(t, otherNode, harness.DefaultKubeletVersion)

	binary := buildManagementCLI(t)
	kubeconfig := writeKubeconfig(t, cluster)
	definitions := harness.CRDDirectory(t)
	platform := []string{
		"upgrade", "platform",
		"--kubeconfig", kubeconfig,
		"--definitions", definitions,
		"--installed-framework", "1.0",
	}

	// No inputs at all: the upgrade names a release, not the installation's
	// database. It has to keep the delivery the workload serves and check that
	// delivery's node, so the account the installation printed still signs in.
	t.Run("keeps the served delivery when it names no node", func(t *testing.T) {
		output, err := exec.Command(binary, platform...).CombinedOutput()
		if err != nil {
			t.Fatalf("platform-only upgrade = %v\n%s", err, output)
		}
		assertServesSeededDatabase(t, cluster, database)
		controlPlane := harness.OpenIdentityAt(t, database.Path, nil, nil)
		if _, err := controlPlane.Service.Login(ctx, "owner", installed.OneTimePassword); err != nil {
			t.Fatalf("sign the first Owner in after the platform-only upgrade: %v", err)
		}
	})

	// A node named without the database: the workload keeps serving the file on
	// its own node, so the named one is not the node the replacement would read.
	// Checking the named node and then rendering the served one is how a pod
	// ends up on a node that has no database at all (ADM-TC-95).
	t.Run("refuses a node named without the database", func(t *testing.T) {
		output, err := exec.Command(binary, append(platform, "--control-plane-node", otherNode)...).CombinedOutput()
		assertRefused(t, err, output, 2, otherNode, "--identity-database")
		assertServesSeededDatabase(t, cluster, database)
	})

	// The same path on two machines is two different files, so naming the served
	// node does not turn the file this command opened into the one the control
	// plane reads. The maintenance records would land in this host's copy while
	// the replacement serves the served node's, and the Owner this host's copy
	// holds would not be the account the control plane answers to. The run
	// refuses before it replaces the workload (ADM-C-54, ADM-C-56).
	t.Run("refuses this host's copy named with the served node", func(t *testing.T) {
		output, err := exec.Command(binary, append(platform,
			"--identity-database", database.Path,
			"--control-plane-node", harness.ControlPlaneNode,
		)...).CombinedOutput()
		assertRefused(t, err, output, 2, database.Path, harness.HostShortName(t))
		assertServesSeededDatabase(t, cluster, database)
	})

	// The node that holds the file is gone. Naming a different node must not
	// pass just because that node exists: the replacement would be pinned to the
	// absent one, the failure the check exists to prevent.
	if err := cluster.Kube.CoreV1().Nodes().Delete(ctx, harness.ControlPlaneNode, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("remove the node that holds the database: %v", err)
	}
	t.Run("refuses a node named after the database node is gone", func(t *testing.T) {
		output, err := exec.Command(binary, append(platform, "--control-plane-node", otherNode)...).CombinedOutput()
		assertRefused(t, err, output, 2, otherNode, "--identity-database")
		assertServesSeededDatabase(t, cluster, database)
	})

	// The inherited delivery's node is gone and the operator names nothing: the
	// upgrade has no node it can show holds the file, so it stops before writing
	// a workload nothing can schedule.
	t.Run("refuses the served delivery whose node is gone", func(t *testing.T) {
		output, err := exec.Command(binary, platform...).CombinedOutput()
		assertRefused(t, err, output, 2, harness.ControlPlaneNode)
		assertServesSeededDatabase(t, cluster, database)
	})
}

// TestTC95PlatformUpgradeRefusesADatabaseOnAnotherNode covers the upgrade run
// from the machine that holds a copy of the database while the running control
// plane serves the file at the same path on another node. The same absolute
// path is not the same file across machines, so the upgrade stops before it
// rewrites the workload, whether it infers this host's node or is told it.
func TestTC95PlatformUpgradeRefusesADatabaseOnAnotherNode(t *testing.T) {
	ctx := context.Background()
	cluster := harness.Start(t)
	installerIdentity := harness.InstallerIdentity()
	cluster.GrantClusterAdmin(t, installerIdentity)
	accounts := harness.OpenIdentity(t, nil, nil)
	cluster.RegisterControlPlaneNode(t)
	database, err := install.SeededDatabase(accounts.DatabasePath, harness.ControlPlaneNode)
	if err != nil {
		t.Fatalf("resolve the database the installation seeds: %v", err)
	}
	clients := cluster.InstallClients(t, installerIdentity)
	runner, err := install.New(install.Config{
		NoNodeAgentController: true,
		Plan: harness.InstallerPlan(func(plan *install.Plan) {
			plan.Database = &database
		}),
		Dynamic:       clients.Dynamic,
		Kube:          clients.Kube,
		APIExtensions: clients.APIExtensions,
		Definitions:   harness.Definitions(t),
		Accounts:      accounts.Service,
		QuietWindow:   200 * time.Millisecond,
		SettleTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("build installer: %v", err)
	}
	installed, err := runner.Install(ctx)
	if err != nil {
		t.Fatalf("install: %v", err)
	}
	if !installed.Completed {
		t.Fatal("the installation did not complete")
	}
	// This machine joins the cluster under its own name, and it is not the node
	// the running control plane serves: the file at the same path here is this
	// host's copy, not the database the workload reads.
	host := harness.HostShortName(t)
	cluster.RegisterNode(t, host, host)

	binary := buildManagementCLI(t)
	platform := []string{
		"upgrade", "platform",
		"--kubeconfig", writeKubeconfig(t, cluster),
		"--definitions", harness.CRDDirectory(t),
		"--installed-framework", "1.0",
	}

	t.Run("infers this host and refuses", func(t *testing.T) {
		output, err := exec.Command(binary, append(platform, "--identity-database", database.Path)...).CombinedOutput()
		assertRefused(t, err, output, 2, database.Path, harness.ControlPlaneNode)
		assertServesSeededDatabase(t, cluster, database)
	})

	t.Run("names this host explicitly and refuses", func(t *testing.T) {
		output, err := exec.Command(binary, append(platform,
			"--identity-database", database.Path,
			"--control-plane-node", host,
		)...).CombinedOutput()
		assertRefused(t, err, output, 2, database.Path, harness.ControlPlaneNode)
		assertServesSeededDatabase(t, cluster, database)
	})

	// The form the help text used to invite: a node-maintenance upgrade run from
	// B that names the served node A for the control plane. The node on the
	// command line says where the replacement should run, not where the file this
	// process opened came from, so the run would audit B's copy while the control
	// plane serves A's. It refuses before the object replacement and before any
	// maintenance record, and B's copy keeps no record of the attempt
	// (ADM-C-54, ADM-C-56).
	t.Run("node upgrade naming the served node refuses before maintenance", func(t *testing.T) {
		output, err := exec.Command(binary, append(platform,
			"--identity-database", database.Path,
			"--control-plane-node", harness.ControlPlaneNode,
			"--node", host,
			"--version", "v1.0.0",
		)...).CombinedOutput()
		assertRefused(t, err, output, 2, database.Path, harness.ControlPlaneNode, host)
		assertServesSeededDatabase(t, cluster, database)
		assertNoMaintenanceRecordIn(t, database.Path, host)
	})

	t.Run("node upgrade form refuses before maintenance", func(t *testing.T) {
		output, err := exec.Command(binary, append(platform,
			"--identity-database", database.Path,
			"--node", host,
			"--version", "v1.0.0",
		)...).CombinedOutput()
		assertRefused(t, err, output, 2, database.Path, harness.ControlPlaneNode)
		assertServesSeededDatabase(t, cluster, database)
	})
}

// assertRefused checks the upgrade stopped with the exit code and named what
// the operator has to change.
func assertRefused(t *testing.T, err error, output []byte, code int, wants ...string) {
	t.Helper()
	var refused *exec.ExitError
	if !errors.As(err, &refused) || refused.ExitCode() != code {
		t.Fatalf("upgrade = %v\n%s, want exit code %d", err, output, code)
	}
	for _, want := range wants {
		if !strings.Contains(string(output), want) {
			t.Fatalf("refusal %q does not name %q", output, want)
		}
	}
}

// assertServesSeededDatabase checks the workload still serves the installation's
// file on the node that holds it, and still mounts the package directory: a
// refused upgrade writes nothing, and an accepted one keeps both deliveries.
func assertServesSeededDatabase(t *testing.T, cluster *harness.Cluster, delivery install.DatabaseDelivery) {
	t.Helper()
	deployment, err := cluster.Kube.AppsV1().Deployments(install.DefaultNamespace).Get(
		context.Background(), install.ControlPlaneName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the control plane: %v", err)
	}
	harness.AssertDatabaseDelivery(t, deployment, delivery)
	harness.AssertPackageDirectory(t, deployment)
}
