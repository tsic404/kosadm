package admtc95

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
)

// TestTC95UpgradeKeepsTheServedDatabase runs a real installation and a real
// upgrade that repeats none of the installation's inputs, and reads what the
// rewritten workload ended up serving.
//
// An upgrade names a release, not the installation's database and node. If the
// rewrite rendered no delivery, the replacement pod would open the image's own
// empty database: the file the installation seeded stays on the node, unread,
// and the account it printed stops existing (C-54, C-63). This case therefore
// performs both writes and inspects the workload the second one landed, instead
// of rendering a plan the case built itself.
func TestTC95UpgradeKeepsTheServedDatabase(t *testing.T) {
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

	// The installation seeds the database and pins the workload to the node that
	// holds it, exactly as an operator's install run does.
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
		Audit:         cluster.AuditSource(t),
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
		t.Fatalf("the installation stopped at %q", installed.StoppedAt)
	}
	if installed.OneTimePassword == "" {
		t.Fatal("the installation printed no first-Owner credential")
	}

	// The upgrade repeats none of those inputs: it names the release alone.
	served, found, err := install.ServedDatabase(ctx, cluster.Kube, install.DefaultNamespace)
	if err != nil {
		t.Fatalf("read the served database: %v", err)
	}
	if !found {
		t.Fatal("the installation left no database delivery on the workload")
	}
	plan := install.InheritDatabaseDelivery(install.Plan{
		Mode:      install.ModeExisting,
		Namespace: install.DefaultNamespace,
		Owner:     "owner",
	}, served, found)
	if _, err := install.Upgrade(ctx, install.UpgradeConfig{
		Plan:          plan,
		APIExtensions: clients.APIExtensions,
		Definitions:   harness.Definitions(t),
		Dynamic:       clients.Dynamic,
		Poll:          20 * time.Millisecond,
		Wait:          60 * time.Second,
	}); err != nil {
		t.Fatalf("upgrade: %v", err)
	}

	// The workload the upgrade wrote still serves the file the installation
	// seeded, on the same node, as the same account.
	deployment, err := cluster.Kube.AppsV1().Deployments(install.DefaultNamespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the upgraded workload: %v", err)
	}
	harness.AssertDatabaseDelivery(t, deployment, database)
	harness.AssertPackageDirectory(t, deployment)

	// The account the installation printed still signs in against the file the
	// upgraded workload serves: that is what "the accounts survived" means.
	controlPlane := harness.OpenIdentityAt(t, database.Path, nil, nil)
	login, err := controlPlane.Service.Login(ctx, installed.Owner, installed.OneTimePassword)
	if err != nil {
		t.Fatalf("sign the first Owner in after the upgrade: %v", err)
	}
	if !login.Session.MustChangePassword {
		t.Fatal("the first login after the upgrade was not required to change the one-time password")
	}
}
