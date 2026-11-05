// Package admtc131 verifies the completed state of an existing-cluster
// installation: the KOS namespace and object definitions exist, the control
// plane reads and writes the cluster, an application reaches Ready, and the
// credential the install printed signs the first Owner in and must be replaced
// (ADM-AC-70, ADM-TC-131).
package admtc131

import (
	"context"
	"net/http"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/pkg/app"
	"github.com/tsic404/kosd/pkg/identity"
	"github.com/tsic404/kosd/pkg/write"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
)

const (
	// applicationName is the application the case installs after the run.
	applicationName = "notebook"
	// definitionName is the catalogue entry it installs from.
	definitionName = "notebook-template"
)

func TestTC131ExistingClusterInstallCompletes(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartAudited(t)
	installerIdentity := harness.InstallerIdentity()
	cluster.GrantClusterAdmin(t, installerIdentity)
	accounts := harness.OpenIdentity(t, nil, nil)
	clients := cluster.InstallClients(t, installerIdentity)

	runner, err := install.New(install.Config{
		NoNodeAgentController: true,
		Plan:                  harness.InstallerPlan(nil),
		Dynamic:               clients.Dynamic,
		Kube:                  clients.Kube,
		APIExtensions:         clients.APIExtensions,
		Definitions:           harness.Definitions(t),
		Accounts:              accounts.Service,
		Audit:                 cluster.AuditSource(t),
		QuietWindow:           200 * time.Millisecond,
		SettleTimeout:         30 * time.Second,
	})
	if err != nil {
		t.Fatalf("build installer: %v", err)
	}
	result, err := runner.Install(ctx)
	if err != nil {
		t.Fatalf("install: %v", err)
	}
	if !result.Completed || result.StoppedAt != "" || !result.Window.Measured || !result.Window.Passed {
		t.Fatalf("run completed=%t stopped at %q with window %+v, want a complete measured run",
			result.Completed, result.StoppedAt, result.Window)
	}

	// ADM-AC-70: the installation's own objects exist.
	if _, err := cluster.Kube.CoreV1().Namespaces().Get(ctx, install.DefaultNamespace, metav1.GetOptions{}); err != nil {
		t.Fatalf("the installation left no namespace: %v", err)
	}
	if _, err := cluster.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().Get(ctx,
		"applications."+api.Group, metav1.GetOptions{}); err != nil {
		t.Fatalf("the installation left no object definitions: %v", err)
	}

	// ADM-C-54: the installation delivers the three artifacts — the control
	// plane, the per-node agent and the management CLI.
	assertThreeArtifacts(t, cluster)

	// The control plane reads and writes the cluster: the application is
	// created and read back through its API (C-12), then reconciled to Ready.
	cluster.InstallDefinition(t, harness.Definition(t, definitionName, nil))
	gateway := cluster.StartControlPlane(t, nil)
	manifest := harness.ApplicationManifest(applicationName, harness.Namespace, definitionName, harness.Namespace, nil, nil)
	if status, body := gateway.Call(t, http.MethodPost, "/api/v1/resources/Application", manifest, nil); status != http.StatusAccepted {
		t.Fatalf("create the application: status %d, body %v", status, body)
	}
	if status, body := gateway.Call(t, http.MethodGet,
		"/api/v1/resources/Application/"+applicationName+"?namespace="+harness.Namespace, nil, nil); status != http.StatusOK {
		t.Fatalf("read the application back: status %d, body %v", status, body)
	}
	controller := cluster.ApplicationController(t, app.Options{})
	if phase := cluster.SettleApplication(t, controller, applicationName, 8); phase != app.PhaseReady {
		t.Fatalf("application phase = %q, want %q", phase, app.PhaseReady)
	}
	if _, err := cluster.Kube.CoreV1().ConfigMaps(harness.Namespace).Get(ctx, applicationName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the application rendered no downstream object: %v", err)
	}

	// ADM-AC-70: the printed credential signs the first Owner in and the first
	// login must replace it.
	if result.Owner == "" || result.OneTimePassword == "" {
		t.Fatalf("install output = owner %q password %q, want the first Owner's credential",
			result.Owner, result.OneTimePassword)
	}
	login, err := accounts.Service.Login(ctx, result.Owner, result.OneTimePassword)
	if err != nil {
		t.Fatalf("sign the first Owner in with the printed password: %v", err)
	}
	if !login.Session.MustChangePassword {
		t.Fatal("the first login was not required to change the one-time password")
	}
	otherSession, err := accounts.Service.Login(ctx, result.Owner, result.OneTimePassword)
	if err != nil {
		t.Fatalf("open another session of the first Owner: %v", err)
	}
	bob, err := accounts.Service.CreateAccount(ctx, login.Session, identity.NewAccount{
		Username: "bob", Role: write.RoleMember,
	})
	if err != nil {
		t.Fatalf("create another account: %v", err)
	}
	bobLogin, err := accounts.Service.Login(ctx, "bob", bob.OneTimePassword)
	if err != nil {
		t.Fatalf("sign the other account in: %v", err)
	}
	if err := accounts.Service.ChangePassword(ctx, login.Token, result.OneTimePassword, "owner-password"); err != nil {
		t.Fatalf("replace the one-time password: %v", err)
	}
	if _, err := accounts.Service.Authenticate(ctx, login.Token); err != nil {
		t.Fatalf("the session that changed the password was revoked: %v", err)
	}
	if _, err := accounts.Service.Authenticate(ctx, otherSession.Token); err == nil {
		t.Fatal("another session of the account survived the password change")
	}
	if _, err := accounts.Service.Authenticate(ctx, bobLogin.Token); err != nil {
		t.Fatalf("another account's session was disturbed by the password change: %v", err)
	}
}

// assertThreeArtifacts checks the artifacts an installation delivers (C-54):
// the control-plane workload, one node agent per node, and the management CLI.
// The node agent is a DaemonSet, which is what makes it one instance per node.
func assertThreeArtifacts(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := cluster.Kube.AppsV1().Deployments(install.DefaultNamespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the installation delivered no control plane: %v", err)
	}
	agent, err := cluster.Kube.AppsV1().DaemonSets(install.DefaultNamespace).Get(ctx, install.NodeAgentName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the installation delivered no node agent: %v", err)
	}
	if agent.Spec.Template.Spec.ServiceAccountName != install.NodeAgentServiceAccount {
		t.Fatalf("the node agent runs as %q, want %q", agent.Spec.Template.Spec.ServiceAccountName, install.NodeAgentServiceAccount)
	}
	if len(agent.Spec.Template.Spec.Containers) != 1 || agent.Spec.Template.Spec.Containers[0].Image != install.DefaultNodeAgentImage {
		t.Fatalf("the node agent runs %v, want %s", agent.Spec.Template.Spec.Containers, install.DefaultNodeAgentImage)
	}
	// The management CLI is the artifact this repository builds; a build that
	// does not run would leave the third artifact missing.
	binary := filepath.Join(t.TempDir(), "kosadm")
	build := exec.Command("go", "build", "-o", binary, "./cmd/kosadm")
	build.Dir = harness.RepoRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the management CLI: %v\n%s", err, output)
	}
	if output, err := exec.Command(binary, "help").CombinedOutput(); err != nil {
		t.Fatalf("run the management CLI: %v\n%s", err, output)
	}
}
