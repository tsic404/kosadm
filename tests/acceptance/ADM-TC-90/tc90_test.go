// Package admtc90 verifies the bootstrap form: the declared node count comes up,
// the control plane answers, an application reaches Ready, and the one-time
// password the install prints signs the first Owner in and must be replaced
// without disturbing another account's session (ADM-AC-45, ADM-TC-90).
package admtc90

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
	"github.com/tsic404/kosd/api/v1beta1"
	"github.com/tsic404/kosd/pkg/identity"
	"github.com/tsic404/kosd/pkg/write"
)

// provisioner registers the declared nodes, the way the run kernel does on a
// bare host, and marks the API server reachable.
type provisioner struct {
	kube kubernetes.Interface
	done *bool
}

// Provision creates the declared node objects.
func (p *provisioner) Provision(ctx context.Context, nodes int) error {
	for index := range nodes {
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d", index)}}
		if _, err := p.kube.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
			return err
		}
	}
	*p.done = true
	return nil
}

// gatedSnapshotter refuses a snapshot until the run kernel is up, which is what
// a bare host looks like: the install window cannot open before provisioning.
type gatedSnapshotter struct {
	inner       install.SnapshotSource
	provisioned *bool
}

// Take reads the snapshot once the run kernel exists.
func (g gatedSnapshotter) Take(ctx context.Context) (*install.Snapshot, error) {
	if !*g.provisioned {
		return nil, fmt.Errorf("the run kernel is not up yet")
	}
	return g.inner.Take(ctx)
}

// nodes reports the nodes the run kernel registered.
type nodes struct {
	kube kubernetes.Interface
}

// ReadyNodes lists the registered nodes.
func (n nodes) ReadyNodes(ctx context.Context) ([]string, error) {
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

func TestTC90BootstrapInstall(t *testing.T) {
	cluster := harness.Start(t)
	installerIdentity := harness.InstallerIdentity()
	cluster.GrantClusterAdmin(t, installerIdentity)
	identityStore := harness.OpenIdentity(t, nil, nil)
	clients := cluster.InstallClients(t, installerIdentity)
	controlPlane := cluster.StartControlPlane(t, nil)

	provisioned := false
	runner, err := install.New(install.Config{
		NoNodeAgentController: true,
		Plan:                  harness.BootstrapPlan(3, nil),
		Dynamic:               clients.Dynamic,
		Kube:                  clients.Kube,
		APIExtensions:         clients.APIExtensions,
		Definitions:           harness.Definitions(t),
		Accounts:              identityStore.Service,
		Provisioner:           &provisioner{kube: clients.Kube, done: &provisioned},
		Nodes:                 nodes{kube: clients.Kube},
		Snapshotter: gatedSnapshotter{
			inner:       &install.Snapshotter{Dynamic: cluster.Dynamic, Discovery: cluster.Kube.Discovery()},
			provisioned: &provisioned,
		},
		Ready: func(ctx context.Context) error {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, controlPlane.URL+"/", nil)
			if err != nil {
				return err
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				return err
			}
			response.Body.Close()
			return nil
		},
		QuietWindow:   200 * time.Millisecond,
		SettleTimeout: 30 * time.Second,
		NodeTimeout:   30 * time.Second,
	})
	if err != nil {
		t.Fatalf("build installer: %v", err)
	}
	result, err := runner.Bootstrap(context.Background())
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if result.Nodes != 3 {
		t.Fatalf("declared nodes = %d, want 3", result.Nodes)
	}
	if !result.ControlPlaneReady {
		t.Fatal("the bootstrap finished without a ready control plane")
	}
	if result.OneTimePassword == "" {
		t.Fatal("the install printed no one-time password")
	}
	commitment := runner.Plan().Commitment()
	if commitment.Replicas != 1 || commitment.AutomaticMigration {
		t.Fatalf("commitment = %+v, want one replica and no migration", commitment)
	}
	if !strings.Contains(commitment.Notice, "3 个节点") {
		t.Fatalf("notice = %q, want it to name the declared node count", commitment.Notice)
	}

	// An application installed on this cluster reaches Ready.
	application := harness.Application("shop", nil)
	status := harness.ObjectStatus()
	status["phase"] = "Ready"
	application.Object["status"] = status
	cluster.Create(t, application)
	stored := cluster.Get(t, v1beta1.GroupVersion.Version, "Application", "shop")
	phase, _, _ := unstructured.NestedString(stored.Object, "status", "phase")
	if phase != "Ready" {
		t.Fatalf("application phase = %q, want Ready", phase)
	}

	// The printed password signs the first Owner in and must be replaced.
	login, err := identityStore.Service.Login(context.Background(), "owner", result.OneTimePassword)
	if err != nil {
		t.Fatalf("sign the first Owner in with the printed password: %v", err)
	}
	if !login.Session.MustChangePassword {
		t.Fatal("the first Owner was not required to change the one-time password")
	}
	secondOwnerLogin, err := identityStore.Service.Login(context.Background(), "owner", result.OneTimePassword)
	if err != nil {
		t.Fatalf("open a second Owner session: %v", err)
	}
	bob, err := identityStore.Service.CreateAccount(context.Background(), login.Session, identity.NewAccount{
		Username: "bob", Role: write.RoleMember,
	})
	if err != nil {
		t.Fatalf("create the second account: %v", err)
	}
	bobLogin, err := identityStore.Service.Login(context.Background(), "bob", bob.OneTimePassword)
	if err != nil {
		t.Fatalf("sign the second account in: %v", err)
	}
	if err := identityStore.Service.ChangePassword(context.Background(), login.Token, result.OneTimePassword, "owner-password"); err != nil {
		t.Fatalf("replace the one-time password: %v", err)
	}
	if _, err := identityStore.Service.Authenticate(context.Background(), login.Token); err != nil {
		t.Fatalf("the session that changed the password was revoked: %v", err)
	}
	if _, err := identityStore.Service.Authenticate(context.Background(), secondOwnerLogin.Token); err == nil {
		t.Fatal("another session of the account survived the password change")
	}
	if _, err := identityStore.Service.Authenticate(context.Background(), bobLogin.Token); err != nil {
		t.Fatalf("another account's session was disturbed by the password change: %v", err)
	}
}
