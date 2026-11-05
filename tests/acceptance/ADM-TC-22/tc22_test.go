// Package admtc22 verifies that an injected mid-install failure stops at the same
// step on a repeated run and that the rollback leaves no KOS objects outside the
// whitelist, no cluster-scoped permission and no namespace (ADM-AC-10, ADM-TC-22).
package admtc22

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
	"github.com/tsic404/kosd/api"
)

const quietWindow = 200 * time.Millisecond

func TestTC22InjectedFailureRollsBack(t *testing.T) {
	cluster := harness.StartAudited(t)
	installerIdentity := harness.InstallerIdentity()
	cluster.GrantClusterAdmin(t, installerIdentity)
	identity := harness.OpenIdentity(t, nil, nil)
	clients := cluster.InstallClients(t, installerIdentity)

	// One kernel definition is missing before the window: install recreates it,
	// and the rollback must remove it again. envtest pre-installs every
	// definition, so this is the only reachable way to exercise that branch.
	if err := cluster.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().Delete(
		context.Background(), "nodedevices.kos.tsic.top", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("remove a definition before the window: %v", err)
	}
	harness.Eventually(t, 30*time.Second, "the removed definition to disappear", func() bool {
		_, err := cluster.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().Get(
			context.Background(), "nodedevices.kos.tsic.top", metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	})

	runner, err := install.New(install.Config{
		NoNodeAgentController: true,
		Plan:                  harness.InstallerPlan(nil),
		Dynamic:               clients.Dynamic,
		Kube:                  clients.Kube,
		APIExtensions:         clients.APIExtensions,
		Definitions:           harness.Definitions(t),
		Accounts:              identity.Service,
		Audit:                 cluster.AuditSource(t),
		Injections:            []install.Injection{{Point: install.StepControlPlane, Hit: 1}},
		QuietWindow:           quietWindow,
		SettleTimeout:         30 * time.Second,
	})
	if err != nil {
		t.Fatalf("build installer: %v", err)
	}

	stopped := make([]string, 0, 2)
	for attempt := 1; attempt <= 2; attempt++ {
		result, err := runner.Install(context.Background())
		if err == nil {
			t.Fatalf("attempt %d installed despite the injection list", attempt)
		}
		var injected *install.InjectedFailure
		if !errors.As(err, &injected) {
			t.Fatalf("attempt %d failed with %v, want the injected failure", attempt, err)
		}
		if result.StoppedAt != install.StepControlPlane || injected.Point != install.StepControlPlane {
			t.Fatalf("attempt %d stopped at %q (%v), want %q", attempt, result.StoppedAt, injected, install.StepControlPlane)
		}
		if !result.RolledBack {
			t.Fatalf("attempt %d reported a failed rollback", attempt)
		}
		if !result.Window.Measured || !result.Window.Passed {
			t.Fatalf("attempt %d window = %+v, want a measured pass without leftovers", attempt, result.Window)
		}
		stopped = append(stopped, result.StoppedAt)
		assertRolledBack(t, cluster)
	}
	if strings.Join(stopped, ",") != install.StepControlPlane+","+install.StepControlPlane {
		t.Fatalf("stop positions = %v, want the same step twice", stopped)
	}
}

// assertRolledBack checks the cluster holds none of the installation's objects.
func assertRolledBack(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	ctx := context.Background()
	if _, err := cluster.Kube.CoreV1().Namespaces().Get(ctx, aiNamespace(), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the install namespace still exists: %v", err)
	}
	if _, err := cluster.Kube.RbacV1().ClusterRoleBindings().Get(ctx, controlPlaneName(), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the cluster role binding survived the rollback: %v", err)
	}
	if _, err := cluster.Kube.RbacV1().ClusterRoles().Get(ctx, controlPlaneName(), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the cluster role survived the rollback: %v", err)
	}
	if _, err := cluster.Kube.CoreV1().ServiceAccounts(aiNamespace()).Get(ctx, controlPlaneName(), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the control-plane service account survived the rollback: %v", err)
	}
	if _, err := cluster.Kube.AppsV1().Deployments(aiNamespace()).Get(ctx, controlPlaneName(), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the control-plane deployment survived the rollback: %v", err)
	}
	if _, err := cluster.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, "nodedevices.kos.tsic.top", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the definition this run created survived the rollback: %v", err)
	}
	// The node agent's cluster-scoped pair is removed with everything else; the
	// namespace-scoped DaemonSet goes with the namespace, but a run that left
	// the namespace behind would still show it.
	if _, err := cluster.Kube.RbacV1().ClusterRoles().Get(ctx, install.NodeAgentName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the node-agent cluster role survived the rollback: %v", err)
	}
	if _, err := cluster.Kube.RbacV1().ClusterRoleBindings().Get(ctx, install.NodeAgentName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the node-agent cluster role binding survived the rollback: %v", err)
	}
	if _, err := cluster.Kube.AppsV1().DaemonSets(aiNamespace()).Get(ctx, install.NodeAgentName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the node agent survived the rollback: %v", err)
	}
	// The application namespace survives the rollback, so its authorization
	// objects are checked separately: the install created them, the rollback
	// must remove them.
	if _, err := cluster.Kube.RbacV1().Roles(harness.Namespace).Get(ctx, controlPlaneName(), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the role in the application namespace survived the rollback: %v", err)
	}
	if _, err := cluster.Kube.RbacV1().RoleBindings(harness.Namespace).Get(ctx, controlPlaneName(), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the role binding in the application namespace survived the rollback: %v", err)
	}
}

// aiNamespace is the installation's namespace; the string comes from the kernel
// constant so a change of the default is caught here.
func aiNamespace() string { return api.SystemNamespace }

// controlPlaneName is the rendered workload name.
func controlPlaneName() string { return install.ControlPlaneName }
