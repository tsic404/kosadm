// Package admtc132 verifies the namespace precondition of an existing-cluster
// installation: a declared application namespace that does not exist rejects
// the whole installation, names that namespace, and leaves neither the
// namespace nor the application behind (ADM-AC-70, ADM-TC-132).
package admtc132

import (
	"context"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/tsic404/kosd/api"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
)

// missingNamespace is the namespace the installation is told to fill without
// the cluster holding it.
const missingNamespace = "tenant-missing"

func TestTC132DeclaredNamespaceMissing(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartAudited(t)
	installerIdentity := harness.InstallerIdentity()
	cluster.GrantClusterAdmin(t, installerIdentity)
	accounts := harness.OpenIdentity(t, nil, nil)
	clients := cluster.InstallClients(t, installerIdentity)

	runner, err := install.New(install.Config{
		NoNodeAgentController: true,
		Plan: harness.InstallerPlan(func(plan *install.Plan) {
			plan.Applications = []string{missingNamespace}
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
	result, err := runner.Install(ctx)
	if err == nil {
		t.Fatal("the installation accepted an application namespace the cluster does not hold")
	}
	// ADM-AC-70: the refusal names the namespace it refused over.
	if !strings.Contains(err.Error(), missingNamespace) {
		t.Fatalf("install error = %v, want it to name %q", err, missingNamespace)
	}
	if result.StoppedAt != install.StepPreflight {
		t.Fatalf("run stopped at %q, want the preflight step", result.StoppedAt)
	}
	if _, err := cluster.Kube.CoreV1().Namespaces().Get(ctx, missingNamespace, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the installation created the declared namespace: %v", err)
	}
	if _, err := cluster.Kube.CoreV1().Namespaces().Get(ctx, install.DefaultNamespace, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the refused installation created its own namespace: %v", err)
	}

	// No application object exists, in the missing namespace or anywhere else.
	applications := cluster.Dynamic.Resource(schema.GroupVersionResource{
		Group: api.Group, Version: "v1beta1", Resource: "applications",
	})
	list, err := applications.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list applications: %v", err)
	}
	if len(list.Items) != 0 {
		t.Fatalf("the refused installation left %d application object(s)", len(list.Items))
	}
}
