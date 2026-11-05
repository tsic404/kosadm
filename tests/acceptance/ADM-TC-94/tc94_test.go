// Package admtc94 verifies the external-control-plane form: one cluster runs the
// control plane, the managed cluster keeps only what reconciliation needs, and
// its objects are still reconciled through the credential the installation
// wrote (ADM-AC-47, ADM-TC-94).
package admtc94

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/api/v1beta1"
	"github.com/tsic404/kosd/pkg/app"
	"github.com/tsic404/kosd/pkg/controlplane"
	"github.com/tsic404/kosd/pkg/identity"
	"github.com/tsic404/kosd/pkg/markers"
	"github.com/tsic404/kosd/pkg/reconcile"
	"github.com/tsic404/kosd/pkg/write"
)

const (
	// applicationName is the application the case installs after the run.
	applicationName = "media"
	// definitionName is the catalogue entry it installs from.
	definitionName = "media-template"
	// firstMode and secondMode are the two config values the case writes, so a
	// spec change is observable at both the projection and the workload.
	firstMode  = "first"
	secondMode = "second"
)

// mediaTemplate renders one ConfigMap from the application's private config.
const mediaTemplate = `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Name }}
  namespace: {{ .Namespace }}
data:
  mode: "{{ .Config.mode }}"
`

// TestTC94ExternalControlPlane installs the external form across two clusters,
// then installs an application and changes its spec.
func TestTC94ExternalControlPlane(t *testing.T) {
	ctx := context.Background()
	// One cluster runs the control plane; the other is an existing cluster that
	// does not carry the KOS object model yet.
	control := harness.Start(t)
	managed := harness.StartBareAudited(t)

	installerIdentity := harness.InstallerIdentity()
	control.GrantClusterAdmin(t, installerIdentity)
	managed.GrantClusterAdmin(t, installerIdentity)

	// The account database is the control plane's own (C-61).
	accounts := harness.OpenIdentity(t, nil, nil)

	controlClients, err := install.NewCluster(ctx, control.Impersonating(t, installerIdentity))
	if err != nil {
		t.Fatalf("build the control-plane cluster clients: %v", err)
	}
	installClients := managed.InstallClients(t, installerIdentity)

	runner, err := install.New(install.Config{
		NoNodeAgentController: true,
		Plan:                  managed.ExternalPlan(nil),
		Dynamic:               installClients.Dynamic,
		Kube:                  installClients.Kube,
		APIExtensions:         installClients.APIExtensions,
		Definitions:           harness.Definitions(t),
		ExternalAccounts:      accounts.Service,
		ControlPlaneCluster:   controlClients,
		Audit:                 managed.AuditSource(t),
		QuietWindow:           200 * time.Millisecond,
		SettleTimeout:         30 * time.Second,
	})
	if err != nil {
		t.Fatalf("build installer: %v", err)
	}
	result, err := runner.InstallExternal(ctx)
	if err != nil {
		t.Fatalf("install the external form: %v", err)
	}
	if !result.Completed {
		t.Fatalf("result = %+v, want a completed installation", result)
	}
	if !result.Window.Measured || !result.Window.Passed {
		t.Fatalf("install window = %+v, want a measured pass over the managed cluster", result.Window)
	}
	if result.OneTimePassword != "" {
		t.Fatal("the external form put the first Owner's password in the install output (C-68)")
	}
	wantClaim := install.DefaultAddress + identity.BootstrapOwnerPath
	if result.OwnerClaim.URL != wantClaim || result.OwnerClaim.Instruction == "" {
		t.Fatalf("install output names claim %+v, want %q with an instruction (C-68)", result.OwnerClaim, wantClaim)
	}
	assertClaimWindowOpened(t, accounts, ctx)

	// ADM-AC-47: the managed cluster runs no KOS control-plane workload ...
	assertNoControlPlaneWorkload(t, managed)
	// ... while it keeps what reconciliation needs: the object model and the
	// identity the control plane acts as.
	assertManagedClusterKeepsReconciliationInputs(t, managed, ctx)
	// The control-plane cluster runs the workload and holds the credential.
	assertControlPlaneCluster(t, control, result.Steps)

	// The credential reaches the managed cluster as the control-plane identity,
	// never as the installer that wrote it (C-04, C-14).
	credential, err := install.ManagedClients(ctx, control.Kube, install.DefaultNamespace)
	if err != nil {
		t.Fatalf("read the managed-cluster credential: %v", err)
	}
	reconciler := credential.Cluster
	caller, err := install.CallerIdentity(ctx, reconciler.Kube)
	if err != nil {
		t.Fatalf("read the credential's identity: %v", err)
	}
	if caller != harness.ControlPlaneIdentity() {
		t.Fatalf("credential acts as %q, want %q", caller, harness.ControlPlaneIdentity())
	}

	// The credential records when it stops authenticating, so a control plane
	// can say why it lost the managed cluster before it does (C-14).
	if credential.ExpiresAt.IsZero() || !credential.ExpiresAt.After(time.Now()) {
		t.Fatalf("credential records expiry %s, want an instant ahead", credential.ExpiresAt)
	}
	if state := credential.State(time.Now()); state != install.CredentialUsable || credential.Notice(time.Now()) != "" {
		t.Fatalf("credential state = %q notice %q, want usable with no notice", state, credential.Notice(time.Now()))
	}
	assertCredentialExpiry(t, control, ctx)

	// Renewal mints a new token and leaves the address alone. It must be handed
	// the cluster the credential points at, which the installation checks.
	managedAdmin, err := install.NewCluster(ctx, managed.Impersonating(t, installerIdentity))
	if err != nil {
		t.Fatalf("build the managed cluster handle: %v", err)
	}
	renewed, err := install.RenewManagedCredential(ctx, control.Kube, managedAdmin, install.DefaultNamespace)
	if err != nil {
		t.Fatalf("renew the credential: %v", err)
	}
	if renewed.Before(credential.ExpiresAt) {
		t.Fatalf("renewed expiry %s is before the recorded %s", renewed, credential.ExpiresAt)
	}
	if _, err := install.ManagedClients(ctx, control.Kube, install.DefaultNamespace); err != nil {
		t.Fatalf("the renewed credential is not usable: %v", err)
	}

	// The control plane reads and writes the managed cluster through that
	// credential: the API is served over it, and so is the reconciler.
	writer := &write.Writer{Dynamic: reconciler.Dynamic, Resources: reconciler.Resources}
	installDefinition(t, ctx, writer)
	gateway := startControlPlane(t, reconciler)
	controller := app.NewController(app.NewWriterStore(writer), app.Options{})

	createApplication(t, gateway, firstMode)
	if phase := settle(t, ctx, controller, reconciler, applicationName); phase != app.PhaseReady {
		t.Fatalf("application phase = %q, want %q", phase, app.PhaseReady)
	}
	if mode := downstreamMode(t, ctx, reconciler, applicationName); mode != firstMode {
		t.Fatalf("downstream mode = %q, want %q", mode, firstMode)
	}

	// A spec change has to reach both the projection and the downstream.
	writeMode(t, gateway, secondMode)
	if mode := projectedMode(t, gateway, applicationName); mode != secondMode {
		t.Fatalf("projection mode = %q, want %q", mode, secondMode)
	}
	settle(t, ctx, controller, reconciler, applicationName)
	if mode := downstreamMode(t, ctx, reconciler, applicationName); mode != secondMode {
		t.Fatalf("downstream mode after the spec change = %q, want %q", mode, secondMode)
	}
}

// assertClaimWindowOpened checks the successful installation opened the window
// once and left it unclaimed (C-68).
func assertClaimWindowOpened(t *testing.T, accounts *harness.Identity, ctx context.Context) {
	t.Helper()
	window, err := accounts.Store.BootstrapWindow(ctx)
	if err != nil {
		t.Fatalf("read the claim window: %v", err)
	}
	if window.OpenedAt.IsZero() || !window.ClaimedAt.IsZero() {
		t.Fatalf("claim window = %+v, want an opened, unclaimed window", window)
	}
	if length := window.Deadline.Sub(window.OpenedAt); length != identity.BootstrapWindowLength {
		t.Fatalf("claim window length = %s, want %s", length, identity.BootstrapWindowLength)
	}
	if !window.Open(accounts.Clock.Now()) {
		t.Fatalf("claim window opened at %s is already closed", window.OpenedAt)
	}
}

// assertCredentialExpiry reads the expiry the installation recorded beside the
// kubeconfig.
func assertCredentialExpiry(t *testing.T, control *harness.Cluster, ctx context.Context) {
	t.Helper()
	secret, err := control.Kube.CoreV1().Secrets(install.DefaultNamespace).Get(ctx, install.ManagedCredentialSecret, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the managed-cluster credential: %v", err)
	}
	stored, found := secret.Data[install.ManagedCredentialExpiryKey]
	if !found {
		t.Fatalf("the credential records no %s", install.ManagedCredentialExpiryKey)
	}
	expiresAt, err := time.Parse(time.RFC3339, string(stored))
	if err != nil || !expiresAt.After(time.Now()) {
		t.Fatalf("recorded expiry %q parses to %s (%v), want an instant ahead", stored, expiresAt, err)
	}
}

// assertNoControlPlaneWorkload reads the workload C-54 keeps off the managed
// cluster.
func assertNoControlPlaneWorkload(t *testing.T, managed *harness.Cluster) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := managed.Kube.AppsV1().Deployments(install.DefaultNamespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{})
	if err == nil {
		t.Fatalf("the managed cluster runs the control-plane deployment %q", install.ControlPlaneName)
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("read the control-plane deployment: %v", err)
	}
	_, err = managed.Kube.CoreV1().Services(install.DefaultNamespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{})
	if err == nil {
		t.Fatalf("the managed cluster serves the control-plane service %q", install.ControlPlaneName)
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("read the control-plane service: %v", err)
	}
}

// assertManagedClusterKeepsReconciliationInputs checks the two things the
// managed cluster has to hold: the object definitions and the identity.
func assertManagedClusterKeepsReconciliationInputs(t *testing.T, managed *harness.Cluster, ctx context.Context) {
	t.Helper()
	if _, err := managed.APIExtensions.ApiextensionsV1().CustomResourceDefinitions().Get(ctx,
		"applications."+api.Group, metav1.GetOptions{}); err != nil {
		t.Fatalf("the managed cluster serves no application definition: %v", err)
	}
	if _, err := managed.Kube.CoreV1().ServiceAccounts(install.DefaultNamespace).Get(ctx,
		install.ControlPlaneServiceAccount, metav1.GetOptions{}); err != nil {
		t.Fatalf("the managed cluster holds no control-plane identity: %v", err)
	}
	if _, err := managed.Kube.RbacV1().ClusterRoleBindings().Get(ctx,
		install.ControlPlaneName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the managed cluster grants the control-plane identity nothing: %v", err)
	}
}

// assertControlPlaneCluster checks the workload and the credential the other
// cluster holds.
func assertControlPlaneCluster(t *testing.T, control *harness.Cluster, steps []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	deployment, err := control.Kube.AppsV1().Deployments(install.DefaultNamespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the control-plane cluster runs no workload: %v", err)
	}
	assertCredentialMount(t, deployment)
	if _, err := control.Kube.CoreV1().Services(install.DefaultNamespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the control-plane cluster serves no API: %v", err)
	}
	// The workload names this identity; a cluster that lacks it rejects the
	// pod, so the control plane would never start even though the objects are
	// there.
	if _, err := control.Kube.CoreV1().ServiceAccounts(install.DefaultNamespace).Get(ctx, install.ControlPlaneServiceAccount, metav1.GetOptions{}); err != nil {
		t.Fatalf("the control-plane cluster holds no control-plane identity: %v", err)
	}
	if _, err := control.Kube.CoreV1().Secrets(install.DefaultNamespace).Get(ctx, install.ManagedCredentialSecret, metav1.GetOptions{}); err != nil {
		t.Fatalf("the control-plane cluster holds no managed-cluster credential: %v", err)
	}
	for _, step := range steps {
		if step == install.StepCredential {
			return
		}
	}
	t.Fatalf("install steps = %v, want the credential step", steps)
}

// assertCredentialMount checks that the workload reads the credential from the
// secret the installation wrote.
func assertCredentialMount(t *testing.T, deployment *appsv1.Deployment) {
	t.Helper()
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Secret != nil && volume.Secret.SecretName == install.ManagedCredentialSecret {
			return
		}
	}
	t.Fatalf("the control-plane workload mounts no %q secret", install.ManagedCredentialSecret)
}

// installDefinition publishes the catalogue entry through the cross-cluster
// credential, so the definition itself travels the same path as everything
// after it.
func installDefinition(t *testing.T, ctx context.Context, writer *write.Writer) {
	t.Helper()
	defaultMode := firstMode
	definition := harness.DefinitionContent(t, definitionName, "", mediaTemplate, nil, []v1beta1.ConfigField{
		{Name: "mode", Type: app.TypeString, Default: &defaultMode},
	})
	document, err := runtime.DefaultUnstructuredConverter.ToUnstructured(definition)
	if err != nil {
		t.Fatalf("encode definition: %v", err)
	}
	object := &unstructured.Unstructured{Object: document}
	object.SetGroupVersionKind(schema.GroupVersionKind{Group: api.Group, Version: "v1beta1", Kind: "ApplicationDefinition"})
	object.SetLabels(map[string]string{markers.ManagedBy: write.ManagedByValue})
	if _, err := writer.Apply(ctx, write.SubjectCatalogueSync, write.Session{}, write.PartSpec, object); err != nil {
		t.Fatalf("install definition: %v", err)
	}
}

// startControlPlane serves the API over the managed cluster's credential.
func startControlPlane(t *testing.T, reconciler *install.Cluster) *harness.ControlPlane {
	t.Helper()
	server, err := controlplane.New(controlplane.Config{Dynamic: reconciler.Dynamic})
	if err != nil {
		t.Fatalf("build control plane: %v", err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(func() {
		httpServer.Close()
		server.Close()
	})
	return &harness.ControlPlane{Server: server, URL: httpServer.URL}
}

// createApplication installs one application through the API (C-12).
func createApplication(t *testing.T, gateway *harness.ControlPlane, mode string) {
	t.Helper()
	manifest := harness.ApplicationManifest(applicationName, harness.Namespace, definitionName, harness.Namespace,
		map[string]any{"mode": mode}, nil)
	if status, body := gateway.Call(t, http.MethodPost, "/api/v1/resources/Application", manifest, nil); status != http.StatusAccepted {
		t.Fatalf("create application: status %d, body %v", status, body)
	}
}

// writeMode submits the user's config change through the API.
func writeMode(t *testing.T, gateway *harness.ControlPlane, next string) {
	t.Helper()
	status, stored := gateway.Call(t, http.MethodGet, "/api/v1/resources/Application/"+applicationName+"?namespace="+harness.Namespace, nil, nil)
	if status != http.StatusOK {
		t.Fatalf("read application: status %d, body %v", status, stored)
	}
	update := harness.ApplicationManifest(applicationName, harness.Namespace, definitionName, harness.Namespace,
		map[string]any{"mode": next}, nil)
	update["metadata"] = map[string]any{
		"name":            applicationName,
		"namespace":       harness.Namespace,
		"labels":          map[string]any{markers.ManagedBy: write.ManagedByValue},
		"resourceVersion": harness.String(stored, "object", "metadata", "resourceVersion"),
	}
	status, answer := gateway.Call(t, http.MethodPut,
		"/api/v1/resources/Application/"+applicationName+"?namespace="+harness.Namespace, update, nil)
	if status != http.StatusAccepted {
		t.Fatalf("write config %q: status %d, body %v", next, status, answer)
	}
}

// projectedMode reads the application's config as the projection serves it
// (C-13).
func projectedMode(t *testing.T, gateway *harness.ControlPlane, name string) string {
	t.Helper()
	status, body := gateway.Call(t, http.MethodGet, "/api/v1/resources/Application/"+name+"?namespace="+harness.Namespace, nil, nil)
	if status != http.StatusOK {
		t.Fatalf("read the projection: status %d, body %v", status, body)
	}
	return harness.String(body, "object", "spec", "config", "mode")
}

// settle runs application rounds until the application leaves the install
// sequence and returns the last phase.
func settle(t *testing.T, ctx context.Context, controller *app.Controller, reconciler *install.Cluster, name string) string {
	t.Helper()
	phase := ""
	for range 6 {
		if _, err := controller.Reconcile(ctx, reconcile.Request{
			Kind: "Application", Namespace: harness.Namespace, Name: name,
		}); err != nil {
			t.Fatalf("reconcile %s: %v", name, err)
		}
		phase = applicationPhase(t, ctx, reconciler, name)
		if phase == app.PhaseReady || phase == app.PhaseFailed {
			break
		}
	}
	return phase
}

// applicationPhase reads the stored phase from the managed cluster.
func applicationPhase(t *testing.T, ctx context.Context, reconciler *install.Cluster, name string) string {
	t.Helper()
	plural, known := reconciler.Resources[schema.GroupKind{Group: api.Group, Kind: "Application"}]
	if !known {
		t.Fatal("the credential cannot address the application class")
	}
	object, err := reconciler.Dynamic.Resource(schema.GroupVersionResource{
		Group: api.Group, Version: "v1beta1", Resource: plural,
	}).Namespace(harness.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read application %s: %v", name, err)
	}
	phase, _, err := unstructured.NestedString(object.Object, "status", "phase")
	if err != nil {
		t.Fatalf("read application %s phase: %v", name, err)
	}
	return phase
}

// downstreamMode reads the ConfigMap the application rendered in the managed
// cluster.
func downstreamMode(t *testing.T, ctx context.Context, reconciler *install.Cluster, name string) string {
	t.Helper()
	configMap, err := reconciler.Kube.CoreV1().ConfigMaps(harness.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the rendered ConfigMap: %v", err)
	}
	return configMap.Data["mode"]
}
