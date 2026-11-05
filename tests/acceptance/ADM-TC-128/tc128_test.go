// Package admtc128 verifies the first-Owner claim of the external-control-plane
// form: the install output names the claim endpoint instead of a password, a
// source other than the control-plane host is refused, the first local claim
// returns the password once, and the password appears in no surface the
// installation can reach (ADM-AC-63, ADM-TC-128).
package admtc128

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/kosd/pkg/controlplane"
	"github.com/tsic404/kosd/pkg/identity"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
)

func TestTC128FirstOwnerClaimDeliversOnce(t *testing.T) {
	ctx := context.Background()
	control := harness.Start(t)
	managed := harness.StartBareAudited(t)
	installerIdentity := harness.InstallerIdentity()
	control.GrantClusterAdmin(t, installerIdentity)
	managed.GrantClusterAdmin(t, installerIdentity)
	accounts := harness.OpenIdentity(t, nil, nil)
	// Every interface serves the claim route, so a request from another host
	// reaches the same endpoint the control-plane host uses (C-68).
	remoteURL := accounts.ServeEverywhere(t)

	runner := harness.ExternalInstaller(t, ctx, managed, control, accounts, nil)
	result, err := runner.InstallExternal(ctx)
	if err != nil {
		t.Fatalf("install the external form: %v", err)
	}

	// The install output names the claim endpoint and its instruction, never a
	// password (C-68).
	if result.OneTimePassword != "" {
		t.Fatal("the external form put the first Owner's password in the install output")
	}
	wantClaim := install.DefaultAddress + identity.BootstrapOwnerPath
	if result.OwnerClaim.URL != wantClaim || result.OwnerClaim.Instruction == "" {
		t.Fatalf("install output names claim %+v, want %q with an instruction", result.OwnerClaim, wantClaim)
	}

	// Only the login and the claim route are served without a session (C-68).
	if status, body := accounts.HTTP.Call(t, http.MethodGet, "/api/v1/accounts", nil, nil); status != http.StatusUnauthorized {
		t.Fatalf("a session-less account request = %d %v, want 401", status, body)
	}

	// A request from the managed-cluster side is refused and leaves the window
	// open.
	remote := &harness.HTTP{URL: remoteURL}
	status, body := remote.Call(t, http.MethodPost, identity.BootstrapOwnerPath, nil, nil)
	if status != http.StatusForbidden {
		t.Fatalf("claim from another host = %d %v, want 403", status, body)
	}
	if carriesPassword(body) {
		t.Fatalf("the refused claim carried a password: %v", body)
	}
	window, err := accounts.Store.BootstrapWindow(ctx)
	if err != nil {
		t.Fatalf("read the claim window: %v", err)
	}
	if !window.ClaimedAt.IsZero() || !window.Open(accounts.Clock.Now()) {
		t.Fatalf("a refused claim changed the window: %+v", window)
	}

	// The control-plane host claims the password once.
	status, body = accounts.HTTP.Call(t, http.MethodPost, identity.BootstrapOwnerPath, nil, nil)
	if status != http.StatusOK {
		t.Fatalf("claim from the control-plane host = %d %v, want 200", status, body)
	}
	password, _ := body["oneTimePassword"].(string)
	if body["username"] != "owner" || password == "" {
		t.Fatalf("claim answered %v, want the first Owner and its password", body)
	}

	// The password reached no surface the installation can write: the install
	// output, the audit trail, the cluster objects and the control-plane
	// database. Backups and diagnostic bundles are exports of that database,
	// so these four are every place it could be persisted.
	assertNoPassword(t, ctx, password, result, accounts, managed, control)

	// The same installation is never served twice.
	status, body = accounts.HTTP.Call(t, http.MethodPost, identity.BootstrapOwnerPath, nil, nil)
	if status != http.StatusForbidden {
		t.Fatalf("second claim = %d %v, want 403", status, body)
	}
	if carriesPassword(body) {
		t.Fatalf("the refused second claim carried a password: %v", body)
	}
	if code := errorCode(body); code != controlplane.CodeBootstrapClosed {
		t.Fatalf("second claim code = %q, want %q", code, controlplane.CodeBootstrapClosed)
	}

	// The delivered password signs the first Owner in and must be replaced.
	login, err := accounts.Service.Login(ctx, "owner", password)
	if err != nil {
		t.Fatalf("sign the first Owner in with the claimed password: %v", err)
	}
	if !login.Session.MustChangePassword {
		t.Fatal("the first login was not required to change the one-time password")
	}
}

// carriesPassword reports whether a refusal body carries a password field.
func carriesPassword(body map[string]any) bool {
	if _, found := body["oneTimePassword"]; found {
		return true
	}
	failure, _ := body["error"].(map[string]any)
	_, found := failure["oneTimePassword"]
	return found
}

// errorCode reads the stable code of a failure envelope.
func errorCode(body map[string]any) string {
	failure, _ := body["error"].(map[string]any)
	code, _ := failure["code"].(string)
	return code
}

// assertNoPassword reads each surface the password could have reached.
func assertNoPassword(t *testing.T, ctx context.Context, password string, result install.Result, accounts *harness.Identity, clusters ...*harness.Cluster) {
	t.Helper()
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("encode the install output: %v", err)
	}
	if bytes.Contains(encoded, []byte(password)) {
		t.Fatal("the install output carries the password")
	}
	entries, err := accounts.Store.AuditEntries(ctx, 50)
	if err != nil {
		t.Fatalf("read the audit trail: %v", err)
	}
	encoded, err = json.Marshal(entries)
	if err != nil {
		t.Fatalf("encode the audit trail: %v", err)
	}
	if bytes.Contains(encoded, []byte(password)) {
		t.Fatal("the audit trail carries the password")
	}
	// SQLite may keep a fresh write in the write-ahead log, so the database is
	// every file the store holds, not only the main one.
	for _, suffix := range []string{"", "-wal", "-shm"} {
		stored, err := os.ReadFile(accounts.DatabasePath + suffix)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatalf("read the control-plane database%s: %v", suffix, err)
		}
		if bytes.Contains(stored, []byte(password)) {
			t.Fatalf("the control-plane database%s carries the password in clear", suffix)
		}
	}
	for _, cluster := range clusters {
		assertClusterHoldsNoPassword(t, ctx, password, cluster)
	}
}

// assertClusterHoldsNoPassword reads the object kinds an installation writes.
func assertClusterHoldsNoPassword(t *testing.T, ctx context.Context, password string, cluster *harness.Cluster) {
	t.Helper()
	secrets, err := cluster.Kube.CoreV1().Secrets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list secrets: %v", err)
	}
	for _, secret := range secrets.Items {
		if secretCarries(&secret, password) {
			t.Fatalf("secret %s/%s carries the password", secret.Namespace, secret.Name)
		}
	}
	configMaps, err := cluster.Kube.CoreV1().ConfigMaps("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list config maps: %v", err)
	}
	for _, configMap := range configMaps.Items {
		for key, value := range configMap.Data {
			if bytes.Contains([]byte(value), []byte(password)) {
				t.Fatalf("config map %s/%s key %s carries the password", configMap.Namespace, configMap.Name, key)
			}
		}
	}
}

// secretCarries reports whether any value of a secret is the password.
func secretCarries(secret *corev1.Secret, password string) bool {
	for _, value := range secret.Data {
		if bytes.Contains(value, []byte(password)) {
			return true
		}
	}
	for _, value := range secret.StringData {
		if bytes.Contains([]byte(value), []byte(password)) {
			return true
		}
	}
	return false
}
