package install

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// TestCredentialStatesCoverTheExpiryBoundary pins the readable degradation a
// stored credential carries (C-14): usable comfortably before expiry, renew-soon
// inside the lead, expired at the deadline.
func TestCredentialStatesCoverTheExpiryBoundary(t *testing.T) {
	expiresAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	credential := Credential{ExpiresAt: expiresAt}
	cases := []struct {
		name    string
		at      time.Time
		want    CredentialState
		notice  bool
		contain string
	}{
		{name: "well before expiry", at: expiresAt.Add(-90 * 24 * time.Hour), want: CredentialUsable},
		{name: "inside the lead", at: expiresAt.Add(-24 * time.Hour), want: CredentialRenewSoon, notice: true, contain: "续期"},
		{name: "at the deadline", at: expiresAt, want: CredentialExpired, notice: true, contain: "过期"},
		{name: "after the deadline", at: expiresAt.Add(time.Hour), want: CredentialExpired, notice: true, contain: "过期"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if state := credential.State(testCase.at); state != testCase.want {
				t.Fatalf("state = %q, want %q", state, testCase.want)
			}
			notice := credential.Notice(testCase.at)
			if testCase.notice && !strings.Contains(notice, testCase.contain) {
				t.Fatalf("notice = %q, want one naming %q", notice, testCase.contain)
			}
			if !testCase.notice && notice != "" {
				t.Fatalf("notice = %q, want none", notice)
			}
		})
	}
	// A credential the installation recorded no expiry for is never reported
	// expired: the cluster gave no deadline to act on.
	if state := (Credential{}).State(expiresAt.Add(100 * 365 * 24 * time.Hour)); state != CredentialUsable {
		t.Fatalf("state without a recorded expiry = %q, want usable", state)
	}
}

// TestManagedClientsRefusesAnExpiredCredential pins the order of the read: the
// recorded deadline decides before any client is built, so an expired credential
// reports why instead of an unauthenticated list from the cluster (C-14).
func TestManagedClientsRefusesAnExpiredCredential(t *testing.T) {
	kube := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: ManagedCredentialSecret, Namespace: DefaultNamespace},
		Data: map[string][]byte{
			// An address nothing listens on: the case only passes while the
			// deadline is read before the cluster is reached.
			ManagedCredentialKey:       testKubeconfig(t, "https://127.0.0.1:1"),
			ManagedCredentialExpiryKey: []byte(time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)),
		},
	})
	_, err := ManagedClients(context.Background(), kube, DefaultNamespace)
	var expired *ExpiryError
	if !errors.As(err, &expired) {
		t.Fatalf("ManagedClients() error = %v, want an expiry error", err)
	}
	if expired.ExpiresAt.IsZero() {
		t.Fatal("the expiry error names no instant")
	}
}

// TestRenewRefusesAClusterOtherThanTheStoredOne pins the pairing rule: a renewal
// may only mint a token from the cluster the credential already points at
// (C-14).
func TestRenewRefusesAClusterOtherThanTheStoredOne(t *testing.T) {
	const stored = "https://managed.test:6443"
	control := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: ManagedCredentialSecret, Namespace: DefaultNamespace},
		Data:       map[string][]byte{ManagedCredentialKey: testKubeconfig(t, stored)},
	})
	other := &Cluster{
		Config: &rest.Config{Host: "https://other.test:6443"},
		Kube:   fake.NewSimpleClientset(),
	}
	_, err := RenewManagedCredential(context.Background(), control, other, DefaultNamespace)
	var mismatch *CredentialClusterMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("RenewManagedCredential() error = %v, want a cluster mismatch", err)
	}
	if mismatch.Stored != stored || mismatch.Offered != other.Config.Host {
		t.Fatalf("mismatch = %+v, want %q against %q", mismatch, stored, other.Config.Host)
	}
}

// testKubeconfig renders a credential pointing at one server with a token.
func testKubeconfig(t *testing.T, server string) []byte {
	t.Helper()
	const name = "kos-managed"
	encoded, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{name: {Server: server}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{name: {Token: "token"}},
		Contexts:       map[string]*clientcmdapi.Context{name: {Cluster: name, AuthInfo: name}},
		CurrentContext: name,
	})
	if err != nil {
		t.Fatalf("render a kubeconfig: %v", err)
	}
	return encoded
}
