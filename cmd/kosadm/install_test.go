package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/client-go/rest"

	"github.com/tsic404/kosadm/pkg/install"
)

// TestManagedEndpointCarriesTheCertificateAuthority pins the external form's
// trust anchor: a kubeconfig may name its CA as data or as a file, and a
// kubeconfig that verifies nothing is refused instead of producing a credential
// that fails at the first request.
func TestManagedEndpointCarriesTheCertificateAuthority(t *testing.T) {
	directory := t.TempDir()
	caPath := filepath.Join(directory, "ca.pem")
	if err := os.WriteFile(caPath, []byte("file-authority"), 0o600); err != nil {
		t.Fatalf("write the certificate authority: %v", err)
	}
	authorityPath := filepath.Join(directory, "authority.pem")
	if err := os.WriteFile(authorityPath, []byte("flag-authority"), 0o600); err != nil {
		t.Fatalf("write the certificate authority: %v", err)
	}

	cases := []struct {
		name      string
		server    string
		authority string
		config    *rest.Config
		want      string
		wantHost  string
		wantErr   bool
	}{
		{
			name:   "kubeconfig data",
			config: &rest.Config{Host: "https://data.test:6443", TLSClientConfig: rest.TLSClientConfig{CAData: []byte("data-authority")}},
			want:   "data-authority", wantHost: "https://data.test:6443",
		},
		{
			name:   "kubeconfig file",
			config: &rest.Config{Host: "https://file.test:6443", TLSClientConfig: rest.TLSClientConfig{CAFile: caPath}},
			want:   "file-authority", wantHost: "https://file.test:6443",
		},
		{
			name:      "flag PEM",
			authority: "-----BEGIN CERTIFICATE-----\npem\n-----END CERTIFICATE-----",
			config:    &rest.Config{Host: "https://flag.test:6443"},
			want:      "-----BEGIN CERTIFICATE-----\npem\n-----END CERTIFICATE-----", wantHost: "https://flag.test:6443",
		},
		{
			name:      "flag file",
			authority: authorityPath,
			config:    &rest.Config{Host: "https://flag.test:6443"},
			want:      "flag-authority", wantHost: "https://flag.test:6443",
		},
		{
			name:   "flag server wins",
			server: "https://declared.test:6443",
			config: &rest.Config{Host: "https://data.test:6443", TLSClientConfig: rest.TLSClientConfig{CAData: []byte("data-authority")}},
			want:   "data-authority", wantHost: "https://declared.test:6443",
		},
		{
			name:    "a kubeconfig that verifies nothing",
			config:  &rest.Config{Host: "https://insecure.test:6443", TLSClientConfig: rest.TLSClientConfig{Insecure: true}},
			wantErr: true,
		},
		{
			name:      "a missing authority file",
			authority: filepath.Join(directory, "missing.pem"),
			config:    &rest.Config{Host: "https://flag.test:6443"},
			wantErr:   true,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			endpoint, err := managedEndpoint(testCase.server, testCase.authority, testCase.config)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("endpoint = %+v, want a refusal", endpoint)
				}
				return
			}
			if err != nil {
				t.Fatalf("managed endpoint: %v", err)
			}
			if endpoint.Server != testCase.wantHost {
				t.Fatalf("server = %q, want %q", endpoint.Server, testCase.wantHost)
			}
			if string(endpoint.Authority) != testCase.want {
				t.Fatalf("authority = %q, want %q", endpoint.Authority, testCase.want)
			}
		})
	}
}

// TestPrintResultKeepsAFailedRunFromNamingACredential pins the install output:
// only a completed run names a credential, and a run that failed after its
// steps still says it was rolled back.
func TestPrintResultKeepsAFailedRunFromNamingACredential(t *testing.T) {
	cases := []struct {
		name        string
		plan        install.Plan
		result      install.Result
		want        []string
		wantMissing []string
	}{
		{
			name: "existing cluster",
			plan: install.Plan{Mode: install.ModeExisting, Owner: "owner"},
			result: install.Result{
				Mode: install.ModeExisting, Namespace: "kos-system", Owner: "owner",
				OneTimePassword: "one-time", Completed: true,
			},
			want: []string{"First Owner: owner", "One-time password: one-time", "The first sign-in must replace it."},
		},
		{
			name: "a failed step",
			plan: install.Plan{Mode: install.ModeExisting, Owner: "owner"},
			result: install.Result{
				Mode: install.ModeExisting, Namespace: "kos-system", Owner: "owner",
				OneTimePassword: "one-time", StoppedAt: install.StepControlPlane, RolledBack: true,
			},
			want:        []string{"Stopped at: control-plane (rolled back: true)", "exists in the control-plane database, but the installation was rolled back"},
			wantMissing: []string{"One-time password", "First Owner:", "Claim endpoint"},
		},
		{
			name: "a failure after the last step",
			plan: install.Plan{Mode: install.ModeExisting, Owner: "owner"},
			result: install.Result{
				Mode: install.ModeExisting, Namespace: "kos-system", Owner: "owner",
				OneTimePassword: "one-time", RolledBack: true,
			},
			want:        []string{"Failed after the installation steps (rolled back: true)", "exists in the control-plane database, but the installation was rolled back"},
			wantMissing: []string{"One-time password", "First Owner:", "Claim endpoint"},
		},
		{
			name: "an external installation",
			plan: install.Plan{Mode: install.ModeExternal, Owner: "owner", Address: install.DefaultAddress},
			result: install.Result{
				Mode: install.ModeExternal, Namespace: "kos-system", Owner: "owner", Completed: true,
				OwnerClaim: install.OwnerClaim{URL: install.DefaultAddress + "/api/v1/bootstrap/owner", Instruction: "claim once"},
			},
			want:        []string{"Claim endpoint:", "claim once"},
			wantMissing: []string{"One-time password"},
		},
		{
			name: "a failed external run",
			plan: install.Plan{Mode: install.ModeExternal, Owner: "owner", Address: install.DefaultAddress},
			result: install.Result{
				Mode: install.ModeExternal, Namespace: "kos-system", Owner: "owner", StoppedAt: install.StepNodeAgent,
			},
			want:        []string{"Stopped at: node-agent"},
			wantMissing: []string{"One-time password", "First Owner:", "Claim endpoint"},
		},
		{
			name: "a run that never reached the account",
			plan: install.Plan{Mode: install.ModeExisting, Owner: "owner"},
			result: install.Result{
				Mode: install.ModeExisting, Namespace: "kos-system", StoppedAt: install.StepPreflight,
			},
			want:        []string{"Stopped at: preflight"},
			wantMissing: []string{"One-time password", "First Owner:", "exists in the control-plane database"},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var output bytes.Buffer
			printResult(&output, testCase.plan, testCase.result)
			printed := output.String()
			for _, want := range testCase.want {
				if !strings.Contains(printed, want) {
					t.Fatalf("output %q does not carry %q", printed, want)
				}
			}
			for _, missing := range testCase.wantMissing {
				if strings.Contains(printed, missing) {
					t.Fatalf("output %q carries %q", printed, missing)
				}
			}
		})
	}
}
