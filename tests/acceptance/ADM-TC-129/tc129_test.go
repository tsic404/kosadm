// Package admtc129 verifies the claim window across a control-plane restart: a
// restart does not move the deadline, a claim before it still delivers a
// password, nothing extends the window, and an installation that discarded its
// control-plane database opens a new window (ADM-AC-63, ADM-TC-129).
package admtc129

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/pkg/identity"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
)

func TestTC129ClaimWindowAcrossRestart(t *testing.T) {
	ctx := context.Background()
	control := harness.Start(t)
	managed := harness.StartBareAudited(t)
	installerIdentity := harness.InstallerIdentity()
	control.GrantClusterAdmin(t, installerIdentity)
	managed.GrantClusterAdmin(t, installerIdentity)

	// A restart inside the window leaves the deadline where the installation
	// put it, and a claim before that deadline still delivers a password.
	t.Run("a restart keeps the deadline", func(t *testing.T) {
		accounts := harness.OpenIdentity(t, nil, nil)
		installer := harness.ExternalInstaller(t, ctx, managed, control, accounts, nil)
		if _, err := installer.InstallExternal(ctx); err != nil {
			t.Fatalf("install the external form: %v", err)
		}
		opened, err := accounts.Store.BootstrapWindow(ctx)
		if err != nil {
			t.Fatalf("read the claim window: %v", err)
		}
		if opened.OpenedAt.IsZero() || !opened.Open(accounts.Clock.Now()) {
			t.Fatalf("claim window = %+v, want an open window", opened)
		}

		restarted := accounts.Restart(t)
		afterRestart, err := restarted.Store.BootstrapWindow(ctx)
		if err != nil {
			t.Fatalf("read the claim window after the restart: %v", err)
		}
		if !afterRestart.OpenedAt.Equal(opened.OpenedAt) || !afterRestart.Deadline.Equal(opened.Deadline) {
			t.Fatalf("restart moved the window from %+v to %+v", opened, afterRestart)
		}
		// Opening the window again is the only path a reinstall takes, and it
		// is not an extension: the deadline stays.
		if err := restarted.Service.OpenBootstrapWindow(ctx); err != nil {
			t.Fatalf("open the window again: %v", err)
		}
		afterReopen, err := restarted.Store.BootstrapWindow(ctx)
		if err != nil {
			t.Fatalf("read the window after reopening it: %v", err)
		}
		if !afterReopen.Deadline.Equal(opened.Deadline) {
			t.Fatalf("the window was extended to %s, want %s", afterReopen.Deadline, opened.Deadline)
		}

		restarted.Clock.Advance(identity.BootstrapWindowLength / 2)
		bootstrap, err := restarted.Service.ClaimFirstOwner(ctx)
		if err != nil {
			t.Fatalf("claim before the original deadline after a restart: %v", err)
		}
		if bootstrap.Username != "owner" || bootstrap.OneTimePassword == "" {
			t.Fatalf("claim answered %+v, want the first Owner and a password", bootstrap)
		}
		login, err := restarted.Service.Login(ctx, bootstrap.Username, bootstrap.OneTimePassword)
		if err != nil {
			t.Fatalf("sign in with the claimed password: %v", err)
		}
		if !login.Session.MustChangePassword {
			t.Fatal("the claimed password was not a one-time password")
		}
		if _, err := restarted.Service.ClaimFirstOwner(ctx); !errors.Is(err, identity.ErrBootstrapClosed) {
			t.Fatalf("second claim after the restart = %v, want a closed window", err)
		}
	})

	// A claim after the original deadline is refused, even though the control
	// plane restarted inside the window, and a reinstall that kept the database
	// does not open a second window.
	t.Run("a claim after the original deadline is refused", func(t *testing.T) {
		accounts := harness.OpenIdentity(t, nil, nil)
		installer := harness.ExternalInstaller(t, ctx, managed, control, accounts, nil)
		if _, err := installer.InstallExternal(ctx); err != nil {
			t.Fatalf("install the external form: %v", err)
		}
		opened, err := accounts.Store.BootstrapWindow(ctx)
		if err != nil {
			t.Fatalf("read the claim window: %v", err)
		}
		restarted := accounts.Restart(t)
		restarted.Clock.Advance(identity.BootstrapWindowLength + identity.BootstrapWindowLength/2)
		if _, err := restarted.Service.ClaimFirstOwner(ctx); !errors.Is(err, identity.ErrBootstrapClosed) {
			t.Fatalf("claim after the original deadline = %v, want a closed window", err)
		}

		reinstalled := harness.ExternalInstaller(t, ctx, managed, control, restarted, nil)
		if _, err := reinstalled.InstallExternal(ctx); err != nil {
			t.Fatalf("reinstall keeping the control-plane database: %v", err)
		}
		afterReinstall, err := restarted.Store.BootstrapWindow(ctx)
		if err != nil {
			t.Fatalf("read the window after the reinstall: %v", err)
		}
		if !afterReinstall.OpenedAt.Equal(opened.OpenedAt) || !afterReinstall.Deadline.Equal(opened.Deadline) {
			t.Fatalf("the reinstall moved the window from %+v to %+v", opened, afterReinstall)
		}
		if _, err := restarted.Service.ClaimFirstOwner(ctx); !errors.Is(err, identity.ErrBootstrapClosed) {
			t.Fatalf("claim after the reinstall = %v, want a closed window", err)
		}
	})

	// A window that expired unclaimed, followed by an installation that keeps
	// no control-plane database, opens a new window with a new password.
	t.Run("a discarded database opens a new window", func(t *testing.T) {
		accounts := harness.OpenIdentity(t, nil, nil)
		installer := harness.ExternalInstaller(t, ctx, managed, control, accounts, nil)
		if _, err := installer.InstallExternal(ctx); err != nil {
			t.Fatalf("install the external form: %v", err)
		}
		expired, err := accounts.Store.BootstrapWindow(ctx)
		if err != nil {
			t.Fatalf("read the claim window: %v", err)
		}
		accounts.Clock.Advance(identity.BootstrapWindowLength + identity.BootstrapWindowLength/2)
		if _, err := accounts.Service.ClaimFirstOwner(ctx); !errors.Is(err, identity.ErrBootstrapClosed) {
			t.Fatalf("claim on an expired window = %v, want a closed window", err)
		}

		// The reinstallation after an uninstall that kept no database starts
		// from an empty one.
		fresh := accounts.DiscardDatabase(t)
		reinstalled := harness.ExternalInstaller(t, ctx, managed, control, fresh, nil)
		if _, err := reinstalled.InstallExternal(ctx); err != nil {
			t.Fatalf("reinstall after discarding the database: %v", err)
		}
		reopened, err := fresh.Store.BootstrapWindow(ctx)
		if err != nil {
			t.Fatalf("read the window of the reinstallation: %v", err)
		}
		if !reopened.OpenedAt.After(expired.Deadline) || !reopened.Open(fresh.Clock.Now()) {
			t.Fatalf("reinstallation window = %+v, want one opened after %s", reopened, expired.Deadline)
		}
		bootstrap, err := fresh.Service.ClaimFirstOwner(ctx)
		if err != nil {
			t.Fatalf("claim on the new window: %v", err)
		}
		if bootstrap.Username != "owner" || bootstrap.OneTimePassword == "" {
			t.Fatalf("claim answered %+v, want the first Owner and a password", bootstrap)
		}
		if _, err := fresh.Service.Login(ctx, bootstrap.Username, bootstrap.OneTimePassword); err != nil {
			t.Fatalf("sign in with the new password: %v", err)
		}
	})

	// An installation that failed after creating the first Owner leaves no
	// claim window: the window opens when the installation completes, not when
	// its last step ran (C-68).
	t.Run("a failed installation leaves no window", func(t *testing.T) {
		accounts := harness.OpenIdentity(t, nil, nil)
		// One reconciled=False object keeps the install window open until the
		// settle budget runs out, which is a failure after the first Owner was
		// created.
		pending := harness.Application("pending", nil)
		status := map[string]any{"conditions": []any{map[string]any{
			"type": api.ConditionReconciled, "status": "False", "reason": "Pending",
			"message": "the controller has not reconciled", "lastTransitionTime": "2026-10-25T00:00:00Z",
		}}}
		applications := managed.Dynamic.Resource(schema.GroupVersionResource{
			Group: api.Group, Version: "v1beta1", Resource: "applications",
		}).Namespace(harness.Namespace)
		var once sync.Once
		installer := harness.ExternalInstaller(t, ctx, managed, control, accounts, func(config *install.Config) {
			config.SettleTimeout = time.Second
			config.AfterStep = func(ctx context.Context, step string) error {
				if step != install.StepPermissions {
					return nil
				}
				var err error
				once.Do(func() {
					// The definition carries a status subresource, so the
					// condition is written after the create.
					var created *unstructured.Unstructured
					if created, err = applications.Create(ctx, pending, metav1.CreateOptions{}); err != nil {
						return
					}
					created.Object["status"] = status
					_, err = applications.UpdateStatus(ctx, created, metav1.UpdateOptions{})
				})
				return err
			}
		})
		t.Cleanup(func() { _ = applications.Delete(context.Background(), "pending", metav1.DeleteOptions{}) })
		result, err := installer.InstallExternal(ctx)
		if err == nil {
			t.Fatal("the installation completed with an unreconciled object in the window")
		}
		// The run failed after its steps: no step stopped it, the window stayed
		// open, and the rollback ran. Only the account survived the rollback.
		if result.Completed {
			t.Fatalf("result = %+v, want a run that did not complete", result)
		}
		if !result.RolledBack {
			t.Fatalf("result = %+v, want a rolled-back run", result)
		}
		if result.Owner != "owner" || result.OneTimePassword != "" {
			t.Fatalf("result = %+v, want the created account and no credential", result)
		}
		if name := accounts.Service.FirstOwnerName(ctx); name != "owner" {
			t.Fatalf("first Owner = %q, want the account the failed run created", name)
		}
		if _, err := accounts.Store.BootstrapWindow(ctx); err == nil {
			t.Fatal("a failed installation left a claim window")
		}
		if _, err := accounts.Service.ClaimFirstOwner(ctx); !errors.Is(err, identity.ErrBootstrapClosed) {
			t.Fatalf("claim after a failed installation = %v, want a closed window", err)
		}
	})
}
