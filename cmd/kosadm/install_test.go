package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"

	"github.com/tsic404/kosadm/pkg/install"
)

// TestRequireNodeRefusesAPinTheClusterCannotHonour pins the pin's precondition:
// a control-plane node the workload cluster does not have leaves the pod
// unschedulable, so the installation has to refuse it before writing anything
// rather than report success over a control plane that never starts.
func TestRequireNodeRefusesAPinTheClusterCannotHonour(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "node-a",
		Labels: map[string]string{"kubernetes.io/hostname": "node-a-host"},
	}}
	kube := fake.NewSimpleClientset(node)
	if err := requireNode(context.Background(), kube, "node-a"); err != nil {
		t.Fatalf("a node the cluster has was refused: %v", err)
	}
	if err := requireNode(context.Background(), kube, ""); err != nil {
		t.Fatalf("an empty pin was refused: %v", err)
	}
	// The pin binds the node's identity, so a name that exists must pass even
	// when its hostname label says something else.
	for name, pin := range map[string]string{
		"a node the cluster lacks": "node-b",
	} {
		err := requireNode(context.Background(), kube, pin)
		if err == nil {
			t.Fatalf("%s: %q was accepted as the control-plane node", name, pin)
		}
		var coded exitError
		if !errors.As(err, &coded) || coded.code != 2 {
			t.Fatalf("%s: %v, want a usage error", name, err)
		}
	}
}

// TestWaitForNodeAcceptsALateJoiningNode pins the bootstrap boundary: the
// provisioning command brings the run kernel up and the API server answers
// before every node registers, so the pinned node may arrive after the first
// check. The wait has to hold for the declared node timeout, not fail on the
// first observation, or an installation that could complete would exit before
// writing anything.
func TestWaitForNodeAcceptsALateJoiningNode(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}
	kube := fake.NewSimpleClientset()
	joined := make(chan struct{})
	kube.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		select {
		case <-joined:
			return true, node, nil
		default:
		}
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "nodes"}, "node-a")
	})
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(joined)
	}()
	node_, err := awaitDatabaseNode(context.Background(), kube, "node-a", 3*time.Second)
	if err != nil {
		t.Fatalf("a node that joined within the timeout was refused: %v", err)
	}
	if node_ != "node-a" {
		t.Fatalf("the wait returned node %q, want node-a", node_)
	}
}

// observeNode is one observation the wait's cases drive: it records the context
// the wait handed it, so a case can assert the request itself carries the
// deadline instead of only the gaps between requests.
func observeNode(answer func(ctx context.Context) (bool, error)) (func(context.Context) (bool, error), *context.Context) {
	var seen context.Context
	return func(ctx context.Context) (bool, error) {
		seen = ctx
		return answer(ctx)
	}, &seen
}

// TestWaitForNodeBoundsTheObservationItself pins that the timeout covers the
// observation request, not just the gap between two of them. A read that hangs
// until its deadline has to end the wait at the deadline; a wait that handed the
// request an unbounded context would sit in it long past the timeout.
//
// The failure is reported as what it is, a read that did not answer, and not as
// a node that failed to join: the wait never learned whether the node was there,
// so blaming the node would send the operator to check the wrong thing.
func TestWaitForNodeBoundsTheObservationItself(t *testing.T) {
	observe, seen := observeNode(func(ctx context.Context) (bool, error) {
		// Answer only when the request's own deadline ends it.
		<-ctx.Done()
		return false, apierrors.NewTimeoutError("request timed out", 1)
	})
	start := time.Now()
	err := awaitNode(context.Background(), 200*time.Millisecond, "--control-plane-node node-a", observe)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a node that never answered was accepted")
	}
	if *seen == nil {
		t.Fatal("the wait made no observation")
	}
	if deadline, ok := (*seen).Deadline(); !ok {
		t.Fatal("the observation carried no deadline: the timeout only bounded the gaps between reads")
	} else if remaining := time.Until(deadline); remaining > time.Second {
		t.Fatalf("the observation's deadline is %s away, want it bounded by the 200ms timeout", remaining)
	}
	var coded exitError
	if errors.As(err, &coded) {
		t.Fatalf("hung observation = %v, want the read failure itself, not a node that did not join", err)
	}
	if !apierrors.IsTimeout(err) {
		t.Fatalf("hung observation = %v, want it to carry the timeout", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("the wait returned after %s, want it bounded by the 200ms timeout", elapsed)
	}
}

// TestWaitForNodeReportsAnInterruptedWait pins the third outcome apart from the
// other two: a wait the operator interrupted (SIGINT, a cancelled parent) is
// not a node that failed to join and not a cluster that failed to answer. The
// interruption is reported as such.
func TestWaitForNodeReportsAnInterruptedWait(t *testing.T) {
	observe, _ := observeNode(func(ctx context.Context) (bool, error) {
		<-ctx.Done()
		return false, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := awaitNode(ctx, 5*time.Second, "--control-plane-node node-a", observe)
	if err == nil {
		t.Fatal("an interrupted wait was accepted")
	}
	var coded exitError
	if errors.As(err, &coded) {
		t.Fatalf("interrupted wait = %v, want the interruption, not a node that did not join", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted wait = %v, want it to carry the cancellation", err)
	}
}

// TestWaitForNodeKeepsTheCallersCancellationOverADeadline pins the order of the
// two ways a wait can end at the same moment. A command the operator stopped
// has learned nothing about the node, so it must not be reported as a node that
// failed to join even when the wait's own deadline has also passed.
func TestWaitForNodeKeepsTheCallersCancellationOverADeadline(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, stop := context.WithTimeout(context.Background(), 0)
	defer stop()
	if err := waitEnded(canceled, expired, "--control-plane-node node-a", time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel and deadline together = %v, want the cancellation", err)
	}
	var coded exitError
	if err := waitEnded(canceled, expired, "--control-plane-node node-a", time.Second); errors.As(err, &coded) {
		t.Fatalf("cancel and deadline together = %v, want no usage error", err)
	}
}

// TestWaitForNodeKeepsACancellationThatLandsBetweenObservations pins the
// interruption that arrives after an observation answered and before the next
// one: the request completed, so nothing about it is a cluster failure, and the
// wait is stopped by the operator rather than by the node staying absent.
func TestWaitForNodeKeepsACancellationThatLandsBetweenObservations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	observations := 0
	err := awaitNode(ctx, time.Minute, "--control-plane-node node-a", func(context.Context) (bool, error) {
		observations++
		if observations == 1 {
			go func() {
				time.Sleep(20 * time.Millisecond)
				cancel()
			}()
		}
		return false, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted wait = %v, want the cancellation", err)
	}
	var coded exitError
	if errors.As(err, &coded) {
		t.Fatalf("interrupted wait = %v, want no usage error", err)
	}
}

// TestWaitForNodeRefusesANodeThatJoinsAfterTheDeadline pins the other side of
// the bound: a node whose registration lands only after the deadline is not a
// node the wait may accept, and the wait does not extend itself with one more
// observation once the budget is spent.
func TestWaitForNodeRefusesANodeThatJoinsAfterTheDeadline(t *testing.T) {
	observe, _ := observeNode(func(ctx context.Context) (bool, error) {
		// The node registers after the deadline: this read comes back with it
		// once the budget is already spent.
		time.Sleep(250 * time.Millisecond)
		return true, nil
	})
	err := awaitNode(context.Background(), 200*time.Millisecond, "--control-plane-node node-a", observe)
	if err == nil {
		t.Fatal("a node that joined only after the deadline was accepted")
	}
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 2 {
		t.Fatalf("late node = %v, want a usage error", err)
	}
}

// TestWaitForNodeRefusesANodeThatNeverJoins pins the plain timeout: a node that
// stays absent for the whole budget is refused with a usage error, before any
// KOS object is written.
func TestWaitForNodeRefusesANodeThatNeverJoins(t *testing.T) {
	kube := fake.NewSimpleClientset()
	_, err := awaitDatabaseNode(context.Background(), kube, "node-a", 150*time.Millisecond)
	if err == nil {
		t.Fatal("a node that never joined was accepted")
	}
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 2 {
		t.Fatalf("missing node = %v, want a usage error", err)
	}
}

// TestWaitForNodeReportsAClusterErrorInsteadOfBlamingTheNode pins the
// distinction between "the node has not joined" and "the cluster did not
// answer": only a missing node means the node is late. A permission or
// connectivity error says nothing about the node, so a wait that folded it into
// the timeout would send the operator to fix the node instead of the access
// that actually failed.
func TestWaitForNodeReportsAClusterErrorInsteadOfBlamingTheNode(t *testing.T) {
	cases := map[string]struct {
		failure error
		matches func(error) bool
	}{
		"forbidden":   {apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"}, "node-a", errors.New("denied")), apierrors.IsForbidden},
		"unavailable": {apierrors.NewServiceUnavailable("the API server is not ready"), apierrors.IsServiceUnavailable},
		"internal":    {apierrors.NewInternalError(errors.New("boom")), apierrors.IsInternalError},
	}
	for name, testCase := range cases {
		kube := fake.NewSimpleClientset()
		kube.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, testCase.failure
		})
		_, err := awaitDatabaseNode(context.Background(), kube, "node-a", 200*time.Millisecond)
		if err == nil {
			t.Fatalf("%s: the cluster error was swallowed as a successful wait", name)
		}
		var coded exitError
		if errors.As(err, &coded) {
			t.Fatalf("%s: %v was reported as a node that did not join, want the cluster error itself", name, err)
		}
		if !testCase.matches(err) {
			t.Fatalf("%s: %v does not carry the cluster error", name, err)
		}
	}
}

// TestDeliverDatabaseHandsTheWorkloadTheSeededFile pins the CLI seam of
// ADM-TC-131: the plan the installation renders names the same database file
// the run opened the first Owner in, resolved to an absolute path and carrying
// the account that owns it. The rendered workload then serves exactly that
// file, so the one-time password in the install output signs in.
func TestDeliverDatabaseHandsTheWorkloadTheSeededFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.db")
	if err := os.WriteFile(path, []byte("seeded"), 0o600); err != nil {
		t.Fatalf("write the database: %v", err)
	}
	plan, err := deliverDatabase(install.Plan{Mode: install.ModeExisting, Owner: "owner"}, path, "node-a")
	if err != nil {
		t.Fatalf("deliver the database: %v", err)
	}
	if plan.Database == nil || plan.Database.Path != path {
		t.Fatalf("plan database = %+v, want %s", plan.Database, path)
	}
	if plan.Database.Node != "node-a" {
		t.Fatalf("plan database node = %q, want node-a", plan.Database.Node)
	}
	if plan.Database.Owner.UID != int64(os.Getuid()) || plan.Database.Owner.GID != int64(os.Getgid()) {
		t.Fatalf("database owner = %+v, want uid/gid %d/%d", plan.Database.Owner, os.Getuid(), os.Getgid())
	}

	documents, err := install.Render(plan)
	if err != nil {
		t.Fatalf("render the installation: %v", err)
	}
	for _, document := range documents {
		if document.GVR.Resource != "deployments" {
			continue
		}
		containers, _, err := unstructured.NestedSlice(document.Object.Object, "spec", "template", "spec", "containers")
		if err != nil || len(containers) != 1 {
			t.Fatalf("containers = %v (%v), want one", containers, err)
		}
		container, _ := containers[0].(map[string]any)
		args, _ := container["args"].([]any)
		if len(args) < 2 || args[0] != "-database" || args[1] != path {
			t.Fatalf("the workload args = %v, want -database %s", container["args"], path)
		}
		return
	}
	t.Fatal("the installation rendered no control-plane Deployment")
}

// TestDeliverDatabaseRefusesAPathTheWorkloadCannotServe pins the refusal: a
// database path that names no file would leave the control plane on a database
// it creates itself, which is the credential that cannot sign in.
func TestDeliverDatabaseRefusesAPathTheWorkloadCannotServe(t *testing.T) {
	_, err := deliverDatabase(install.Plan{Mode: install.ModeExisting, Owner: "owner"},
		filepath.Join(t.TempDir(), "absent.db"), "node-a")
	if err == nil {
		t.Fatal("a database path that does not exist was delivered")
	}
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 2 {
		t.Fatalf("missing database = %v, want a usage error", err)
	}
}

// TestInstallPinsTheControlPlaneToTheNodeHoldingTheDatabase covers the install
// entry's default path: no --control-plane-node, two schedulable nodes, and the
// database only on the host the command runs on. The run has to identify that
// node and pin the control plane to it, because the database is a hostPath and
// the other node's filesystem holds nothing at that path.
func TestInstallPinsTheControlPlaneToTheNodeHoldingTheDatabase(t *testing.T) {
	original := hostName
	hostName = func() (string, error) { return "kos-a", nil }
	t.Cleanup(func() { hostName = original })
	kube := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-a", Labels: map[string]string{corev1.LabelHostname: "kos-a"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-b", Labels: map[string]string{corev1.LabelHostname: "kos-b"}}},
	)
	path := filepath.Join(t.TempDir(), "kos.db")
	if err := os.WriteFile(path, []byte("seeded"), 0o600); err != nil {
		t.Fatalf("write the database: %v", err)
	}

	node, err := databaseNode(context.Background(), kube, "")
	if err != nil {
		t.Fatalf("find the node that holds the database: %v", err)
	}
	if node != "kos-a" {
		t.Fatalf("control-plane node = %q, want kos-a, the node that is this host", node)
	}
	plan, err := deliverDatabase(install.Plan{Mode: install.ModeExisting, Owner: "owner"}, path, node)
	if err != nil {
		t.Fatalf("deliver the database: %v", err)
	}
	if plan.Database == nil || plan.Database.Node != "kos-a" {
		t.Fatalf("plan database = %+v, want node kos-a", plan.Database)
	}

	documents, err := install.Render(plan)
	if err != nil {
		t.Fatalf("render the installation: %v", err)
	}
	for _, document := range documents {
		if document.GVR.Resource != "deployments" {
			continue
		}
		affinity, found, err := unstructured.NestedSlice(document.Object.Object,
			"spec", "template", "spec", "affinity", "nodeAffinity",
			"requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms")
		if err != nil || !found || len(affinity) != 1 {
			t.Fatalf("the workload's node affinity = %v (%v), want one required term", affinity, err)
		}
		term, _ := affinity[0].(map[string]any)
		fields, _ := term["matchFields"].([]any)
		if len(fields) != 1 {
			t.Fatalf("the workload's node selector = %v, want one matchField", fields)
		}
		field, _ := fields[0].(map[string]any)
		if field["key"] != "metadata.name" || field["operator"] != "In" ||
			len(field["values"].([]any)) != 1 || field["values"].([]any)[0] != "kos-a" {
			t.Fatalf("the workload's pin = %v, want the node holding the database", field)
		}
		return
	}
	t.Fatal("the installation rendered no control-plane Deployment")
}

// TestInstallRefusesAHostTheClusterDoesNotKnow pins the refusal: a database on
// a host no node represents cannot be shown to be on any node's filesystem, so
// the run stops before writing anything instead of rendering an unpinned
// workload that reads an empty directory.
func TestInstallRefusesAHostTheClusterDoesNotKnow(t *testing.T) {
	original := hostName
	hostName = func() (string, error) { return "kos-c", nil }
	t.Cleanup(func() { hostName = original })
	kube := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-b"}},
	)
	_, err := databaseNode(context.Background(), kube, "")
	if err == nil {
		t.Fatal("a host no node reports was accepted without a pin")
	}
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 2 {
		t.Fatalf("unknown host = %v, want a usage error naming --control-plane-node", err)
	}
	if !strings.Contains(err.Error(), "--control-plane-node") {
		t.Fatalf("unknown host = %v, want it to name the flag that resolves it", err)
	}
}

// TestInstallRefusesAHostThatOnlyAnotherDomainReports pins the cross-domain
// refusal at the command's own decision point: a node in another domain is not
// this machine, even when both names share a short name. Accepting it would pin
// the control plane to a filesystem that does not hold the database.
func TestInstallRefusesAHostThatOnlyAnotherDomainReports(t *testing.T) {
	original := hostName
	hostName = func() (string, error) { return "worker.site-a.example", nil }
	t.Cleanup(func() { hostName = original })
	kube := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "worker",
		Labels: map[string]string{corev1.LabelHostname: "worker.site-b.example"},
	}})
	_, err := databaseNode(context.Background(), kube, "")
	if err == nil {
		t.Fatal("a node that reports another machine's name was accepted")
	}
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 2 {
		t.Fatalf("other-domain host = %v, want a usage error naming --control-plane-node", err)
	}
	if !strings.Contains(err.Error(), "--control-plane-node") {
		t.Fatalf("other-domain host = %v, want it to name the flag that resolves it", err)
	}
}

// TestInstallNamesTheNodeFlagWhenTheHostIsAmbiguous pins ADM-C-70's remedy: two
// nodes that both report this host are two filesystems, and the command cannot
// choose between them. The refusal names --control-plane-node rather than the
// node list alone, so the operator has the step that resolves it.
func TestInstallNamesTheNodeFlagWhenTheHostIsAmbiguous(t *testing.T) {
	original := hostName
	hostName = func() (string, error) { return "kos-a", nil }
	t.Cleanup(func() { hostName = original })
	kube := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-a", Labels: map[string]string{corev1.LabelHostname: "kos-a"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-a-2", Labels: map[string]string{corev1.LabelHostname: "kos-a"}}},
	)

	for name, resolve := range map[string]func() error{
		"existing cluster": func() error {
			_, err := databaseNode(context.Background(), kube, "")
			return err
		},
		"bootstrap": func() error {
			_, err := awaitDatabaseNode(context.Background(), kube, "", 5*time.Second)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := resolve()
			if err == nil {
				t.Fatal("an ambiguous host was resolved without a flag")
			}
			var coded exitError
			if !errors.As(err, &coded) || coded.code != 2 {
				t.Fatalf("ambiguous host = %v, want a usage error", err)
			}
			if !strings.Contains(coded.message, "--control-plane-node") {
				t.Fatalf("ambiguous host = %v, want it to name --control-plane-node", err)
			}
			for _, node := range []string{"kos-a", "kos-a-2"} {
				if !strings.Contains(coded.message, node) {
					t.Fatalf("ambiguous host = %v, want it to list %s", err, node)
				}
			}
		})
	}
}

// TestBootstrapNamesTheNodeFlagWhenNoNodeJoins pins the same remedy for the
// bootstrap form's zero-match timeout: the node that is this host never
// registered, so the wait has to say that naming --control-plane-node is how
// the operator gets past it. The timeout classification itself is unchanged.
func TestBootstrapNamesTheNodeFlagWhenNoNodeJoins(t *testing.T) {
	original := hostName
	hostName = func() (string, error) { return "kos-a", nil }
	t.Cleanup(func() { hostName = original })
	kube := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-b", Labels: map[string]string{corev1.LabelHostname: "kos-b"}}},
	)
	_, err := awaitDatabaseNode(context.Background(), kube, "", 150*time.Millisecond)
	if err == nil {
		t.Fatal("a host no node reports was accepted")
	}
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 2 {
		t.Fatalf("missing host node = %v, want a usage error", err)
	}
	if !strings.Contains(coded.message, "--control-plane-node") {
		t.Fatalf("missing host node = %v, want it to name --control-plane-node", err)
	}
	if !strings.Contains(coded.message, notJoined) {
		t.Fatalf("missing host node = %v, want the timeout reported as a node that did not join", err)
	}
}

// TestBootstrapKeepsItsCancellationAndClusterFailuresApart pins that the new
// remedy does not collapse the outcomes: a cancelled wait stays a cancellation,
// and a cluster that failed to answer stays that failure, neither becoming a
// missing node with a flag hint.
func TestBootstrapKeepsItsCancellationAndClusterFailuresApart(t *testing.T) {
	original := hostName
	hostName = func() (string, error) { return "kos-a", nil }
	t.Cleanup(func() { hostName = original })

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()
		_, err := awaitDatabaseNode(ctx, fake.NewSimpleClientset(), "", 5*time.Second)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled wait = %v, want the cancellation", err)
		}
		var coded exitError
		if errors.As(err, &coded) {
			t.Fatalf("cancelled wait = %v, want no usage error", err)
		}
	})

	t.Run("cluster failure", func(t *testing.T) {
		kube := fake.NewSimpleClientset()
		kube.PrependReactor("list", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewServiceUnavailable("the API server is not ready")
		})
		_, err := awaitDatabaseNode(context.Background(), kube, "", 200*time.Millisecond)
		if err == nil {
			t.Fatal("a cluster that did not answer was accepted")
		}
		var coded exitError
		if errors.As(err, &coded) {
			t.Fatalf("cluster failure = %v, want the cluster error itself", err)
		}
		if !apierrors.IsServiceUnavailable(err) {
			t.Fatalf("cluster failure = %v, want it to carry the API error", err)
		}
	})
}

// TestInstallKeepsAnOperatorsNodePin pins that the resolution never overrides a
// node the operator named: the flag is the operator's statement about where the
// file is, including on a host whose name differs from the node's.
func TestInstallKeepsAnOperatorsNodePin(t *testing.T) {
	original := hostName
	hostName = func() (string, error) { return "kos-a", nil }
	t.Cleanup(func() { hostName = original })
	kube := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kos-b"}},
	)
	node, err := databaseNode(context.Background(), kube, "kos-b")
	if err != nil {
		t.Fatalf("use the operator's node: %v", err)
	}
	if node != "kos-b" {
		t.Fatalf("control-plane node = %q, want the operator's kos-b", node)
	}
}

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

// TestTheCommandEntrySaysTheControlPlaneIsPinned pins the help text and the
// flag description to ADM-C-70: the control plane is pinned to the node that
// holds the database, and no single-node form leaves it unpinned.
func TestTheCommandEntrySaysTheControlPlaneIsPinned(t *testing.T) {
	var help bytes.Buffer
	usage(&help)
	if !strings.Contains(help.String(), "--control-plane-node") {
		t.Fatalf("usage does not mention --control-plane-node:\n%s", help.String())
	}
	if strings.Contains(help.String(), "optional on one node") {
		t.Fatalf("usage still tells single-node installs the node is optional:\n%s", help.String())
	}
	if !strings.Contains(help.String(), "pinned") {
		t.Fatalf("usage does not say the control plane is pinned:\n%s", help.String())
	}

	var flags installFlags
	set := flag.NewFlagSet("install", flag.ContinueOnError)
	flags.register(set)
	described := set.Lookup("control-plane-node")
	if described == nil {
		t.Fatal("install does not register --control-plane-node")
	}
	if strings.Contains(described.Usage, "optional") {
		t.Fatalf("--control-plane-node is described as optional: %q", described.Usage)
	}
	if !strings.Contains(described.Usage, "pinned") {
		t.Fatalf("--control-plane-node does not say the control plane is pinned: %q", described.Usage)
	}
}
