// Package admtc93 verifies the single-node promise: one control-plane replica, no
// automatic migration of a workload off a failed node, and a notice that
// claims no high availability (ADM-AC-46, ADM-TC-93).
package admtc93

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
	"github.com/tsic404/kosd/api"
)

func TestTC93SingleNodeCommitment(t *testing.T) {
	cluster := harness.Start(t)
	installerIdentity := harness.InstallerIdentity()
	cluster.GrantClusterAdmin(t, installerIdentity)
	identity := harness.OpenIdentity(t, nil, nil)
	clients := cluster.InstallClients(t, installerIdentity)
	ctx := context.Background()

	plan := harness.InstallerPlan(nil)
	runner, err := install.New(install.Config{
		Plan:                  plan,
		Dynamic:               clients.Dynamic,
		Kube:                  clients.Kube,
		APIExtensions:         clients.APIExtensions,
		Definitions:           harness.Definitions(t),
		Accounts:              identity.Service,
		QuietWindow:           200 * time.Millisecond,
		SettleTimeout:         30 * time.Second,
		NoNodeAgentController: true,
	})
	if err != nil {
		t.Fatalf("build installer: %v", err)
	}
	if _, err := runner.Install(ctx); err != nil {
		t.Fatalf("install: %v", err)
	}

	deployment, err := cluster.Kube.AppsV1().Deployments(api.SystemNamespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the control-plane deployment: %v", err)
	}
	if replicas := *deployment.Spec.Replicas; replicas != 1 {
		t.Fatalf("control-plane replicas = %d, want one", replicas)
	}
	assertNoDisruptionBudget(t, cluster)

	commitment := runner.Plan().Commitment()
	if commitment.Replicas != 1 || commitment.AutomaticMigration {
		t.Fatalf("commitment = %+v, want one replica and no automatic migration", commitment)
	}
	if !strings.Contains(commitment.Notice, "不承诺高可用") || strings.Contains(commitment.Notice, "高可用集群") {
		t.Fatalf("notice = %q, want the degradation stated and no availability claim", commitment.Notice)
	}

	// A failed node does not move the workload: KOS starts no migration and
	// the pod keeps its node.
	createNodeAndPod(t, cluster)
	markNodeNotReady(t, cluster, "node-a")
	snapshotter := &install.Snapshotter{Dynamic: cluster.Dynamic, Discovery: cluster.Kube.Discovery()}
	before, err := snapshotter.Take(ctx)
	if err != nil {
		t.Fatalf("snapshot after the node failure: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	after, err := snapshotter.Take(ctx)
	if err != nil {
		t.Fatalf("snapshot after the wait: %v", err)
	}
	beforeObjects := withoutServerBookkeeping(before.Objects)
	afterObjects := withoutServerBookkeeping(after.Objects)
	if !reflect.DeepEqual(beforeObjects, afterObjects) {
		for key, state := range afterObjects {
			if previous, found := beforeObjects[key]; !found || !reflect.DeepEqual(previous, state) {
				t.Logf("changed: %v", key)
			}
		}
		for key := range beforeObjects {
			if _, found := afterObjects[key]; !found {
				t.Logf("disappeared: %v", key)
			}
		}
		t.Fatal("the node failure changed cluster objects; KOS must not migrate the workload")
	}
	pods, err := cluster.Kube.CoreV1().Pods(harness.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 || pods.Items[0].Spec.NodeName != "node-a" {
		t.Fatalf("pods = %+v, want the one pod still on node-a", pods.Items)
	}
}

// withoutServerBookkeeping drops the objects the API server's own identity
// machinery renews: leases in kube-system change on their own timer and are not
// workload state.
func withoutServerBookkeeping(objects map[install.ObjectKey]install.ObjectState) map[install.ObjectKey]install.ObjectState {
	filtered := make(map[install.ObjectKey]install.ObjectState, len(objects))
	for key, state := range objects {
		if key.Group == "coordination.k8s.io" && key.Kind == "Lease" && key.Namespace == "kube-system" {
			continue
		}
		filtered[key] = state
	}
	return filtered
}

// assertNoDisruptionBudget checks KOS renders no availability machinery.
func assertNoDisruptionBudget(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	budgets, err := cluster.Kube.PolicyV1().PodDisruptionBudgets(metav1.NamespaceAll).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list disruption budgets: %v", err)
	}
	if len(budgets.Items) != 0 {
		t.Fatalf("disruption budgets = %+v, want none", budgets.Items)
	}
}

// createNodeAndPod places one workload on one node.
func createNodeAndPod(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	ctx := context.Background()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}
	if _, err := cluster.Kube.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create node-a: %v", err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "shop-0", Namespace: harness.Namespace},
		Spec: corev1.PodSpec{
			NodeName:   "node-a",
			Containers: []corev1.Container{{Name: "shop", Image: "busybox"}},
		},
	}
	if _, err := cluster.Kube.CoreV1().Pods(harness.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create the pod: %v", err)
	}
}

// markNodeNotReady reports the node failure the kubelet would report.
func markNodeNotReady(t *testing.T, cluster *harness.Cluster, name string) {
	t.Helper()
	ctx := context.Background()
	node, err := cluster.Kube.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read node %s: %v", name, err)
	}
	node.Status.Conditions = []corev1.NodeCondition{{
		Type: corev1.NodeReady, Status: corev1.ConditionFalse,
		Reason: "NodeNotReady", LastTransitionTime: metav1.Now(),
	}}
	if _, err := cluster.Kube.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("mark node %s not ready: %v", name, err)
	}
}
