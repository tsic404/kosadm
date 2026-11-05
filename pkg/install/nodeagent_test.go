package install

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// TestNodeAgentWaitsForAnObservedRollout pins the node-agent wait: a freshly
// created DaemonSet reports no status at all, and its zeros must not read as a
// finished rollout (C-54).
func TestNodeAgentWaitsForAnObservedRollout(t *testing.T) {
	client := fake.NewSimpleClientset()
	var reads int32
	client.PrependReactor("get", "daemonsets", func(k8stesting.Action) (bool, runtime.Object, error) {
		agent := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{
			Name: NodeAgentName, Namespace: DefaultNamespace, Generation: 1,
		}}
		if atomic.AddInt32(&reads, 1) > 1 {
			// The controller has seen the object and scheduled it on two nodes.
			agent.Status = appsv1.DaemonSetStatus{ObservedGeneration: 1, DesiredNumberScheduled: 2, NumberReady: 2}
		}
		return true, agent, nil
	})
	runner := &Installer{
		config: Config{Kube: client, NodeAgentTimeout: 5 * time.Second},
		plan:   Plan{Namespace: DefaultNamespace, NodeAgentImage: "example.test/kos-node:v1"},
	}
	if err := runner.waitNodeAgent(context.Background()); err != nil {
		t.Fatalf("wait for the node agent: %v", err)
	}
	if reads := atomic.LoadInt32(&reads); reads != 2 {
		t.Fatalf("read the node agent %d times, want the unobserved status to keep it waiting", reads)
	}
}

// TestNodeAgentFailsWhenTheRolloutNeverCompletes pins the other direction: a
// node whose agent cannot start keeps the installation from reporting the third
// artifact as delivered.
func TestNodeAgentFailsWhenTheRolloutNeverCompletes(t *testing.T) {
	agent := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: NodeAgentName, Namespace: DefaultNamespace, Generation: 1},
		Status:     appsv1.DaemonSetStatus{ObservedGeneration: 1, DesiredNumberScheduled: 2, NumberReady: 0},
	}
	runner := &Installer{
		config: Config{Kube: fake.NewSimpleClientset(agent), NodeAgentTimeout: 300 * time.Millisecond},
		plan:   Plan{Namespace: DefaultNamespace, NodeAgentImage: "example.test/kos-node:v1"},
	}
	err := runner.waitNodeAgent(context.Background())
	if err == nil {
		t.Fatal("the wait passed with no ready node agent")
	}
	if !strings.Contains(err.Error(), "ready on 0 of 2 nodes") || !strings.Contains(err.Error(), "example.test/kos-node:v1") {
		t.Fatalf("error = %v, want the ready count and the image named", err)
	}
}

// TestNodeAgentWaitSkipsAClusterWithoutControllers pins the explicit skip: a
// cluster that runs no workload controller cannot observe a rollout.
func TestNodeAgentWaitSkipsAClusterWithoutControllers(t *testing.T) {
	runner := &Installer{
		config: Config{Kube: fake.NewSimpleClientset(), NoNodeAgentController: true},
		plan:   Plan{Namespace: DefaultNamespace},
	}
	if err := runner.waitNodeAgent(context.Background()); err != nil {
		t.Fatalf("wait without a controller: %v", err)
	}
}
