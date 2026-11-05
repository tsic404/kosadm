// Package admtc96 verifies that a cluster upgrade has no path that force-drains a
// node holding a node-local application: the request that asks for one is
// refused, the drain refuses while the application still runs, and the
// workload and its data stay where they are (ADM-AC-48, ADM-TC-96).
package admtc96

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/tsic404/kosadm/pkg/maintenance"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
	"github.com/tsic404/kosd/pkg/app"
	"github.com/tsic404/kosd/pkg/controlplane"
)

const (
	appsNamespace = "apps"
	application   = "shop"
	nodeName      = "node-a"
	volumeName    = "data"
	storageClass  = "local"
	claimName     = "data"
	ownerName     = "owner"
	downtime      = "15m"
)

// shopTemplate renders the Deployment of the node-local application.
const shopTemplate = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Name }}
  namespace: {{ .Namespace }}
spec:
  replicas: 2
  selector:
    matchLabels:
      app: {{ .Name }}
  template:
    metadata:
      labels:
        app: {{ .Name }}
    spec:
      containers:
        - name: app
          image: nginx:1.27
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: {{ index .Volumes "data" }}
`

// TestTC96NoForceDrain checks that neither the maintenance requests nor the
// drain offer a forced path (C-55, C-65).
func TestTC96NoForceDrain(t *testing.T) {
	cluster := harness.Start(t)
	cluster.EnsureNamespace(t, appsNamespace)
	cluster.StorageClass(t, storageClass, "local.example", false, "Delete", nil)
	cluster.CreateNode(t, nodeName, harness.DefaultKubeletVersion)
	cluster.CreateClaim(t, appsNamespace, claimName, storageClass)
	cluster.CreateIn(t, appsNamespace, boundVolume())
	cluster.InstallDefinition(t, harness.TemplateDefinition(t, application, shopTemplate, nil))

	gateway := cluster.StartControlPlane(t, func(config *controlplane.Config) {
		config.Maintenance = &maintenance.Service{
			Dynamic: cluster.Dynamic,
			Writer:  cluster.Writer(),
			Pods:    maintenance.Cluster{Kube: cluster.Kube},
			Auditor: config.Auditor,
		}
	})
	manifest := harness.ApplicationManifest(application, harness.Namespace, application, appsNamespace, nil, []string{volumeName})
	manifest["spec"].(map[string]any)["replicas"] = int64(2)
	if status, body := gateway.Call(t, http.MethodPost, "/api/v1/resources/Application", manifest, nil); status != http.StatusAccepted {
		t.Fatalf("create the application: status %d, body %v", status, body)
	}
	controller := cluster.ApplicationController(t, app.Options{})
	harness.Eventually(t, 30*time.Second, "the application reaches Ready", func() bool {
		cluster.ApplicationRound(t, controller, application)
		return cluster.ApplicationPhase(t, application) == app.PhaseReady
	})
	cluster.CreatePod(t, appsNamespace, "shop-abc-1", nodeName, "shop")
	claimBefore := cluster.Claim(t, appsNamespace, claimName).GetResourceVersion()

	// A maintenance start that asks for a forced drain names a field the model
	// does not define, so it is refused before anything moves (C-55).
	status, body := gateway.Call(t, http.MethodPost, "/api/v1/node-maintenance", map[string]any{
		"node": nodeName, "downtime": downtime, "force": true,
	}, nil)
	if status != http.StatusBadRequest || failureCode(t, body) != controlplane.CodeInvalidRequest {
		t.Fatalf("a forced maintenance start: status %d, body %v, want 400 %s", status, body, controlplane.CodeInvalidRequest)
	}
	if cluster.NodeUnschedulable(t, nodeName) {
		t.Fatal("the refused request cordoned the node")
	}

	// The cordon itself is accepted; the node-local application has not stopped
	// yet, so the drain waits for it instead of forcing it out (C-65).
	status, body = gateway.Call(t, http.MethodPost, "/api/v1/node-maintenance", map[string]any{
		"node": nodeName, "downtime": downtime,
	}, nil)
	if status != http.StatusAccepted {
		t.Fatalf("start the maintenance: status %d, body %v", status, body)
	}

	status, body = gateway.Call(t, http.MethodPost, "/api/v1/node-maintenance/drain", map[string]any{
		"node": nodeName, "force": true,
	}, nil)
	if status != http.StatusBadRequest || failureCode(t, body) != controlplane.CodeInvalidRequest {
		t.Fatalf("a forced drain: status %d, body %v, want 400 %s", status, body, controlplane.CodeInvalidRequest)
	}

	status, body = gateway.Call(t, http.MethodPost, "/api/v1/node-maintenance/drain", map[string]any{"node": nodeName}, nil)
	if status != http.StatusConflict || failureCode(t, body) != controlplane.CodeConflict {
		t.Fatalf("a drain before the stop: status %d, body %v, want 409 %s", status, body, controlplane.CodeConflict)
	}
	if message := failureMessage(t, body); !contains(message, harness.Namespace+"/"+application) {
		t.Fatalf("the refusal names %q, want the application %s/%s", message, harness.Namespace, application)
	}

	// Nothing moved: the workload still runs, its pod is still on the node, and
	// the claim holding the data is untouched (C-55).
	if replicas := cluster.WorkloadReplicas(t, "Deployment", appsNamespace, application); replicas != 2 {
		t.Fatalf("workload replicas = %d after the refused drain, want 2", replicas)
	}
	if !cluster.PodExists(t, appsNamespace, "shop-abc-1") {
		t.Fatal("the refused drain removed the node-local application's pod")
	}
	if got := cluster.Claim(t, appsNamespace, claimName).GetResourceVersion(); got != claimBefore {
		t.Fatalf("the claim holding the data changed: %s -> %s", claimBefore, got)
	}
	if node := cluster.VolumeNode(t, appsNamespace, volumeName); node != nodeName {
		t.Fatalf("the data moved to node %q, want %q", node, nodeName)
	}
}

// boundVolume is the node-local volume whose data sits on the case's node.
func boundVolume() *unstructured.Unstructured {
	object := harness.Volume(appsNamespace, volumeName, map[string]any{
		"owner":        ownerName,
		"mobility":     "node-local",
		"capacity":     "1Gi",
		"storageClass": storageClass,
	})
	object.Object["status"] = map[string]any{"node": nodeName}
	return object
}

// failureCode reads the stable code of one error response.
func failureCode(t *testing.T, body map[string]any) string {
	t.Helper()
	failure, _ := body["error"].(map[string]any)
	code, _ := failure["code"].(string)
	return code
}

// failureMessage reads the user-facing message of one error response.
func failureMessage(t *testing.T, body map[string]any) string {
	t.Helper()
	failure, _ := body["error"].(map[string]any)
	message, _ := failure["message"].(string)
	return message
}

// contains reports whether value names want.
func contains(value, want string) bool {
	return strings.Contains(value, want)
}
