package install

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
)

// TestControlPlaneDeploymentCarriesThePackageVolume pins the delivery channel
// an installation puts on the control plane: the host directory an operator
// drops an extension package in, mounted read-only, and the flag that points
// the process at it (KOSD-C-17, KOSD-AC-72). The external form runs the
// workload in another cluster, so it carries the same channel there.
func TestControlPlaneDeploymentCarriesThePackageVolume(t *testing.T) {
	plans := map[string]Plan{
		"existing-cluster": {Mode: ModeExisting, Owner: "owner"},
		"bootstrap":        {Mode: ModeBootstrap, Owner: "owner", Nodes: 1},
		"external-control-plane": {
			Mode: ModeExternal, Owner: "owner",
			Managed: ManagedEndpoint{Server: "https://managed.example:6443", Authority: []byte("authority")},
		},
	}
	for name, plan := range plans {
		deployment := deploymentDocument(t, renderedControlPlane(t, plan))
		container := controlPlaneContainer(t, deployment)
		if args, _ := container["args"].([]any); len(args) != 2 ||
			args[0] != "-packages" || args[1] != PackageDirectory {
			t.Fatalf("%s: args = %v, want -packages %s", name, container["args"], PackageDirectory)
		}
		mounts, _ := container["volumeMounts"].([]any)
		if !hasPackageMount(mounts) {
			t.Fatalf("%s: volumeMounts = %v, want the package directory read-only", name, mounts)
		}
		volumes, _, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "volumes")
		if err != nil || !hasPackageVolume(volumes) {
			t.Fatalf("%s: volumes = %v (%v), want the host package directory", name, volumes, err)
		}
		if plan.Mode == ModeExternal && !hasCredentialMount(mounts) {
			t.Fatalf("%s: volumeMounts = %v, want the managed-cluster credential", name, mounts)
		}
	}
}

// TestControlPlaneDeploymentMatchesTheKosdManifest pins this package's copy of
// the control-plane workload to the manifest the kernel ships: an installation
// and a kubectl apply of kosd's deploy manifest must put one workload shape in
// the cluster. A change on the kernel side fails here instead of leaving two
// shapes to drift apart (ADM-C-54, KOSD-C-17).
func TestControlPlaneDeploymentMatchesTheKosdManifest(t *testing.T) {
	shipped := shippedKosdDeployment(t)
	rendered := deploymentDocument(t, renderedControlPlane(t, Plan{Mode: ModeExisting, Owner: "owner"}))
	renderedJSON, err := json.Marshal(rendered.Object)
	if err != nil {
		t.Fatalf("encode the rendered workload: %v", err)
	}
	shippedJSON, err := json.Marshal(shipped.Object)
	if err != nil {
		t.Fatalf("encode the shipped workload: %v", err)
	}
	if !bytes.Equal(renderedJSON, shippedJSON) {
		t.Fatalf("the rendered workload differs from kosd's deploy manifest:\nrendered: %s\nshipped:  %s",
			renderedJSON, shippedJSON)
	}
}

// TestControlPlaneDeploymentServesTheSeededDatabase pins the database delivery:
// an installation that seeds the first Owner mounts that file's directory into
// the workload and points the process at the same path, so the credential the
// installation prints signs in against the accounts it wrote (C-54, C-63). The
// external form runs the workload in another cluster and carries the same
// delivery there.
func TestControlPlaneDeploymentServesTheSeededDatabase(t *testing.T) {
	database := DatabaseDelivery{
		Path:  "/var/lib/kosd/kos.db",
		Node:  "node-a",
		Owner: DatabaseIdentity{UID: 1000, GID: 1001},
	}
	plans := map[string]Plan{
		"existing-cluster": {Mode: ModeExisting, Owner: "owner", Database: &database},
		"bootstrap":        {Mode: ModeBootstrap, Owner: "owner", Nodes: 1, Database: &database},
		"external-control-plane": {
			Mode: ModeExternal, Owner: "owner",
			Managed:  ManagedEndpoint{Server: "https://managed.example:6443", Authority: []byte("authority")},
			Database: &database,
		},
	}
	for name, plan := range plans {
		deployment := deploymentDocument(t, renderedControlPlane(t, plan))
		container := controlPlaneContainer(t, deployment)
		if !hasArgument(container, "-database", database.Path) {
			t.Fatalf("%s: args = %v, want -database %s", name, container["args"], database.Path)
		}
		// The package channel stays: a database delivery must not push it out.
		if !hasArgument(container, "-packages", PackageDirectory) {
			t.Fatalf("%s: args = %v, want -packages %s", name, container["args"], PackageDirectory)
		}
		mounts, _ := container["volumeMounts"].([]any)
		directory := filepath.Dir(database.Path)
		if !hasWritableMount(mounts, DatabaseVolume, directory) {
			t.Fatalf("%s: volumeMounts = %v, want %s writable at %s", name, mounts, DatabaseVolume, directory)
		}
		if !hasPackageMount(mounts) {
			t.Fatalf("%s: volumeMounts = %v, want the package directory read-only", name, mounts)
		}
		if !databaseMountPrecedesPackages(mounts) {
			t.Fatalf("%s: volumeMounts = %v, want the database directory mounted before its nested package directory", name, mounts)
		}
		volumes, _, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "volumes")
		if err != nil || !hasDatabaseVolume(volumes, directory) {
			t.Fatalf("%s: volumes = %v (%v), want the host's %s directory", name, volumes, err, directory)
		}
		context, _ := container["securityContext"].(map[string]any)
		if context["runAsUser"] != int64(1000) || context["runAsGroup"] != int64(1001) {
			t.Fatalf("%s: securityContext = %v, want the database owner's uid/gid 1000/1001", name, context)
		}
		// The database is a hostPath: it reads one node's filesystem, so the
		// workload has to start on the node that holds the file. Without this
		// pin a two-node cluster can place the pod on the other node, where the
		// file is absent and the control plane serves an empty database.
		if pinned := requiredNode(t, deployment); pinned != database.Node {
			t.Fatalf("%s: required node affinity = %q, want %q, the node that holds the file", name, pinned, database.Node)
		}
		// The pin is the node's identity, not a label: a node whose hostname
		// label differs from its name must still serve the pod.
		if selector, _, err := unstructured.NestedStringMap(deployment.Object, "spec", "template", "spec", "nodeSelector"); err != nil || len(selector) != 0 {
			t.Fatalf("%s: nodeSelector = %v (%v), want none: the workload binds the node by name", name, selector, err)
		}
	}
}

// TestControlPlaneDeploymentBindsTheNodeByIdentity pins the identity binding:
// the workload is pinned by node name, not by a hostname label the node may not
// carry. A node whose name differs from its kubernetes.io/hostname label is
// still the node that holds the file, so it must be the node the pod runs on.
func TestControlPlaneDeploymentBindsTheNodeByIdentity(t *testing.T) {
	database := DatabaseDelivery{
		Path:  "/var/lib/kosd/kos.db",
		Node:  "node-a",
		Owner: DatabaseIdentity{UID: 1000, GID: 1000},
	}
	plan := Plan{Mode: ModeExisting, Owner: "owner", Database: &database}
	deployment := deploymentDocument(t, renderedControlPlane(t, plan))
	// The hostname label is deliberately absent from the assertions: binding
	// through it would match nothing on a node whose label differs.
	if pinned := requiredNode(t, deployment); pinned != "node-a" {
		t.Fatalf("required node affinity = %q, want node-a: the pin must follow the node's identity", pinned)
	}
	if selector, _, err := unstructured.NestedStringMap(deployment.Object, "spec", "template", "spec", "nodeSelector"); err != nil || len(selector) != 0 {
		t.Fatalf("nodeSelector = %v (%v), want none: a hostname label is not the node identity", selector, err)
	}
}

// TestControlPlaneDeploymentPinGoesThroughTheScheduler pins how the workload is
// held on that node: a required node affinity, not a nodeName field. nodeName
// binds a pod without the scheduler, so a drain would evict it and the
// replacement would be placed straight back onto the node the operator is
// taking out (KOSD-C-65). A regression to nodeName fails here.
func TestControlPlaneDeploymentPinGoesThroughTheScheduler(t *testing.T) {
	database := DatabaseDelivery{
		Path:  "/var/lib/kosd/kos.db",
		Node:  "node-a",
		Owner: DatabaseIdentity{UID: 1000, GID: 1000},
	}
	deployment := deploymentDocument(t, renderedControlPlane(t, Plan{Mode: ModeExisting, Owner: "owner", Database: &database}))
	if bound, _, err := unstructured.NestedString(deployment.Object, "spec", "template", "spec", "nodeName"); err != nil || bound != "" {
		t.Fatalf("nodeName = %q (%v), want none: a nodeName bypasses the scheduler and defeats the drain", bound, err)
	}
	terms, found, err := unstructured.NestedSlice(deployment.Object,
		"spec", "template", "spec", "affinity", "nodeAffinity",
		"requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms")
	if err != nil || !found || len(terms) == 0 {
		t.Fatalf("node affinity terms = %v (%v), want a required node affinity", terms, err)
	}
	if pinned := requiredNode(t, deployment); pinned != "node-a" {
		t.Fatalf("required node affinity = %q, want node-a", pinned)
	}
}

// TestControlPlaneDeploymentKeepsThePinAcrossAnUpgrade pins the upgrade side of
// the pin: an upgrade rewrites the workload, and the rewritten pod has to start
// on the same node as the one it replaced. If the pin were dropped here, a
// two-node cluster could reschedule the new pod onto the other node, where the
// database is absent, and the accounts would be gone even though the
// installation pinned the first pod correctly.
func TestControlPlaneDeploymentKeepsThePinAcrossAnUpgrade(t *testing.T) {
	database := DatabaseDelivery{
		Path:  "/var/lib/kosd/kos.db",
		Node:  "node-a",
		Owner: DatabaseIdentity{UID: 1000, GID: 1000},
	}
	// The install and the upgrade render the same plan shape; an upgrade that
	// dropped the delivery would lose both the file and the pin.
	plan := Plan{Mode: ModeExisting, Owner: "owner", Database: &database}
	deployment := deploymentDocument(t, renderedControlPlane(t, plan))
	if pinned := requiredNode(t, deployment); pinned != "node-a" {
		t.Fatalf("required node affinity = %q, want node-a: the rewritten pod has to stay on the node that holds the file", pinned)
	}
	container := controlPlaneContainer(t, deployment)
	if !hasArgument(container, "-database", database.Path) {
		t.Fatalf("args = %v, want -database %s", container["args"], database.Path)
	}
}

// TestControlPlaneDeploymentLeavesUnpinnedWithoutANode pins the compatible
// fallback: an installation that does not name a node renders no pin, so the
// command keeps working where the operator does not have to choose one.
func TestControlPlaneDeploymentLeavesUnpinnedWithoutANode(t *testing.T) {
	database := DatabaseDelivery{
		Path:  "/var/lib/kosd/kos.db",
		Owner: DatabaseIdentity{UID: 1000, GID: 1000},
	}
	deployment := deploymentDocument(t, renderedControlPlane(t, Plan{Mode: ModeExisting, Owner: "owner", Database: &database}))
	if pinned := requiredNode(t, deployment); pinned != "" {
		t.Fatalf("required node affinity = %q, want none: a delivery without a node must not pin the workload", pinned)
	}
}

// TestControlPlaneDeploymentLeavesTheImageDatabaseAlone pins the other half: an
// installation that seeds no database renders no delivery, so the workload
// keeps the image's own database. This is the shape kosd ships, and the
// byte-for-byte comparison above depends on it.
func TestControlPlaneDeploymentLeavesTheImageDatabaseAlone(t *testing.T) {
	deployment := deploymentDocument(t, renderedControlPlane(t, Plan{Mode: ModeExisting, Owner: "owner"}))
	container := controlPlaneContainer(t, deployment)
	if hasArgument(container, "-database", "") || hasArgument(container, "-database", "/var/lib/kosd/kos.db") {
		t.Fatalf("args = %v, want no -database override", container["args"])
	}
	if _, ok := container["securityContext"]; ok {
		t.Fatalf("securityContext = %v, want the image's own identity", container["securityContext"])
	}
	volumes, _, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "volumes")
	if err != nil || hasDatabaseVolume(volumes, "/var/lib/kosd") {
		t.Fatalf("volumes = %v (%v), want no database volume", volumes, err)
	}
	if pinned := requiredNode(t, deployment); pinned != "" {
		t.Fatalf("required node affinity = %q, want none: a workload on the image's own database is not pinned to a node", pinned)
	}
}

// requiredNode returns the node a rendered workload's required node affinity
// selects by name, or the empty string when the workload carries no such pin.
func requiredNode(t *testing.T, deployment *unstructured.Unstructured) string {
	t.Helper()
	terms, found, err := unstructured.NestedSlice(deployment.Object,
		"spec", "template", "spec", "affinity", "nodeAffinity",
		"requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms")
	if err != nil || !found {
		return ""
	}
	for _, raw := range terms {
		term, _ := raw.(map[string]any)
		fields, _ := term["matchFields"].([]any)
		for _, rawField := range fields {
			field, _ := rawField.(map[string]any)
			values, _ := field["values"].([]any)
			if field["key"] == "metadata.name" && field["operator"] == "In" && len(values) == 1 {
				node, _ := values[0].(string)
				return node
			}
		}
	}
	return ""
}

// hasArgument reports whether the container carries flag with exactly value.
func hasArgument(container map[string]any, flag, value string) bool {
	args, _ := container["args"].([]any)
	for index := 0; index+1 < len(args); index++ {
		if args[index] == flag && args[index+1] == value {
			return true
		}
	}
	return false
}

// hasWritableMount reports whether one mount carries name writable at path.
func hasWritableMount(mounts []any, name, path string) bool {
	for _, raw := range mounts {
		mount, _ := raw.(map[string]any)
		if mount["name"] == name && mount["mountPath"] == path && mount["readOnly"] != true {
			return true
		}
	}
	return false
}

// hasDatabaseVolume reports whether one volume is the host database directory.
func hasDatabaseVolume(volumes []any, directory string) bool {
	for _, raw := range volumes {
		volume, _ := raw.(map[string]any)
		hostPath, _ := volume["hostPath"].(map[string]any)
		if volume["name"] == DatabaseVolume && hostPath["path"] == directory && hostPath["type"] == "Directory" {
			return true
		}
	}
	return false
}

// databaseMountPrecedesPackages reports whether the database directory is
// mounted before the package directory nested inside it.
func databaseMountPrecedesPackages(mounts []any) bool {
	database, packages := -1, -1
	for index, raw := range mounts {
		mount, _ := raw.(map[string]any)
		switch mount["name"] {
		case DatabaseVolume:
			database = index
		case PackageVolume:
			packages = index
		}
	}
	return database >= 0 && packages > database
}

// renderedControlPlane returns the documents one plan puts on the control-plane
// side: RenderControlPlane for the external form, Render for every other form.
func renderedControlPlane(t *testing.T, plan Plan) []Document {
	t.Helper()
	if plan.Mode == ModeExternal {
		documents, err := RenderControlPlane(plan)
		if err != nil {
			t.Fatalf("render the control plane of the external form: %v", err)
		}
		return documents
	}
	documents, err := Render(plan)
	if err != nil {
		t.Fatalf("render the %s form: %v", plan.Mode, err)
	}
	return documents
}

// deploymentDocument returns the control-plane Deployment from one render.
func deploymentDocument(t *testing.T, documents []Document) *unstructured.Unstructured {
	t.Helper()
	for _, document := range documents {
		if document.GVR.Resource == "deployments" && document.Object.GetName() == ControlPlaneName {
			return document.Object
		}
	}
	t.Fatalf("rendered no %s Deployment", ControlPlaneName)
	return nil
}

// controlPlaneContainer returns the one container of the workload.
func controlPlaneContainer(t *testing.T, deployment *unstructured.Unstructured) map[string]any {
	t.Helper()
	containers, _, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	if err != nil || len(containers) != 1 {
		t.Fatalf("containers = %v (%v), want one", containers, err)
	}
	container, ok := containers[0].(map[string]any)
	if !ok || container["name"] != ControlPlaneName {
		t.Fatalf("container = %v, want %s", containers[0], ControlPlaneName)
	}
	return container
}

// hasPackageMount reports whether one mount carries the package directory
// read-only.
func hasPackageMount(mounts []any) bool {
	for _, raw := range mounts {
		mount, _ := raw.(map[string]any)
		if mount["name"] == PackageVolume && mount["mountPath"] == PackageDirectory && mount["readOnly"] == true {
			return true
		}
	}
	return false
}

// hasPackageVolume reports whether one volume is the host package directory.
func hasPackageVolume(volumes []any) bool {
	for _, raw := range volumes {
		volume, _ := raw.(map[string]any)
		hostPath, _ := volume["hostPath"].(map[string]any)
		if volume["name"] == PackageVolume && hostPath["path"] == PackageDirectory && hostPath["type"] == "DirectoryOrCreate" {
			return true
		}
	}
	return false
}

// hasCredentialMount reports whether one mount carries the managed-cluster
// credential the external form reads.
func hasCredentialMount(mounts []any) bool {
	for _, raw := range mounts {
		mount, _ := raw.(map[string]any)
		if mount["name"] == "managed-credential" && mount["mountPath"] == "/etc/kos/managed" && mount["readOnly"] == true {
			return true
		}
	}
	return false
}

// shippedKosdDeployment reads the control-plane Deployment kosd ships for
// operators who apply the manifests themselves. It lives in the kosd module the
// release pins, so a new pin brings the changed manifest here.
func shippedKosdDeployment(t *testing.T) *unstructured.Unstructured {
	t.Helper()
	output, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/tsic404/kosd").Output()
	if err != nil {
		t.Fatalf("locate the kosd module: %v", err)
	}
	directory := strings.TrimSpace(string(output))
	content, err := os.ReadFile(filepath.Join(directory, "config", "deploy", "kos-control-plane.yaml"))
	if err != nil {
		t.Fatalf("read kosd's deploy manifest: %v", err)
	}
	decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(content), 4096)
	for {
		var object map[string]any
		err := decoder.Decode(&object)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode kosd's deploy manifest: %v", err)
		}
		manifest := &unstructured.Unstructured{Object: object}
		if manifest.GetKind() == "Deployment" && manifest.GetName() == ControlPlaneName {
			return manifest
		}
	}
	t.Fatalf("kosd's deploy manifest carries no %s Deployment", ControlPlaneName)
	return nil
}
