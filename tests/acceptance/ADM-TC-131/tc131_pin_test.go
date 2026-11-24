package admtc131

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/tests/acceptance/harness"
)

// databaseNodeName and otherNodeName are the two schedulable nodes the case
// installs onto. Only the first one is the host that holds the database: the
// file was written on the machine the command runs on, so that machine's node
// is the only node whose filesystem has it.
const (
	databaseNodeName = "kos-database-node"
	otherNodeName    = "kos-other-node"
)

// TestTC131InstallWithoutANodeFlagPinsTheControlPlaneToTheDatabaseHost covers
// the install entry's default path on a cluster where the choice matters: two
// schedulable nodes, a database on this host alone, and no
// --control-plane-node. The database is a hostPath, so a workload the scheduler
// is free to place on either node can start where the file is not and serve an
// empty database; the run has to find the node that is this host and pin the
// control plane to it (ADM-C-54, ADM-AC-70, ADM-TC-131).
func TestTC131InstallWithoutANodeFlagPinsTheControlPlaneToTheDatabaseHost(t *testing.T) {
	ctx := context.Background()
	cluster := harness.Start(t)
	// The node that is this host carries its host name as the hostname label and
	// a different name, so the pin proves the node's identity and not the label.
	registerNode(t, cluster, databaseNodeName, hostShortName(t))
	registerNode(t, cluster, otherNodeName, otherNodeName+"-host")
	serveNodeAgentRollout(t, cluster, 2)

	database := filepath.Join(t.TempDir(), "kos.db")
	output, err := exec.Command(buildManagementCLI(t),
		"install",
		"--kubeconfig", kubeconfigFrom(t, cluster.Config),
		"--definitions", harness.CRDDirectory(t),
		"--identity-database", database,
		"--settle-timeout", "60s",
		"--node-agent-timeout", "60s",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("install without --control-plane-node = %v\n%s", err, output)
	}
	password := oneTimePassword(t, output)

	deployment, err := cluster.Kube.AppsV1().Deployments(install.DefaultNamespace).Get(ctx, install.ControlPlaneName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read the control plane the installation delivered: %v", err)
	}
	if pinned := harness.RequiredNode(deployment.Spec.Template.Spec); pinned == "" {
		t.Fatalf("the control plane was left unpinned on a %d-node cluster: the pod can start on %s, where the database is not", 2, otherNodeName)
	}
	delivery, err := install.SeededDatabase(database, databaseNodeName)
	if err != nil {
		t.Fatalf("resolve the database the installation seeded: %v", err)
	}
	harness.AssertDatabaseDelivery(t, deployment, delivery)

	// The printed credential is the account the workload serves: it signs in
	// against the file the pin points at, which is what the pin is for.
	controlPlane := harness.OpenIdentityAt(t, delivery.Path, nil, nil)
	login, err := controlPlane.Service.Login(ctx, "owner", password)
	if err != nil {
		t.Fatalf("sign the first Owner in against the database the pin names: %v", err)
	}
	if !login.Session.MustChangePassword {
		t.Fatal("the first login was not required to change the one-time password")
	}
}

// registerNode creates one node with a hostname label, so a case can control
// which node a host name resolves to.
func registerNode(t *testing.T, cluster *harness.Cluster, name, hostname string) {
	t.Helper()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		Labels: map[string]string{corev1.LabelHostname: hostname},
	}}
	if _, err := cluster.Kube.CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{}); err != nil {
		t.Fatalf("register node %s: %v", name, err)
	}
}

// serveNodeAgentRollout stands in for the controller manager envtest does not
// run. The node agent is a DaemonSet, and the installation waits for its
// rollout before it creates the control plane (C-54); nothing writes that
// status unless the case does.
func serveNodeAgentRollout(t *testing.T, cluster *harness.Cluster, nodes int32) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			agent, err := cluster.Kube.AppsV1().DaemonSets(install.DefaultNamespace).Get(ctx, install.NodeAgentName, metav1.GetOptions{})
			if err != nil {
				continue
			}
			agent.Status.ObservedGeneration = agent.Generation
			agent.Status.DesiredNumberScheduled = nodes
			agent.Status.NumberReady = nodes
			agent.Status.UpdatedNumberScheduled = nodes
			agent.Status.NumberAvailable = nodes
			if _, err := cluster.Kube.AppsV1().DaemonSets(install.DefaultNamespace).UpdateStatus(ctx, agent, metav1.UpdateOptions{}); err != nil {
				continue
			}
		}
	}()
}

// hostShortName is this machine's name without its domain, the way a node
// registers itself.
func hostShortName(t *testing.T) string {
	t.Helper()
	name, err := os.Hostname()
	if err != nil {
		t.Fatalf("read this host's name: %v", err)
	}
	if name == "" {
		t.Fatal("this host reports no name")
	}
	if cut := strings.IndexByte(name, '.'); cut > 0 {
		return name[:cut]
	}
	return name
}

// oneTimePassword reads the credential the installation printed.
func oneTimePassword(t *testing.T, output []byte) string {
	t.Helper()
	for _, line := range strings.Split(string(output), "\n") {
		if value, found := strings.CutPrefix(line, "One-time password: "); found {
			return strings.TrimSpace(value)
		}
	}
	t.Fatalf("the installation printed no one-time password:\n%s", output)
	return ""
}

// buildManagementCLI builds the artifact an operator runs.
func buildManagementCLI(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "kosadm")
	build := exec.Command("go", "build", "-o", binary, "./cmd/kosadm")
	build.Dir = harness.RepoRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the management CLI: %v\n%s", err, output)
	}
	return binary
}

// kubeconfigFrom renders one rest configuration as a kubeconfig the CLI reads.
func kubeconfigFrom(t *testing.T, config *rest.Config) string {
	t.Helper()
	user := clientcmdapi.NewAuthInfo()
	user.ClientCertificateData = credential(t, config.CertData, config.CertFile)
	user.ClientKeyData = credential(t, config.KeyData, config.KeyFile)
	user.Token = config.BearerToken
	user.Username = config.Username
	user.Password = config.Password
	kubeconfig := clientcmdapi.NewConfig()
	kubeconfig.Clusters["acceptance"] = &clientcmdapi.Cluster{
		Server:                   config.Host,
		CertificateAuthorityData: credential(t, config.CAData, config.CAFile),
		InsecureSkipTLSVerify:    config.Insecure,
	}
	kubeconfig.AuthInfos["acceptance"] = user
	kubeconfig.Contexts["acceptance"] = &clientcmdapi.Context{Cluster: "acceptance", AuthInfo: "acceptance"}
	kubeconfig.CurrentContext = "acceptance"
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*kubeconfig, path); err != nil {
		t.Fatalf("write the kubeconfig: %v", err)
	}
	return path
}

// credential reads one credential the rest configuration holds inline or on
// disk.
func credential(t *testing.T, inline []byte, path string) []byte {
	t.Helper()
	if len(inline) > 0 || path == "" {
		return inline
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the credential %s: %v", path, err)
	}
	return content
}
