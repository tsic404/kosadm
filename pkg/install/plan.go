package install

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/tsic404/kosd/api"
)

// Names the installation writes. The control-plane workload and its service
// share one name so an operator reads the same name in the namespace and in the
// rendered manifest (C-54).
const (
	// DefaultNamespace is the namespace an installation creates for itself. An
	// application's declared namespace is never created by installation (C-54).
	DefaultNamespace = api.SystemNamespace
	// ControlPlaneName is the control-plane workload and its service.
	ControlPlaneName = "kos-control-plane"
	// InstallerServiceAccount is the identity the install and uninstall run as.
	InstallerServiceAccount = "kos-installer"
	// ControlPlaneServiceAccount is the identity the control plane runs as.
	ControlPlaneServiceAccount = "kos-control-plane"
	// NodeAgentName is the per-node agent's workload, service account and role.
	// One instance runs per node and writes only that node's device fact (C-54).
	NodeAgentName = "kos-node"
	// NodeAgentServiceAccount is the identity the node agent runs as.
	NodeAgentServiceAccount = "kos-node"
	// DefaultImage is the control-plane image a plan that names none renders.
	// It is pinned rather than floating, so an installation is reproducible.
	DefaultImage = "ghcr.io/tsic404/kos-control-plane:v0.1.0"
	// DefaultNodeAgentImage is the node-agent image a plan that names none
	// renders. It is pinned for the same reason as DefaultImage.
	DefaultNodeAgentImage = "ghcr.io/tsic404/kos-node:v0.1.0"
	// SingleNodeNotice is the availability an installation promises on one
	// node: no high availability and no automatic migration (C-54, F13).
	SingleNodeNotice = "单节点安装是退化形态：控制面为单副本，节点故障后工作负载不会自动迁移，不承诺高可用。"
	// ManagedCredentialSecret is the secret the external form writes into the
	// control-plane cluster: the credential to the managed cluster (C-14 keeps
	// credentials on the control-plane side).
	ManagedCredentialSecret = "kos-managed-cluster"
	// ManagedCredentialKey is the key inside that secret holding the kubeconfig.
	ManagedCredentialKey = "kubeconfig"
	// ManagedCredentialExpiryKey is the key holding when the token stops
	// authenticating, so a control plane degrades with a readable reason before
	// it loses the managed cluster (C-14).
	ManagedCredentialExpiryKey = "expiration"
	// DefaultAddress is where an external-form control plane answers on its own
	// host; the first-Owner claim is served from there (C-68).
	DefaultAddress = "http://127.0.0.1:8080"
)

// Mode is an installation form (C-54).
type Mode string

const (
	// ModeExisting installs into a cluster that already runs.
	ModeExisting Mode = "existing-cluster"
	// ModeBootstrap brings the run kernel up on bare hosts first.
	ModeBootstrap Mode = "bootstrap"
	// ModeExternal runs the control plane outside the cluster it reconciles:
	// the managed cluster keeps the object definitions and the identity the
	// control plane acts as, and the control-plane cluster keeps the workload.
	ModeExternal Mode = "external-control-plane"
)

// ManagedEndpoint is the managed cluster as a control plane elsewhere reaches
// it (C-54 external form).
type ManagedEndpoint struct {
	// Server is the managed cluster's API server address.
	Server string
	// Authority is the PEM bundle a client verifies that server with.
	Authority []byte
}

// Plan is one installation. It is data: rendering and the install run both
// read it and neither mutates it.
type Plan struct {
	// Mode is the installation form.
	Mode Mode
	// Namespace is where the installation keeps its own objects; empty selects
	// DefaultNamespace.
	Namespace string
	// Owner is the login name of the first Owner the install creates (C-54,
	// C-63).
	Owner string
	// Nodes is the declared node count of a bootstrap install; the existing
	// form declares none.
	Nodes int
	// Applications are the namespaces the applications to install declare.
	// Installation verifies each exists and never creates one (C-54).
	Applications []string
	// Image is the control-plane image; empty selects DefaultImage.
	Image string
	// NodeAgentImage is the node-agent image; empty selects
	// DefaultNodeAgentImage.
	NodeAgentImage string
	// Managed is how the control plane reaches the managed cluster; the
	// external form requires it and the other forms leave it empty.
	Managed ManagedEndpoint
	// Address is where the control plane answers on its own host; empty selects
	// DefaultAddress. The external form prints it with the claim instruction
	// (C-68).
	Address string
}

// Normalize fills the defaults and refuses a plan the install cannot honour.
func (p Plan) Normalize() (Plan, error) {
	switch p.Mode {
	case ModeExisting, ModeBootstrap, ModeExternal:
	default:
		return Plan{}, fmt.Errorf("install mode %q is not an installation form", p.Mode)
	}
	if p.Namespace == "" {
		p.Namespace = DefaultNamespace
	}
	if problems := validation.IsDNS1123Label(p.Namespace); len(problems) > 0 {
		return Plan{}, fmt.Errorf("namespace %q is not a DNS label: %s", p.Namespace, problems[0])
	}
	if strings.TrimSpace(p.Owner) == "" {
		return Plan{}, fmt.Errorf("installation names no first Owner")
	}
	if p.Mode == ModeBootstrap && p.Nodes < 1 {
		return Plan{}, fmt.Errorf("bootstrap install declares %d nodes; it must declare at least one", p.Nodes)
	}
	if p.Mode == ModeExisting && p.Nodes != 0 {
		return Plan{}, fmt.Errorf("the existing-cluster form declares no nodes, got %d", p.Nodes)
	}
	if p.Mode == ModeExternal {
		if p.Nodes != 0 {
			return Plan{}, fmt.Errorf("the external-control-plane form declares no nodes, got %d", p.Nodes)
		}
		if strings.TrimSpace(p.Managed.Server) == "" {
			return Plan{}, fmt.Errorf("the external-control-plane form needs the managed cluster's address")
		}
		// The credential the control plane gets must verify the managed
		// cluster; without a certificate authority it cannot, and the
		// installation would report success over a connection that fails at
		// the first request (C-54).
		if len(p.Managed.Authority) == 0 {
			return Plan{}, fmt.Errorf("the external-control-plane form needs the managed cluster's certificate authority")
		}
		if p.Address == "" {
			p.Address = DefaultAddress
		}
	}
	if p.Image == "" {
		p.Image = DefaultImage
	}
	if p.NodeAgentImage == "" {
		p.NodeAgentImage = DefaultNodeAgentImage
	}
	seen := map[string]bool{}
	applications := make([]string, 0, len(p.Applications))
	for _, namespace := range p.Applications {
		if problems := validation.IsDNS1123Label(namespace); len(problems) > 0 {
			return Plan{}, fmt.Errorf("declared namespace %q is not a DNS label: %s", namespace, problems[0])
		}
		if namespace == p.Namespace {
			return Plan{}, fmt.Errorf("the installation namespace is not an application namespace")
		}
		if seen[namespace] {
			continue
		}
		seen[namespace] = true
		applications = append(applications, namespace)
	}
	sort.Strings(applications)
	p.Applications = applications
	return p, nil
}

// Commitment is what an installation promises about availability (C-54, F13).
// It is one value for every node count: KOS never runs a second control-plane
// replica and never migrates a workload to another node.
type Commitment struct {
	// Replicas is the control-plane replica count.
	Replicas int
	// AutomaticMigration reports whether a failed node triggers KOS migration.
	AutomaticMigration bool
	// Notice is the text an installation shows about the promise.
	Notice string
}

// Commitment reports the availability promise of this plan. The replica count
// and the absence of migration are the same for every form; only the notice
// differs, because a bootstrap install knows its node count and an existing
// cluster does not.
func (p Plan) Commitment() Commitment {
	commitment := Commitment{Replicas: 1, AutomaticMigration: false}
	switch {
	case p.Mode == ModeBootstrap && p.Nodes == 1:
		commitment.Notice = SingleNodeNotice
	case p.Mode == ModeBootstrap:
		commitment.Notice = fmt.Sprintf(
			"安装声明 %d 个节点；控制面为单副本，节点故障后 KOS 不迁移工作负载，不承诺高可用。", p.Nodes)
	default:
		commitment.Notice = "控制面为单副本；KOS 不随节点故障迁移工作负载，不承诺高可用。"
	}
	return commitment
}

// SubjectUsers returns the API server identities the installation's components
// run as, inside namespace: the installer, the control plane and the node
// agent. The install invariant judges writes by these principals (C-15).
func SubjectUsers(namespace string) []string {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	prefix := "system:serviceaccount:" + namespace + ":"
	return []string{
		prefix + InstallerServiceAccount,
		prefix + ControlPlaneServiceAccount,
		prefix + NodeAgentServiceAccount,
	}
}
