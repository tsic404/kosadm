package install

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/tsic404/kosd/api"
	"github.com/tsic404/kosd/pkg/markers"
	"github.com/tsic404/kosd/pkg/write"
)

// ManagedTokenLifetime is what an installation asks the managed cluster for. The
// cluster may shorten it, so the issued instant is what the credential records
// rather than this value.
const ManagedTokenLifetime = 10 * 365 * 24 * time.Hour

// CredentialRenewLead is how long before it stops authenticating a credential
// reports that it should be renewed (C-14).
const CredentialRenewLead = 30 * 24 * time.Hour

// CredentialState is how a stored credential stands at one instant.
type CredentialState string

const (
	// CredentialUsable means the token authenticates and has time left.
	CredentialUsable CredentialState = "usable"
	// CredentialRenewSoon means the token still authenticates but expires inside
	// the lead, so the credential should be renewed now.
	CredentialRenewSoon CredentialState = "renew-soon"
	// CredentialExpired means the token no longer authenticates.
	CredentialExpired CredentialState = "expired"
)

// Credential is the managed-cluster credential an external installation wrote.
type Credential struct {
	// Cluster is the managed cluster as the credential reaches it.
	Cluster *Cluster
	// ExpiresAt is when the token stops authenticating; zero when the
	// installation recorded no expiry.
	ExpiresAt time.Time
}

// State reports how the credential stands at instant (C-14). A credential with
// no recorded expiry is reported usable, because the cluster then gave no
// deadline to act on.
func (c Credential) State(at time.Time) CredentialState {
	if c.ExpiresAt.IsZero() {
		return CredentialUsable
	}
	switch {
	case !at.Before(c.ExpiresAt):
		return CredentialExpired
	case !at.Add(CredentialRenewLead).Before(c.ExpiresAt):
		return CredentialRenewSoon
	default:
		return CredentialUsable
	}
}

// Notice is the readable form of State: empty while the credential is
// comfortably valid, otherwise what to do about it (C-14).
func (c Credential) Notice(at time.Time) string {
	switch c.State(at) {
	case CredentialExpired:
		return fmt.Sprintf("到被管集群的凭据已于 %s 过期，控制面无法再读写该集群；请用 RenewManagedCredential 续期或重跑安装。",
			c.ExpiresAt.UTC().Format(time.RFC3339))
	case CredentialRenewSoon:
		return fmt.Sprintf("到被管集群的凭据将于 %s 过期；请用 RenewManagedCredential 续期。",
			c.ExpiresAt.UTC().Format(time.RFC3339))
	default:
		return ""
	}
}

// ExpiryError reports a credential that no longer authenticates, so the control
// plane says what to do instead of failing every read and write anonymously.
type ExpiryError struct {
	// ExpiresAt is when the token stopped authenticating.
	ExpiresAt time.Time
}

// Error implements error.
func (e *ExpiryError) Error() string {
	return fmt.Sprintf("the managed-cluster credential expired at %s; renew it or re-run the installation",
		e.ExpiresAt.UTC().Format(time.RFC3339))
}

// ManagedToken mints the credential the control plane authenticates with in the
// managed cluster and reports when it stops authenticating. The token names the
// control-plane service account, so the control plane acts with the permissions
// the installation granted it and never with the installer's own rights (C-04,
// C-14).
func ManagedToken(ctx context.Context, kube kubernetes.Interface, namespace string) (string, time.Time, error) {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	lifetime := int64(ManagedTokenLifetime / time.Second)
	issued, err := kube.CoreV1().ServiceAccounts(namespace).CreateToken(ctx, ControlPlaneServiceAccount,
		&authv1.TokenRequest{Spec: authv1.TokenRequestSpec{ExpirationSeconds: &lifetime}}, metav1.CreateOptions{})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("mint the managed-cluster credential: %w", err)
	}
	if issued.Status.Token == "" {
		return "", time.Time{}, fmt.Errorf("the managed cluster issued an empty credential")
	}
	// The cluster caps the lifetime on its own, so its answer is the expiry that
	// counts; a request for ten years is not a grant of ten years.
	return issued.Status.Token, issued.Status.ExpirationTimestamp.UTC(), nil
}

// Cluster is one cluster's clients plus the resource names a writer resolves
// kinds with. The external form holds two of them: the managed cluster, where
// the KOS objects live, and the control-plane cluster, where the workload runs.
type Cluster struct {
	// Config is the rest configuration the clients were built from.
	Config *rest.Config
	// Dynamic reads and writes every served class.
	Dynamic dynamic.Interface
	// Kube reads namespaces, secrets and permissions.
	Kube kubernetes.Interface
	// APIExtensions writes and reads the object definitions.
	APIExtensions apiextensionsclientset.Interface
	// Resources maps a kind to the resource name the API server serves it
	// under, which the write path needs to address a class by kind.
	Resources map[schema.GroupKind]string
}

// NewCluster builds the clients of one cluster and discovers the resource names
// it serves.
func NewCluster(ctx context.Context, config *rest.Config) (*Cluster, error) {
	if config == nil {
		return nil, errors.New("a cluster needs a rest configuration")
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("build the dynamic client: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("build the kubernetes client: %w", err)
	}
	extensionsClient, err := apiextensionsclientset.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("build the apiextensions client: %w", err)
	}
	cluster := &Cluster{
		Config:        config,
		Dynamic:       dynamicClient,
		Kube:          kubeClient,
		APIExtensions: extensionsClient,
	}
	resources, err := discoverResources(ctx, extensionsClient, kubeClient)
	if err != nil {
		return nil, err
	}
	cluster.Resources = resources
	return cluster, nil
}

// discoverResources reads the resource names of the kernel classes the cluster
// serves and of the native classes a rendered application writes (C-04, C-32).
func discoverResources(ctx context.Context, extensions apiextensionsclientset.Interface, kube kubernetes.Interface) (map[schema.GroupKind]string, error) {
	resources := make(map[schema.GroupKind]string, len(api.Kinds())+16)
	definitions, err := extensions.ApiextensionsV1().CustomResourceDefinitions().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list the object definitions: %w", err)
	}
	for _, definition := range definitions.Items {
		if definition.Spec.Group != api.Group {
			continue
		}
		resources[schema.GroupKind{Group: api.Group, Kind: definition.Spec.Names.Kind}] = definition.Spec.Names.Plural
	}
	for _, groupVersion := range []schema.GroupVersion{
		{Version: "v1"}, {Group: "apps", Version: "v1"}, {Group: "batch", Version: "v1"},
	} {
		list, err := kube.Discovery().ServerResourcesForGroupVersion(groupVersion.String())
		if err != nil {
			continue
		}
		for _, resource := range list.APIResources {
			if strings.Contains(resource.Name, "/") {
				continue
			}
			key := schema.GroupKind{Group: groupVersion.Group, Kind: resource.Kind}
			if _, known := resources[key]; !known {
				resources[key] = resource.Name
			}
		}
	}
	return resources, nil
}

// managedKubeconfig renders the credential as a kubeconfig: the managed
// cluster's address, its authority and the token above.
func managedKubeconfig(plan Plan, token string) ([]byte, error) {
	const name = "kos-managed"
	config := clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			name: {Server: plan.Managed.Server, CertificateAuthorityData: plan.Managed.Authority},
		},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{name: {Token: token}},
		Contexts:       map[string]*clientcmdapi.Context{name: {Cluster: name, AuthInfo: name}},
		CurrentContext: name,
	}
	encoded, err := clientcmd.Write(config)
	if err != nil {
		return nil, fmt.Errorf("render the managed-cluster credential: %w", err)
	}
	return encoded, nil
}

// storeCredential writes the credential into a control-plane cluster and
// returns whether this call created the secret. A repeated write replaces the
// token it finds, so an expired credential never outlives a renewal.
func storeCredential(ctx context.Context, kube kubernetes.Interface, namespace string, raw []byte, expiresAt time.Time) (bool, error) {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	data := map[string][]byte{ManagedCredentialKey: raw}
	if !expiresAt.IsZero() {
		data[ManagedCredentialExpiryKey] = []byte(expiresAt.UTC().Format(time.RFC3339))
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ManagedCredentialSecret,
			Namespace: namespace,
			Labels:    map[string]string{markers.ManagedBy: write.ManagedByValue},
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}
	secrets := kube.CoreV1().Secrets(namespace)
	_, err := secrets.Create(ctx, secret, metav1.CreateOptions{})
	switch {
	case apierrors.IsAlreadyExists(err):
		stored, getErr := secrets.Get(ctx, ManagedCredentialSecret, metav1.GetOptions{})
		if getErr != nil {
			return false, fmt.Errorf("read the managed-cluster credential: %w", getErr)
		}
		stored.Data = secret.Data
		if _, updateErr := secrets.Update(ctx, stored, metav1.UpdateOptions{}); updateErr != nil {
			return false, fmt.Errorf("replace the managed-cluster credential: %w", updateErr)
		}
		return false, nil
	case err != nil:
		return false, fmt.Errorf("write the managed-cluster credential: %w", err)
	}
	return true, nil
}

// writeCredential stores the credential in the control-plane cluster.
func (i *Installer) writeCredential(ctx context.Context, raw []byte, expiresAt time.Time) (bool, error) {
	return storeCredential(ctx, i.controlPlane().Kube, i.plan.Namespace, raw, expiresAt)
}

// ManagedClients reads the credential an external installation wrote into the
// control-plane cluster and builds the managed cluster's clients from it. It is
// how the control plane process reaches the cluster it reconciles (C-12, C-13).
// A credential that already stopped authenticating is refused with an expiry
// error before any request reaches the cluster, so the caller reads why instead
// of an unauthenticated failure (C-14).
func ManagedClients(ctx context.Context, kube kubernetes.Interface, namespace string) (*Credential, error) {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	secret, err := kube.CoreV1().Secrets(namespace).Get(ctx, ManagedCredentialSecret, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read the managed-cluster credential: %w", err)
	}
	expiresAt := storedExpiry(secret)
	if !expiresAt.IsZero() && !time.Now().Before(expiresAt) {
		return nil, &ExpiryError{ExpiresAt: expiresAt}
	}
	raw, found := secret.Data[ManagedCredentialKey]
	if !found || len(raw) == 0 {
		return nil, fmt.Errorf("the managed-cluster credential carries no %s", ManagedCredentialKey)
	}
	config, err := clientcmd.RESTConfigFromKubeConfig(raw)
	if err != nil {
		return nil, fmt.Errorf("decode the managed-cluster credential: %w", err)
	}
	cluster, err := NewCluster(ctx, config)
	if err != nil {
		return nil, err
	}
	return &Credential{Cluster: cluster, ExpiresAt: expiresAt}, nil
}

// storedExpiry reads the expiry the installation recorded beside the kubeconfig;
// zero means the record carries none.
func storedExpiry(secret *corev1.Secret) time.Time {
	stored, found := secret.Data[ManagedCredentialExpiryKey]
	if !found {
		return time.Time{}
	}
	expiresAt, err := time.Parse(time.RFC3339, string(stored))
	if err != nil {
		return time.Time{}
	}
	return expiresAt.UTC()
}

// CredentialClusterMismatchError reports a renewal asked to mint a token from a
// cluster other than the one the stored credential points at: the new token
// would be paired with an address it does not belong to.
type CredentialClusterMismatchError struct {
	// Stored is the address the credential points at.
	Stored string
	// Offered is the address of the cluster the caller wanted to mint from.
	Offered string
}

// Error implements error.
func (e *CredentialClusterMismatchError) Error() string {
	return fmt.Sprintf("the renewal names cluster %q but the stored credential points at %q", e.Offered, e.Stored)
}

// RenewManagedCredential mints a new token for the control-plane identity and
// replaces the stored credential, so the control plane keeps reaching the
// managed cluster without reinstalling (C-14, C-54). It returns the new expiry.
// managed must be the cluster the stored credential points at: a mismatch is
// refused instead of pairing a token from one cluster with another's address.
func RenewManagedCredential(ctx context.Context, controlPlane kubernetes.Interface, managed *Cluster, namespace string) (time.Time, error) {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	if controlPlane == nil || managed == nil || managed.Kube == nil || managed.Config == nil {
		return time.Time{}, errors.New("renewing the managed-cluster credential needs both clusters")
	}
	secret, err := controlPlane.CoreV1().Secrets(namespace).Get(ctx, ManagedCredentialSecret, metav1.GetOptions{})
	if err != nil {
		return time.Time{}, fmt.Errorf("read the managed-cluster credential: %w", err)
	}
	stored, found := secret.Data[ManagedCredentialKey]
	if !found || len(stored) == 0 {
		return time.Time{}, fmt.Errorf("the managed-cluster credential carries no %s", ManagedCredentialKey)
	}
	config, err := clientcmd.Load(stored)
	if err != nil {
		return time.Time{}, fmt.Errorf("decode the managed-cluster credential: %w", err)
	}
	if server := credentialServer(config); !sameServer(server, managed.Config.Host) {
		return time.Time{}, &CredentialClusterMismatchError{Stored: server, Offered: managed.Config.Host}
	}
	token, expiresAt, err := ManagedToken(ctx, managed.Kube, namespace)
	if err != nil {
		return time.Time{}, err
	}
	// Only the token changes: the address and the authority the installation
	// recorded stay as they were, and the check above keeps the token and the
	// address on the same cluster.
	for _, authInfo := range config.AuthInfos {
		authInfo.Token = token
	}
	raw, err := clientcmd.Write(*config)
	if err != nil {
		return time.Time{}, fmt.Errorf("render the renewed credential: %w", err)
	}
	if _, err := storeCredential(ctx, controlPlane, namespace, raw, expiresAt); err != nil {
		return time.Time{}, err
	}
	return expiresAt, nil
}

// credentialServer reads the API server address one kubeconfig names.
func credentialServer(config *clientcmdapi.Config) string {
	for _, cluster := range config.Clusters {
		return cluster.Server
	}
	return ""
}

// sameServer reports whether two API server addresses name the same endpoint. A
// missing scheme is compared as written rather than guessed.
func sameServer(left, right string) bool {
	return serverAddress(left) == serverAddress(right)
}

// serverAddress normalizes one address to scheme and host.
func serverAddress(server string) string {
	trimmed := strings.TrimSpace(server)
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Host == "" {
		return strings.TrimSuffix(trimmed, "/")
	}
	return parsed.Scheme + "://" + parsed.Host
}
