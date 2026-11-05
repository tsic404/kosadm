package harness

import (
	"context"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// ServiceAccountUser is the API server user name a service account
// authenticates as.
func ServiceAccountUser(namespace, name string) string {
	return "system:serviceaccount:" + namespace + ":" + name
}

// Impersonating returns a copy of the cluster configuration that acts as
// username. The test admin is in system:masters, so it may impersonate; the
// request then carries username in the audit log (C-15).
func (c *Cluster) Impersonating(t *testing.T, username string) *rest.Config {
	t.Helper()
	if c.Config == nil {
		t.Fatal("the cluster carries no rest configuration")
	}
	config := rest.CopyConfig(c.Config)
	config.Impersonate = rest.ImpersonationConfig{UserName: username}
	return config
}

// DynamicAs returns a dynamic client acting as one user.
func (c *Cluster) DynamicAs(t *testing.T, username string) dynamic.Interface {
	t.Helper()
	client, err := dynamic.NewForConfig(c.Impersonating(t, username))
	if err != nil {
		t.Fatalf("build dynamic client as %q: %v", username, err)
	}
	return client
}

// KubeAs returns a typed client acting as one user.
func (c *Cluster) KubeAs(t *testing.T, username string) kubernetes.Interface {
	t.Helper()
	client, err := kubernetes.NewForConfig(c.Impersonating(t, username))
	if err != nil {
		t.Fatalf("build kubernetes client as %q: %v", username, err)
	}
	return client
}

// ExtensionsAs returns an apiextensions client acting as one user.
func (c *Cluster) ExtensionsAs(t *testing.T, username string) apiextensionsclientset.Interface {
	t.Helper()
	client, err := apiextensionsclientset.NewForConfig(c.Impersonating(t, username))
	if err != nil {
		t.Fatalf("build apiextensions client as %q: %v", username, err)
	}
	return client
}

// GrantClusterAdmin binds one user to cluster-admin so an impersonated
// installation can create namespaces, definitions and permissions. The binding
// is an administrator's, not KOS's: it is created before the install window.
func (c *Cluster) GrantClusterAdmin(t *testing.T, username string) {
	t.Helper()
	name := "acceptance-" + strings.NewReplacer(":", "-", "/", "-", "@", "-").Replace(username)
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
		},
		Subjects: []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: username}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	_, err := c.Kube.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("grant cluster-admin to %q: %v", username, err)
	}
}
