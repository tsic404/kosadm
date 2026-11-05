package main

import (
	"context"
	"fmt"
	"os"
	"time"

	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/tsic404/kosadm/pkg/install"
)

// clients are the three clients one installation needs, all acting as the
// caller's kubeconfig identity.
type clients struct {
	dynamic  dynamic.Interface
	kube     kubernetes.Interface
	extended apiextensionsclientset.Interface
}

// loadConfig reads the kubeconfig, falling back to KUBECONFIG and the default
// location when path is empty.
func loadConfig(path string) (*rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if path != "" {
		rules.ExplicitPath = path
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("read kubeconfig: %w", err)
	}
	return config, nil
}

// newClients builds the clients from one rest configuration.
func newClients(config *rest.Config) (clients, error) {
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return clients{}, fmt.Errorf("build dynamic client: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return clients{}, fmt.Errorf("build kubernetes client: %w", err)
	}
	extensionsClient, err := apiextensionsclientset.NewForConfig(config)
	if err != nil {
		return clients{}, fmt.Errorf("build apiextensions client: %w", err)
	}
	return clients{dynamic: dynamicClient, kube: kubeClient, extended: extensionsClient}, nil
}

// awaitCluster waits until the API server behind path answers.
func awaitCluster(ctx context.Context, path string, timeout time.Duration) (*rest.Config, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		var config *rest.Config
		config, lastErr = loadConfig(path)
		if lastErr == nil {
			var client kubernetes.Interface
			client, lastErr = kubernetes.NewForConfig(config)
			if lastErr == nil {
				if _, lastErr = client.Discovery().ServerVersion(); lastErr == nil {
					return config, nil
				}
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("the run kernel did not answer within %s: %v", timeout, lastErr)
		}
		if err := sleep(ctx, 500*time.Millisecond); err != nil {
			return nil, err
		}
	}
}

// sleep waits, or returns when the context ends.
func sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// definitions reads the release's object definition manifests from a directory.
func definitions(directory string) (install.DefinitionSource, error) {
	if directory == "" {
		return nil, fmt.Errorf("--definitions is required: name the directory of the release's object definition manifests")
	}
	info, err := os.Stat(directory)
	if err != nil {
		return nil, fmt.Errorf("read object definition directory %s: %w", directory, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory of object definition manifests", directory)
	}
	return install.CRDDirectory{FS: os.DirFS(directory)}, nil
}
