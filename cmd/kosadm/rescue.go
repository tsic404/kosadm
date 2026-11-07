package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/tsic404/kosadm/pkg/install"
)

// runRescue reads native resources without writing them (C-54). Without a
// resource the command counts what every listable resource holds; with one it
// prints that resource's objects.
func runRescue(args []string, out io.Writer) error {
	set := flag.NewFlagSet("rescue", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	kubeconfig := set.String("kubeconfig", "",
		"kubeconfig of the cluster to read; empty reads KUBECONFIG and ~/.kube/config")
	namespace := set.String("namespace", "",
		"namespace to read; empty reads every namespace, cluster-scoped resources ignore it")
	var resources repeated
	set.Var(&resources, "resource",
		"resource to read as group/version/resource or version/resource; repeatable, empty counts every resource")
	if err := set.Parse(args); err != nil {
		return exitError{message: err.Error(), code: 2}
	}

	config, err := loadConfig(*kubeconfig)
	if err != nil {
		return err
	}
	clusterClients, err := newClients(config)
	if err != nil {
		return err
	}
	rescue, err := install.NewRescue(clusterClients.dynamic, clusterClients.kube.Discovery())
	if err != nil {
		return err
	}
	ctx, stop := signalContext()
	defer stop()

	if len(resources) == 0 {
		report, err := rescue.Native(ctx)
		if err != nil {
			return err
		}
		printNativeReport(out, report)
		if len(report.Failures) > 0 {
			return exitError{message: fmt.Sprintf("rescue could not read %d resource(s)", len(report.Failures)), code: 1}
		}
		return nil
	}
	for _, value := range resources {
		resource, err := parseResource(value)
		if err != nil {
			return err
		}
		objects, err := rescue.List(ctx, resource, *namespace)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "%s: %d object(s)\n", resourceText(resource), len(objects))
		for _, object := range objects {
			fmt.Fprintf(out, "  %s\n", nameText(object.GetNamespace(), object.GetName()))
		}
	}
	return nil
}

// printNativeReport writes the resources that answered and, last, the ones the
// server refused: a partial read is shown with its gaps instead of passing as a
// clean count.
func printNativeReport(out io.Writer, report install.NativeReport) {
	printNativeViews(out, report.Views)
	if len(report.Failures) == 0 {
		return
	}
	fmt.Fprintf(out, "Unreadable: %d resource(s)\n", len(report.Failures))
	for _, failure := range report.Failures {
		fmt.Fprintf(out, "  %s: %v\n", failureText(failure), failure.Err)
	}
}

// printNativeViews writes the non-empty resources and their object counts.
func printNativeViews(out io.Writer, views []install.NativeView) {
	for _, view := range views {
		if len(view.Objects) == 0 {
			continue
		}
		fmt.Fprintf(out, "%s: %d object(s)\n", resourceText(view.Resource), len(view.Objects))
	}
}

// failureText names what a rescue read could not reach.
func failureText(failure install.NativeFailure) string {
	if failure.Resource.Resource != "" {
		return resourceText(failure.Resource)
	}
	if name := failure.Resource.GroupVersion().String(); name != "" {
		return name
	}
	return "discovery"
}

// parseResource reads group/version/resource or version/resource.
func parseResource(value string) (schema.GroupVersionResource, error) {
	parts := strings.Split(value, "/")
	for _, part := range parts {
		if part == "" {
			return schema.GroupVersionResource{}, exitError{
				message: fmt.Sprintf("--resource %q is not group/version/resource or version/resource", value), code: 2,
			}
		}
	}
	switch len(parts) {
	case 2:
		return schema.GroupVersionResource{Version: parts[0], Resource: parts[1]}, nil
	case 3:
		return schema.GroupVersionResource{Group: parts[0], Version: parts[1], Resource: parts[2]}, nil
	}
	return schema.GroupVersionResource{}, exitError{
		message: fmt.Sprintf("--resource %q is not group/version/resource or version/resource", value), code: 2,
	}
}

// resourceText renders one resource the way the flag names it.
func resourceText(resource schema.GroupVersionResource) string {
	if resource.Group == "" {
		return resource.Version + "/" + resource.Resource
	}
	return resource.Group + "/" + resource.Version + "/" + resource.Resource
}

// nameText renders one object the way an operator reads it.
func nameText(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}
