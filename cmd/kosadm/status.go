package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tsic404/kosadm/pkg/install"
)

// runStatus reports the installation and component state without changing it
// (the status module of the design's module table).
func runStatus(args []string, out io.Writer) error {
	set := flag.NewFlagSet("status", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	kubeconfig := set.String("kubeconfig", "",
		"kubeconfig of the cluster to read; empty reads KUBECONFIG and ~/.kube/config")
	namespace := set.String("namespace", install.DefaultNamespace,
		"namespace the installation created for itself")
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
	reader, err := install.NewStatusReader(clusterClients.kube, clusterClients.extended, clusterClients.dynamic)
	if err != nil {
		return err
	}
	reader.Namespace = *namespace
	ctx, stop := signalContext()
	defer stop()
	status, err := reader.Read(ctx)
	if err != nil {
		return err
	}
	printStatus(out, status)
	return nil
}

// printStatus writes the read-only report.
func printStatus(out io.Writer, status install.Status) {
	state := "absent"
	if status.Installed {
		state = "installed"
	}
	fmt.Fprintf(out, "Namespace: %s (%s)\n", status.Namespace, state)
	fmt.Fprintf(out, "Control plane: %s\n", workloadText(status.ControlPlane))
	fmt.Fprintf(out, "Object definitions: %d\n", len(status.Definitions))
	if len(status.Components) == 0 {
		fmt.Fprintln(out, "Components: none")
		return
	}
	fmt.Fprintln(out, "Components:")
	for _, component := range status.Components {
		if component.Ready {
			fmt.Fprintf(out, "  %s ready\n", component.Name)
			continue
		}
		fmt.Fprintf(out, "  %s not ready (%s)\n", component.Name, component.Reason)
	}
}

// workloadText renders one workload's rollout.
func workloadText(workload install.WorkloadStatus) string {
	if !workload.Present {
		return workload.Name + " absent"
	}
	return fmt.Sprintf("%s %d/%d ready", workload.Name, workload.Ready, workload.Desired)
}
