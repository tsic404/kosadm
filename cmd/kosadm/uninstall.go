package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosd/api/v1beta1"
)

// dataHelp describes --data. It names only the value the command accepts:
// advertising a value the run refuses would make --help contradict the refusal.
const dataHelp = "what the uninstall does with the installation's data: retain keeps it; removing data belongs to the volume path, which owns the claim writes"

// uninstallFlags are the values the uninstall command reads after parsing.
type uninstallFlags struct {
	kubeconfig             *string
	namespace              *string
	mode                   *string
	controlPlaneKubeconfig *string
	data                   *string
}

// declareUninstallFlags registers the command's flags and their help.
func declareUninstallFlags(set *flag.FlagSet) uninstallFlags {
	return uninstallFlags{
		kubeconfig: set.String("kubeconfig", "",
			"kubeconfig of the cluster the installation reconciles; empty reads KUBECONFIG and ~/.kube/config"),
		namespace: set.String("namespace", install.DefaultNamespace,
			"namespace the installation created for itself"),
		mode: set.String("mode", string(install.ModeExisting),
			"installation form: existing-cluster, bootstrap or external-control-plane"),
		controlPlaneKubeconfig: set.String("control-plane-kubeconfig", "",
			"kubeconfig of the cluster that runs the control plane (external-control-plane form)"),
		data: set.String("data", "retain", dataHelp),
	}
}

// runUninstall removes an installation and prints the leftover manifest
// (C-54). Data is kept unless the operator says otherwise: the uninstall never
// deletes a claim, because only the volume path writes PVCs (KOSD-C-04).
func runUninstall(args []string, out io.Writer) error {
	set := flag.NewFlagSet("uninstall", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	flags := declareUninstallFlags(set)
	if err := set.Parse(args); err != nil {
		return exitError{message: err.Error(), code: 2}
	}
	form := install.Mode(*flags.mode)
	switch form {
	case install.ModeExisting, install.ModeBootstrap, install.ModeExternal:
	default:
		return exitError{message: fmt.Sprintf("--mode %q is not an installation form", *flags.mode), code: 2}
	}
	if form == install.ModeExternal && *flags.controlPlaneKubeconfig == "" {
		return exitError{message: "--mode external-control-plane needs --control-plane-kubeconfig", code: 2}
	}
	if form != install.ModeExternal && *flags.controlPlaneKubeconfig != "" {
		return exitError{message: "--control-plane-kubeconfig belongs to --mode external-control-plane", code: 2}
	}
	switch *flags.data {
	case "retain":
	case "delete":
		return exitError{message: "the uninstall cannot remove data: only the volume path deletes claims. Purge the volumes first, or keep the data and read the leftover manifest.", code: 2}
	default:
		return exitError{message: fmt.Sprintf("--data %q is not a retention option: use retain", *flags.data), code: 2}
	}

	ctx, stop := signalContext()
	defer stop()

	managedConfig, err := loadConfig(*flags.kubeconfig)
	if err != nil {
		return err
	}
	clusterClients, err := newClients(managedConfig)
	if err != nil {
		return err
	}
	config := install.UninstallConfig{
		Mode:          form,
		Namespace:     *flags.namespace,
		RetainData:    true,
		Dynamic:       clusterClients.dynamic,
		Kube:          clusterClients.kube,
		APIExtensions: clusterClients.extended,
		Discovery:     clusterClients.kube.Discovery(),
	}
	if form == install.ModeExternal {
		controlConfig, err := loadConfig(*flags.controlPlaneKubeconfig)
		if err != nil {
			return err
		}
		controlPlane, err := install.NewCluster(ctx, controlConfig)
		if err != nil {
			return fmt.Errorf("build the control-plane cluster clients: %w", err)
		}
		config.ControlPlaneCluster = controlPlane
	}
	uninstaller, err := install.NewUninstaller(config)
	if err != nil {
		return err
	}
	manifest, runErr := uninstaller.Uninstall(ctx)
	if runErr != nil {
		printIncomplete(out, manifest)
		return runErr
	}
	printManifest(out, manifest)
	return nil
}

// printManifest writes all six leftover categories, including the empty ones.
func printManifest(out io.Writer, manifest install.Manifest) {
	fmt.Fprintf(out, "Finished: %s\n", manifest.GeneratedAt.Format(time.RFC3339))
	printEntries(out, manifest)
}

// printIncomplete writes what a stopped run had collected. It never prints a
// finish time: an empty-looking finished manifest is exactly what an operator
// must not be handed after a failure (C-54).
func printIncomplete(out io.Writer, manifest install.Manifest) {
	fmt.Fprintln(out, "Incomplete: the uninstall stopped before finishing; the entries below were collected before it stopped")
	printEntries(out, manifest)
}

// printEntries writes the categories of one manifest in the fixed order.
func printEntries(out io.Writer, manifest install.Manifest) {
	fmt.Fprintln(out, "Leftovers:")
	for _, category := range install.Categories() {
		entries := manifest.ByCategory(category)
		fmt.Fprintf(out, "  %s: %d\n", category, len(entries))
		for _, entry := range entries {
			fmt.Fprintf(out, "    %s (%s)\n", objectText(entry.Object), entry.Disposition)
		}
	}
}

// objectText renders one manifest object the way an operator reads it.
func objectText(object v1beta1.ObjectRef) string {
	name := object.Name
	if object.Namespace != "" {
		name = object.Namespace + "/" + name
	}
	kind := object.Kind
	if object.Group != "" {
		kind = object.Kind + "." + object.Group
	}
	return kind + " " + name
}
