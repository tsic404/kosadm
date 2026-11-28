package harness

import (
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/tsic404/kosadm/pkg/install"
)

// AssertPackageDirectory checks the delivery channel an installation writes
// into its control-plane workload: the host directory an operator drops a
// package in, mounted read-only, and the flag that points the process at it
// (KOSD-C-17, KOSD-AC-72).
func AssertPackageDirectory(t *testing.T, deployment *appsv1.Deployment) {
	t.Helper()
	if len(deployment.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("%s carries %d containers, want the control plane alone",
			deployment.Name, len(deployment.Spec.Template.Spec.Containers))
	}
	container := deployment.Spec.Template.Spec.Containers[0]
	if container.Name != install.ControlPlaneName {
		t.Fatalf("the workload runs %q, want %q", container.Name, install.ControlPlaneName)
	}
	if !hasArgument(container.Args, "-packages", install.PackageDirectory) {
		t.Fatalf("the workload args = %v, want -packages %s", container.Args, install.PackageDirectory)
	}
	if !hasReadOnlyMount(container.VolumeMounts, install.PackageVolume, install.PackageDirectory) {
		t.Fatalf("the workload mounts %v, want %s read-only at %s",
			container.VolumeMounts, install.PackageVolume, install.PackageDirectory)
	}
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Name != install.PackageVolume || volume.HostPath == nil {
			continue
		}
		if volume.HostPath.Path == install.PackageDirectory && volume.HostPath.Type != nil &&
			*volume.HostPath.Type == corev1.HostPathDirectoryOrCreate {
			return
		}
	}
	t.Fatalf("the workload volumes = %v, want the host's %s directory", deployment.Spec.Template.Spec.Volumes, install.PackageDirectory)
}

// AssertDatabaseDelivery checks the database channel an installation writes into
// its control-plane workload: the directory of the database the run seeded,
// mounted writable at the path it was written, the flag that points the process
// at that exact file, and the identity that owns it. This is what makes the
// credential the installation printed the account the pod serves (C-54,
// ADM-TC-131). The workload is also pinned to the node that holds the file: a
// hostPath reads one node's filesystem, and an unpinned pod can start where the
// file is not and serve an empty database. The pin is a required node affinity
// and not a nodeName field, so a drain can still move the pod off the node
// (KOSD-C-65).
func AssertDatabaseDelivery(t *testing.T, deployment *appsv1.Deployment, delivery install.DatabaseDelivery) {
	t.Helper()
	container := deployment.Spec.Template.Spec.Containers[0]
	if !hasArgument(container.Args, "-database", delivery.Path) {
		t.Fatalf("the workload args = %v, want -database %s", container.Args, delivery.Path)
	}
	directory := filepath.Dir(delivery.Path)
	if !hasWritableMount(container.VolumeMounts, install.DatabaseVolume, directory) {
		t.Fatalf("the workload mounts %v, want %s writable at %s",
			container.VolumeMounts, install.DatabaseVolume, directory)
	}
	if context := container.SecurityContext; context == nil || context.RunAsUser == nil ||
		*context.RunAsUser != delivery.Owner.UID || context.RunAsGroup == nil ||
		*context.RunAsGroup != delivery.Owner.GID {
		t.Fatalf("the workload runs as %v, want the database owner %d:%d", container.SecurityContext, delivery.Owner.UID, delivery.Owner.GID)
	}
	if pinned := RequiredNode(deployment.Spec.Template.Spec); pinned != delivery.Node {
		t.Fatalf("the workload's required node affinity = %q, want %q, the node that holds the file", pinned, delivery.Node)
	}
	if bound := deployment.Spec.Template.Spec.NodeName; bound != "" {
		t.Fatalf("the workload nodeName = %q, want none: a nodeName bypasses the scheduler and defeats the drain", bound)
	}
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Name != install.DatabaseVolume || volume.HostPath == nil {
			continue
		}
		if volume.HostPath.Path == directory && volume.HostPath.Type != nil &&
			*volume.HostPath.Type == corev1.HostPathDirectory {
			return
		}
	}
	t.Fatalf("the workload volumes = %v, want the host's %s directory", deployment.Spec.Template.Spec.Volumes, directory)
}

// hasArgument reports whether args carries flag with exactly value.
func hasArgument(args []string, flag, value string) bool {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == flag && args[index+1] == value {
			return true
		}
	}
	return false
}

// hasWritableMount reports whether one mount carries name writable at path.
func hasWritableMount(mounts []corev1.VolumeMount, name, path string) bool {
	for _, mount := range mounts {
		if mount.Name == name && mount.MountPath == path && !mount.ReadOnly {
			return true
		}
	}
	return false
}

// hasReadOnlyMount reports whether one mount carries name read-only at path.
func hasReadOnlyMount(mounts []corev1.VolumeMount, name, path string) bool {
	for _, mount := range mounts {
		if mount.Name == name && mount.MountPath == path && mount.ReadOnly {
			return true
		}
	}
	return false
}

// RequiredNode returns the node the workload's required node affinity selects by
// name, or the empty string when it carries no such pin.
func RequiredNode(podSpec corev1.PodSpec) string {
	if podSpec.Affinity == nil || podSpec.Affinity.NodeAffinity == nil ||
		podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return ""
	}
	for _, term := range podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, field := range term.MatchFields {
			if field.Key == "metadata.name" && field.Operator == corev1.NodeSelectorOpIn && len(field.Values) == 1 {
				return field.Values[0]
			}
		}
	}
	return ""
}
