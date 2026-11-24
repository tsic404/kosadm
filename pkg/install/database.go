package install

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// SeededDatabase resolves the control-plane database an installation wrote into
// the delivery its workload mounts: the absolute path of the file, the node
// whose filesystem holds it, and the local account that owns it. The workload
// runs as that account, because a database file is writable only by its owner
// and its journal lives beside it. An empty node leaves the pin empty, which
// keeps the command usable where the operator does not name one.
//
// Every symbolic link is resolved first: the workload mounts the directory of
// the path it is given, so a link would be delivered as its own parent
// directory and the target would be missing inside the container. A path that
// does not resolve to a regular file is refused: the workload would create a
// fresh database there and serve accounts the installation never wrote, which
// is exactly the credential that cannot sign in.
func SeededDatabase(path, node string) (DatabaseDelivery, error) {
	return resolveSeededDatabase(path, node, PackageDirectory)
}

// NodeForHost names the node that is the host the install process runs on. The
// process is what wrote the database, so that host's filesystem is the one a
// hostPath mount has to read. A node reports the machine it runs on through its
// kubernetes.io/hostname label; its name is used only when the label is absent.
// found is false when no node reports the host; more than one node reporting it
// is refused, because the pin would have to pick one of two different
// filesystems.
func NodeForHost(ctx context.Context, kube kubernetes.Interface, hostname string) (string, bool, error) {
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return "", false, fmt.Errorf("this host reports no name")
	}
	list, err := kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", false, fmt.Errorf("list the cluster's nodes: %w", err)
	}
	matches := make([]string, 0, 1)
	for index := range list.Items {
		node := &list.Items[index]
		if nodeNamesHost(node, hostname) {
			matches = append(matches, node.Name)
		}
	}
	switch len(matches) {
	case 0:
		return "", false, nil
	case 1:
		return matches[0], true, nil
	}
	sort.Strings(matches)
	return "", false, &AmbiguousHostError{Host: hostname, Nodes: matches}
}

// AmbiguousHostError reports that more than one node claims to be the host the
// command runs on. Two nodes that both report the name are two filesystems, and
// the pin would have to pick one of them, so the caller must refuse rather than
// guess. It is a distinct type because the operator's remedy is a flag, not a
// retry: the command that receives it turns it into a usage error naming
// --control-plane-node, while a cluster that merely did not answer stays a
// cluster failure.
type AmbiguousHostError struct {
	// Host is the name this command's host reported.
	Host string
	// Nodes are the node names that claim to be that host, sorted.
	Nodes []string
}

// Error implements error.
func (e *AmbiguousHostError) Error() string {
	return fmt.Sprintf("%d nodes report this host %q: %s", len(e.Nodes), e.Host, strings.Join(e.Nodes, ", "))
}

// nodeNamesHost reports whether node is the host hostname. The hostname label
// names the machine; the node name is the fallback for a cluster that does not
// label it. A label that names another machine is not overridden by a node
// name that happens to share the host's short name.
func nodeNamesHost(node *corev1.Node, hostname string) bool {
	if label := strings.TrimSpace(node.Labels[corev1.LabelHostname]); label != "" {
		return sameHostName(label, hostname)
	}
	return sameHostName(node.Name, hostname)
}

// sameHostName reports whether candidate names the same machine as hostname.
// Two fully qualified names are the same machine only when they are equal:
// dropping the domain would make worker.site-a.example and
// worker.site-b.example, two different machines, one name. A bare name that
// carries no domain still matches a qualified one, because a host that reports
// only its short name gives no evidence of a different domain.
func sameHostName(candidate, hostname string) bool {
	candidate = strings.ToLower(strings.TrimSpace(candidate))
	hostname = strings.ToLower(strings.TrimSpace(hostname))
	if candidate == "" || hostname == "" {
		return false
	}
	if candidate == hostname {
		return true
	}
	if strings.Contains(candidate, ".") && strings.Contains(hostname, ".") {
		return false
	}
	return shortHostName(candidate) == shortHostName(hostname)
}

// shortHostName drops the domain part of a host name.
func shortHostName(name string) string {
	if cut := strings.IndexByte(name, '.'); cut > 0 {
		return name[:cut]
	}
	return name
}

// resolveSeededDatabase is SeededDatabase with the read-only package directory
// named, so the judgement on the resolved target can be exercised on a
// directory a test owns.
func resolveSeededDatabase(path, node, readOnlyDirectory string) (DatabaseDelivery, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return DatabaseDelivery{}, fmt.Errorf("resolve the control-plane database %s: %w", path, err)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if errors.Is(err, fs.ErrNotExist) {
		return DatabaseDelivery{}, fmt.Errorf("the control-plane database %s does not exist: the control plane serves the file the installation wrote, and an upgrade never creates one", absolute)
	}
	if err != nil {
		return DatabaseDelivery{}, fmt.Errorf("resolve the control-plane database %s: %w", absolute, err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return DatabaseDelivery{}, fmt.Errorf("read the control-plane database %s: %w", resolved, err)
	}
	if !info.Mode().IsRegular() {
		return DatabaseDelivery{}, fmt.Errorf("the control-plane database %s is not a regular file: the control plane serves the file the installation wrote", resolved)
	}
	// The package directory is mounted read-only and after the database
	// directory, so a database inside it would not be writable and its journal
	// could not be written. Judging the resolved target is what catches a link
	// into that directory: the link's own path says nothing about where the
	// file actually lives.
	if resolved == readOnlyDirectory || strings.HasPrefix(resolved, readOnlyDirectory+"/") {
		return DatabaseDelivery{}, fmt.Errorf("the control-plane database %s resolves into %s: the workload mounts that directory read-only for packages, so the database would not be writable", resolved, readOnlyDirectory)
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return DatabaseDelivery{}, fmt.Errorf("read the owner of the control-plane database %s", resolved)
	}
	return DatabaseDelivery{
		Path:  resolved,
		Node:  node,
		Owner: DatabaseIdentity{UID: int64(owner.Uid), GID: int64(owner.Gid)},
	}, nil
}

// servedNodePin reads the node a rendered workload is pinned to, whichever way
// the pin is expressed. A workload pinned through a required node affinity is
// selected by the scheduler; one pinned through nodeName was bound without it,
// and both name the node by its identity.
func servedNodePin(podSpec corev1.PodSpec) string {
	if podSpec.NodeName != "" {
		return podSpec.NodeName
	}
	affinity := podSpec.Affinity
	if affinity == nil || affinity.NodeAffinity == nil ||
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return ""
	}
	for _, term := range affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, field := range term.MatchFields {
			if field.Key == "metadata.name" && field.Operator == corev1.NodeSelectorOpIn && len(field.Values) == 1 {
				return field.Values[0]
			}
		}
	}
	return ""
}

// ServedDatabase reads the database delivery an installation put on the running
// control-plane workload: the file the process serves, the node that holds it
// and the account it runs as. An upgrade rewrites that workload, and a rewrite
// that renders no delivery would point the new pod at the image's own empty
// database and lose every account, the first Owner included (C-54, C-63). A
// cluster with no control-plane workload yet, or one that serves no database,
// reports false and leaves the caller's plan as it is.
//
// A workload from a release that pinned no node still reads exactly one
// machine's file — the node its pod runs on. The running pod is what makes that
// node observable, so a delivery whose template names no node takes the node of
// its serving pods. No pod, or pods on more than one node, leaves the node
// empty: nothing then shows which filesystem holds the file, and the caller
// refuses instead of guessing.
func ServedDatabase(ctx context.Context, kube kubernetes.Interface, namespace string) (DatabaseDelivery, bool, error) {
	deployment, err := kube.AppsV1().Deployments(namespace).Get(ctx, ControlPlaneName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return DatabaseDelivery{}, false, nil
	case err != nil:
		return DatabaseDelivery{}, false, fmt.Errorf("read the %s workload: %w", ControlPlaneName, err)
	}
	podSpec := deployment.Spec.Template.Spec
	for _, container := range podSpec.Containers {
		if container.Name != ControlPlaneName {
			continue
		}
		path := argumentValue(container.Args, "-database")
		if path == "" {
			return DatabaseDelivery{}, false, nil
		}
		delivery := DatabaseDelivery{Path: path, Node: servedNodePin(podSpec)}
		if delivery.Node == "" {
			node, err := servedPodNode(ctx, kube, deployment, path)
			if err != nil {
				return DatabaseDelivery{}, false, err
			}
			delivery.Node = node
		}
		if identity := container.SecurityContext; identity != nil {
			if identity.RunAsUser != nil {
				delivery.Owner.UID = *identity.RunAsUser
			}
			if identity.RunAsGroup != nil {
				delivery.Owner.GID = *identity.RunAsGroup
			}
		}
		return delivery, true, nil
	}
	return DatabaseDelivery{}, false, nil
}

// servedPodNode names the node a workload without a node pin really serves its
// hostPath database from, which is the node its control-plane pod runs on. The
// pod is the only record of that, so it has to be a pod that is demonstrably
// this workload's and demonstrably serving this delivery: a same-label pod that
// runs something else, a pod whose mount failed, or a pod still waiting for a
// node says nothing about where the file is. Pods on two nodes say the delivery
// serves two files, which no single node names. Anything less than one node
// with proof leaves the node empty, and the caller refuses.
func servedPodNode(ctx context.Context, kube kubernetes.Interface, deployment *appsv1.Deployment, path string) (string, error) {
	selector := "app=" + ControlPlaneName
	if labels := deployment.Spec.Selector; labels != nil && len(labels.MatchLabels) > 0 {
		selector = metav1.FormatLabelSelector(labels)
	}
	pods, err := kube.CoreV1().Pods(deployment.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return "", fmt.Errorf("list the %s pods: %w", ControlPlaneName, err)
	}
	node := ""
	for index := range pods.Items {
		pod := &pods.Items[index]
		if pod.DeletionTimestamp != nil || pod.Spec.NodeName == "" {
			continue
		}
		owned, err := ownedByDeployment(ctx, kube, pod, deployment)
		if err != nil {
			return "", err
		}
		if !owned {
			continue
		}
		if !servesDelivery(pod, path) {
			continue
		}
		if node != "" && node != pod.Spec.NodeName {
			return "", nil
		}
		node = pod.Spec.NodeName
	}
	return node, nil
}

// servesDelivery reports whether pod is really serving the database at path: it
// is running and ready, and its control-plane container reads that file from the
// same host directory through the plain mount the replacement renders. A pod
// that fails any of these is not evidence of the node that holds the file, and
// treating it as such would pin a replacement to a machine whose copy of the
// path is a different database.
func servesDelivery(pod *corev1.Pod, path string) bool {
	if pod.Status.Phase != corev1.PodRunning || !podReady(pod) {
		return false
	}
	container, found := controlPlanePodContainer(pod.Spec)
	if !found || argumentValue(container.Args, "-database") != path {
		return false
	}
	directory := pathpkg.Dir(cleanPath(path))
	// The file is read through the mount that covers it most specifically: a
	// mount of the file itself hides the parent directory's copy, so checking
	// the parent mount alone would accept a pod that reads another volume. The
	// replacement renders only the plain directory mount, so any other
	// effective mount is a different file.
	mount, found := effectiveMount(container.VolumeMounts, path)
	if !found || containerMountRoot(mount.MountPath) != directory || mount.ReadOnly {
		return false
	}
	// A subPath or subPathExpr redirects this container path to another host
	// file under the same volume root, and the replacement renders no sub path.
	// The pod would then read a different file than the one the upgrade keeps.
	if mount.SubPath != "" || mount.SubPathExpr != "" {
		return false
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == mount.Name && volume.HostPath != nil && cleanPath(volume.HostPath.Path) == directory {
			return true
		}
	}
	return false
}

// cleanPath returns the path the kernel resolves to on a Linux node:
// repeated separators, "." components and a trailing slash name the same
// file, and Kubernetes does not require a mount path to be in canonical form.
func cleanPath(name string) string {
	return pathpkg.Clean(name)
}

// containerMountRoot returns the absolute destination a container mount covers.
// Kubelet prefixes a relative MountPath with the container root, and the runtime
// resolves "." and repeated separators, so "var/lib/kosd/kos.db" and
// "/var/lib/kosd/kos.db" name one destination. path.Join clamps ".." at the
// root, where the container's filesystem view keeps it.
func containerMountRoot(name string) string {
	return pathpkg.Join("/", name)
}

// coversPath reports whether mountRoot is target or a directory above it. Both
// are cleaned absolute paths, and a root mount covers every path.
func coversPath(target, mountRoot string) bool {
	if target == mountRoot {
		return true
	}
	return strings.HasPrefix(target, strings.TrimSuffix(mountRoot, "/")+"/")
}

// effectiveMount returns the container mount that actually serves path: the one
// whose mount path is the longest prefix of path. A mount of a file hides the
// directory mounted above it, so the shortest match is not what the process
// reads. Comparing cleaned paths is what makes a mount written as
// /var/lib/kosd/./kos.db the same cover as one written /var/lib/kosd/kos.db.
// Two mounts that cover target at the same length are ambiguous: the runtime
// applies them in order, so the later one can serve and the pod says nothing
// about which volume the process reads. That, like a target that is not
// absolute, matches nothing and leaves the caller to refuse rather than guess.
func effectiveMount(mounts []corev1.VolumeMount, path string) (corev1.VolumeMount, bool) {
	target := cleanPath(path)
	if !pathpkg.IsAbs(target) {
		return corev1.VolumeMount{}, false
	}
	var chosen corev1.VolumeMount
	longest := -1
	ambiguous := false
	for _, mount := range mounts {
		root := containerMountRoot(mount.MountPath)
		if !coversPath(target, root) {
			continue
		}
		switch {
		case len(root) > longest:
			chosen, longest = mount, len(root)
			ambiguous = false
		case len(root) == longest:
			ambiguous = true
		}
	}
	if ambiguous {
		return corev1.VolumeMount{}, false
	}
	return chosen, longest >= 0
}

// podReady reports whether the pod's Ready condition is true, which is what
// distinguishes a pod that started its control plane from one still mounting
// the host directory.
func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// ownedByDeployment reports whether a ReplicaSet of deployment controls the pod,
// following the controller chain and comparing UIDs at both links. A ReplicaSet
// name that merely starts with the deployment's name proves nothing: another
// deployment in the namespace can own a ReplicaSet with that prefix, and its pod
// would then name a node the installation never chose. The pod's labels say
// nothing about who wrote it either.
func ownedByDeployment(ctx context.Context, kube kubernetes.Interface, pod *corev1.Pod, deployment *appsv1.Deployment) (bool, error) {
	var controller *metav1.OwnerReference
	for index := range pod.OwnerReferences {
		owner := &pod.OwnerReferences[index]
		if owner.Kind == "ReplicaSet" && owner.Controller != nil && *owner.Controller {
			controller = owner
			break
		}
	}
	if controller == nil {
		return false, nil
	}
	replicaSet, err := kube.AppsV1().ReplicaSets(pod.Namespace).Get(ctx, controller.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("read the ReplicaSet %s/%s that owns pod %s: %w", pod.Namespace, controller.Name, pod.Name, err)
	}
	// The pod names the ReplicaSet by UID as well as name, so a pod left over
	// from a deleted ReplicaSet whose name was reused is not read as this one's.
	if controller.UID == "" || replicaSet.UID == "" || controller.UID != replicaSet.UID {
		return false, nil
	}
	for index := range replicaSet.OwnerReferences {
		owner := &replicaSet.OwnerReferences[index]
		if owner.Kind != "Deployment" || owner.Controller == nil || !*owner.Controller {
			continue
		}
		if owner.Name != deployment.Name {
			continue
		}
		// The controller reference is followed all the way to this deployment,
		// by UID: another deployment whose name extends this one's owns
		// ReplicaSets that share the name prefix, and their pods must not be
		// read as this workload's.
		if owner.UID == "" || deployment.UID == "" || owner.UID != deployment.UID {
			continue
		}
		return true, nil
	}
	return false, nil
}

// controlPlanePodContainer returns the pod's control-plane container, which is
// the one whose command line names the database.
func controlPlanePodContainer(spec corev1.PodSpec) (corev1.Container, bool) {
	for index := range spec.Containers {
		if spec.Containers[index].Name == ControlPlaneName {
			return spec.Containers[index], true
		}
	}
	return corev1.Container{}, false
}

// argumentValue returns the value that follows flag in a rendered command line.
func argumentValue(args []string, flag string) string {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == flag {
			return args[index+1]
		}
	}
	return ""
}

// InheritDatabaseDelivery returns plan with the database delivery the running
// control-plane workload serves filled in where the plan leaves it out: an
// upgrade names the release, not the installation's inputs, and rendering
// without the delivery would point the new pod at the image's own empty
// database (C-54, C-63). A plan that names the database but no node also takes
// the served node, because the file is only reachable from the node that holds
// it. A plan that names its own delivery keeps it untouched.
func InheritDatabaseDelivery(plan Plan, served DatabaseDelivery, found bool) Plan {
	if !found {
		return plan
	}
	switch {
	case plan.Database == nil:
		delivery := served
		plan.Database = &delivery
	case plan.Database.Node == "":
		plan.Database.Node = served.Node
	}
	return plan
}
