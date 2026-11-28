package main

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/tsic404/kosd/pkg/controlplane"
	"github.com/tsic404/kosd/pkg/extension"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosadm/pkg/upgrade"
)

// TestUpgradeTargetsResolveTheirPackage pins the extension target resolution: a
// target without a supplied artifact is refused before anything is written, and
// a supplied one carries the digest of exactly the bytes it names.
func TestUpgradeTargetsResolveTheirPackage(t *testing.T) {
	content, digest, err := extension.Pack(extension.Declaration{
		Identifier: "demo", Version: "2.0.0", FrameworkVersion: ">=1.0 <2.0",
	}, nil)
	if err != nil {
		t.Fatalf("pack the target declaration: %v", err)
	}
	artifacts := extension.MemoryArtifacts{"registry/demo-2": content}
	targets, err := upgradeTargets(assignments{"demo": "registry/demo-2"}, artifacts)
	if err != nil {
		t.Fatalf("resolve targets: %v", err)
	}
	if len(targets) != 1 || targets[0].Component != "demo" {
		t.Fatalf("targets = %+v, want one entry for demo", targets)
	}
	if targets[0].Digest != digest {
		t.Fatalf("digest = %q, want %q", targets[0].Digest, digest)
	}

	if _, err := upgradeTargets(assignments{"demo": "registry/absent"}, artifacts); err == nil {
		t.Fatal("a target whose package is not supplied was resolved")
	}
	unreadable := extension.MemoryArtifacts{"registry/unreadable": []byte("not a package")}
	if _, err := upgradeTargets(assignments{"demo": "registry/unreadable"}, unreadable); err == nil {
		t.Fatal("a target whose package cannot be read was resolved")
	}
}

// TestAssignmentsRefuseMalformedValues keeps a repeated flag from silently
// dropping an entry.
func TestAssignmentsRefuseMalformedValues(t *testing.T) {
	values := assignments{}
	if err := values.Set("demo=registry/demo-2"); err != nil {
		t.Fatalf("set a well-formed value: %v", err)
	}
	for _, value := range []string{"demo", "=registry/demo-2", "demo=", "demo=registry/demo-2"} {
		if err := values.Set(value); err == nil {
			t.Fatalf("%q was accepted", value)
		}
	}
}

// TestBlockedReasonsNameEveryProblem checks that a refused extension upgrade
// reports the dependency that blocked it item by item (C-55).
func TestBlockedReasonsNameEveryProblem(t *testing.T) {
	preflight := extension.UpgradePreflight{
		Components: []extension.ComponentUpgrade{{
			Component: "demo",
			Problems:  []string{"依赖未满足：demo.absent/capability@v1"},
			IsBlocked: true,
		}},
		IsBlocked: true,
	}
	reasons := blockedReasons(preflight)
	if !strings.Contains(reasons, "demo") || !strings.Contains(reasons, "demo.absent/capability") {
		t.Fatalf("reasons = %q, want the component and the missing dependency", reasons)
	}
}

// TestPrintUpgradeReportNamesWhatTheUpgradeStops pins the preflight output: the
// affected applications, the declared downtime, the unhealthy count and every
// item the flow could not judge (C-55).
func TestPrintUpgradeReportNamesWhatTheUpgradeStops(t *testing.T) {
	var out bytes.Buffer
	printUpgradeReport(&out, upgrade.Report{
		Nodes: []upgrade.NodePlan{{
			Node:           "node-a",
			KubeletVersion: "v1.37.0",
			Applications:   []string{"default/shop"},
			Volumes:        []string{"apps/data"},
			Downtime:       "10m",
		}},
		UnhealthyApplications: 1,
		Undetermined:          []string{"卷 apps/data（卷没有记录数据所在节点）"},
		Blocked:               true,
	})
	printed := out.String()
	for _, want := range []string{"node-a", "default/shop", "apps/data", "10m", "Unhealthy applications: 1", "卷 apps/data", "Blocked: true"} {
		if !strings.Contains(printed, want) {
			t.Fatalf("report %q does not carry %q", printed, want)
		}
	}
}

// TestPrintExtensionPreflightShowsEachClass checks that the contribution
// difference reads item by item (C-55).
func TestPrintExtensionPreflightShowsEachClass(t *testing.T) {
	var out bytes.Buffer
	printExtensionPreflight(&out, extension.UpgradePreflight{Components: []extension.ComponentUpgrade{{
		Component: "demo",
		From:      "registry/demo-1",
		To:        "registry/demo-2",
		Changes: []extension.ContributionChange{
			{Class: extension.ClassPages, Removed: []string{"demo"}},
			{Class: extension.ClassSettings, Added: []string{"demo-setting"}},
		},
	}}})
	printed := out.String()
	for _, want := range []string{"registry/demo-1 -> registry/demo-2", "pages: -demo", "settings: -none +demo-setting"} {
		if !strings.Contains(printed, want) {
			t.Fatalf("preflight %q does not carry %q", printed, want)
		}
	}
}

// TestPlatformFlagsRefuseAnUpgradeItCannotHonour covers the flag combinations
// the platform form cannot run.
func TestPlatformFlagsRefuseAnUpgradeItCannotHonour(t *testing.T) {
	cases := []struct {
		name  string
		flags platformFlags
	}{
		{"no definitions", platformFlags{installedFrameworks: repeated{"1.0"}}},
		{"no framework versions", platformFlags{definitions: "definitions"}},
		{"version without a node", platformFlags{definitions: "definitions", installedFrameworks: repeated{"1.0"}, version: "v1.38.0"}},
		{"node without a version", platformFlags{definitions: "definitions", installedFrameworks: repeated{"1.0"}, nodes: repeated{"node-a"}, identityDatabase: "kos.db"}},
		{"node without the control-plane database", platformFlags{definitions: "definitions", installedFrameworks: repeated{"1.0"}, nodes: repeated{"node-a"}, version: "v1.38.0"}},
		{"external flags on the existing form", platformFlags{definitions: "definitions", installedFrameworks: repeated{"1.0"}, address: "http://127.0.0.1:8080"}},
		{"external form without the control-plane kubeconfig", platformFlags{definitions: "definitions", installedFrameworks: repeated{"1.0"}, external: true}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.flags.validate(); err == nil {
				t.Fatal("the upgrade flags were accepted")
			}
		})
	}
	valid := platformFlags{definitions: "definitions", installedFrameworks: repeated{"1.0"}, nodes: repeated{"node-a"}, version: "v1.38.0", downtime: "10m", identityDatabase: "kos.db", controlPlaneNode: "node-a"}
	if err := valid.validate(); err != nil {
		t.Fatalf("a complete platform upgrade was refused: %v", err)
	}
}

// TestPlatformFlagsKeepAPlatformOnlyUpgradeUsable pins the external surface an
// operator already has: a platform upgrade that takes no node out and names no
// database is the shape the release shipped before, and it must keep working.
// Requiring the database unconditionally turned every such call into a usage
// error, on a single node as well, which is a contract change the release never
// declared (ADM-C-55).
func TestPlatformFlagsKeepAPlatformOnlyUpgradeUsable(t *testing.T) {
	platformOnly := platformFlags{definitions: "definitions", installedFrameworks: repeated{"1.0"}}
	if err := platformOnly.validate(); err != nil {
		t.Fatalf("a platform-only upgrade was refused: %v", err)
	}
	// The database stays required where the flow writes maintenance records
	// into it, which is exactly the node form (ADM-C-56).
	nodeForm := platformFlags{definitions: "definitions", installedFrameworks: repeated{"1.0"}, nodes: repeated{"node-a"}, version: "v1.38.0"}
	if err := nodeForm.validate(); err == nil {
		t.Fatal("a node upgrade without the control-plane database was accepted")
	}
}

// TestPlatformUpgradeKeepsTheDatabaseDelivery pins the upgrade side of the same
// chain: an upgrade rewrites the control-plane workload, so it must render the
// database the installation seeded. Without it the new pod would open an empty
// database and every account, the first Owner included, would be gone (C-54).
func TestPlatformUpgradeKeepsTheDatabaseDelivery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kos.db")
	if err := os.WriteFile(path, []byte("seeded"), 0o600); err != nil {
		t.Fatalf("write the database: %v", err)
	}
	flags := platformFlags{
		definitions:         "definitions",
		installedFrameworks: repeated{"1.0"},
		owner:               "owner",
		identityDatabase:    path,
		controlPlaneNode:    "node-a",
	}
	plan, err := flags.plan(nil)
	if err != nil {
		t.Fatalf("build the upgrade plan: %v", err)
	}
	if plan.Database == nil || plan.Database.Path != path {
		t.Fatalf("plan database = %+v, want %s", plan.Database, path)
	}
	if plan.Database.Node != "node-a" {
		t.Fatalf("plan database node = %q, want node-a", plan.Database.Node)
	}
}

// TestSameServedDatabaseRefusesTheSamePathOnAnotherNode pins the proof the
// upgrade needs before it reuses a node: two machines that each hold the
// database path hold two different files, so the same string cannot stand for
// the file the running control plane serves.
func TestSameServedDatabaseRefusesTheSamePathOnAnotherNode(t *testing.T) {
	served := install.DatabaseDelivery{Path: "/var/lib/kosd/kos.db", Node: "node-a"}
	named := install.DatabaseDelivery{Path: "/var/lib/kosd/kos.db", Node: "node-b"}
	err := sameServedDatabase(served, named)
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 2 {
		t.Fatalf("same path on two nodes = %v, want a usage refusal", err)
	}
	for _, want := range []string{"node-a", "node-b", "/var/lib/kosd/kos.db"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not name %q", err, want)
		}
	}
}

// TestSameServedDatabaseRefusesAnotherFileOnTheServedNode pins the other
// mismatch: the node is the one the workload runs on, but the named path is
// another database, so the replacement would stop serving the accounts the
// installation wrote.
func TestSameServedDatabaseRefusesAnotherFileOnTheServedNode(t *testing.T) {
	served := install.DatabaseDelivery{Path: "/var/lib/kosd/kos.db", Node: "node-a"}
	named := install.DatabaseDelivery{Path: "/srv/other.db", Node: "node-a"}
	if err := sameServedDatabase(served, named); err == nil {
		t.Fatal("another file on the served node was accepted")
	}
}

// TestSameServedDatabaseAcceptsTheServedFileOnItsNode pins the accepted proof:
// the same node mounting the same path is the same file.
func TestSameServedDatabaseAcceptsTheServedFileOnItsNode(t *testing.T) {
	delivery := install.DatabaseDelivery{Path: "/var/lib/kosd/kos.db", Node: "node-a"}
	if err := sameServedDatabase(delivery, delivery); err != nil {
		t.Fatalf("the served file on its own node = %v, want accepted", err)
	}
}

// TestOpenAuditRefusesAPathThatIsNotAnExistingFile pins the database check:
// opening creates and migrates a database, so a mistyped path must be refused
// before anything is opened, rather than taking the maintenance records into a
// database the control plane never serves (C-14, C-65).
func TestOpenAuditRefusesAPathThatIsNotAnExistingFile(t *testing.T) {
	directory := t.TempDir()
	missing := filepath.Join(directory, "control-plane.db")
	_, closeStore, err := openAudit(context.Background(), missing)
	if err == nil {
		closeStore()
		t.Fatal("a database path that does not exist was opened")
	}
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 2 {
		t.Fatalf("missing database = %v, want a usage error", err)
	}
	if _, statErr := os.Stat(missing); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("the refused database was created: %v", statErr)
	}

	if _, closeStore, err := openAudit(context.Background(), directory); err == nil {
		closeStore()
		t.Fatal("a directory was opened as the control-plane database")
	}
}

// TestRequireExtensionPackagesRefusesAnUnreadableExtension pins the judgement's
// input completeness: the framework dependency preflight skips a component
// whose package was not supplied, so the CLI refuses before it judges (C-16,
// C-55).
func TestRequireExtensionPackagesRefusesAnUnreadableExtension(t *testing.T) {
	component := func(name, reference string, labels map[string]string) *unstructured.Unstructured {
		object := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "kos.tsic.top/v1beta1",
			"kind":       "SystemComponent",
			"metadata":   map[string]any{"name": name},
		}}
		object.SetLabels(labels)
		if reference != "" {
			if err := unstructured.SetNestedField(object.Object, reference, "spec", "package", "reference"); err != nil {
				t.Fatalf("build component %s: %v", name, err)
			}
		}
		return object
	}
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(),
		component("demo", "registry/demo-1", map[string]string{"managed-by": "kos"}),
		component("taken", "registry/taken-1", map[string]string{"adopted-by": "owner", "adoption-reason": "existing installation"}),
		component("foreign", "registry/foreign-1", nil),
	)

	err := requireExtensionPackages(context.Background(), client, extension.MemoryArtifacts{})
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 2 {
		t.Fatalf("missing package = %v, want a usage error", err)
	}
	if !strings.Contains(coded.message, "demo") || !strings.Contains(coded.message, "registry/demo-1") {
		t.Fatalf("message %q does not name the unreadable extension", coded.message)
	}
	for _, skipped := range []string{"taken", "foreign"} {
		if strings.Contains(coded.message, skipped) {
			t.Fatalf("message %q names %s, which the judgement does not read", coded.message, skipped)
		}
	}

	covered := extension.MemoryArtifacts{"registry/demo-1": []byte("declaration")}
	if err := requireExtensionPackages(context.Background(), client, covered); err != nil {
		t.Fatalf("a covered extension was refused: %v", err)
	}
}

// TestRunUpgradeRefusesAFormItDoesNotKnow keeps the command surface honest: an
// unknown or missing form is a usage error, not a silent no-op.
func TestRunUpgradeRefusesAFormItDoesNotKnow(t *testing.T) {
	for _, args := range [][]string{{"upgrade"}, {"upgrade", "bogus"}} {
		var out bytes.Buffer
		err := run(args, &out)
		var coded exitError
		if !errors.As(err, &coded) || coded.code != 2 {
			t.Fatalf("run(%v) = %v, want a usage error", args, err)
		}
	}
}

// TestUpgradeFailureCarriesTheFlowMessage keeps the flow's own message, which
// names the node or the application that refused the step (C-55).
func TestUpgradeFailureCarriesTheFlowMessage(t *testing.T) {
	err := upgradeFailure(&controlplane.Error{Code: controlplane.CodeConflict, Message: "node-local 应用仍在运行"})
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 1 || !strings.Contains(coded.message, "node-local") {
		t.Fatalf("failure = %v, want the flow's message at exit code 1", err)
	}
}

// TestUpgradePlanKeepsTheInstallNamespace checks that an upgrade plan names the
// installation's own namespace, not a second one (C-54).
func TestUpgradePlanKeepsTheInstallNamespace(t *testing.T) {
	flags := platformFlags{definitions: "definitions", owner: "owner"}
	plan, err := flags.plan(nil)
	if err != nil {
		t.Fatalf("build the plan: %v", err)
	}
	if plan.Namespace != install.DefaultNamespace || plan.Image != install.DefaultImage {
		t.Fatalf("plan = %+v, want the installation defaults", plan)
	}
}
