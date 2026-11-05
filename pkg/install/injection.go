package install

import (
	"fmt"
	"strconv"
	"strings"
)

// Installation steps the flow reports. C-58 lets an injection list name these
// names, so a test or an operator reproduces a failure at the same step instead
// of guessing a code path.
const (
	// StepProvision brings the run kernel up on bare hosts (bootstrap only).
	StepProvision = "provision"
	// StepNodes waits for the declared node count (bootstrap only).
	StepNodes = "nodes"
	// StepPreflight verifies the cluster and the declared namespaces.
	StepPreflight = "preflight"
	// StepNamespace creates the installation's own namespace.
	StepNamespace = "namespace"
	// StepDefinitions writes the object definitions.
	StepDefinitions = "definitions"
	// StepPermissions writes the service accounts and cluster-scoped permissions.
	StepPermissions = "permissions"
	// StepNodeAgent writes the per-node agent into the managed cluster.
	StepNodeAgent = "node-agent"
	// StepCredential mints the managed cluster's credential and stores it in the
	// control-plane cluster (external form only).
	StepCredential = "credential"
	// StepControlPlane writes the control-plane workload and service.
	StepControlPlane = "control-plane"
	// StepReady waits for the control plane to answer.
	StepReady = "ready"
	// StepFirstOwner creates the first Owner and its one-time password (C-54).
	StepFirstOwner = "first-owner"
)

// Steps lists every install step in execution order.
func Steps() []string {
	return []string{
		StepProvision, StepNodes, StepPreflight, StepNamespace, StepDefinitions,
		StepPermissions, StepNodeAgent, StepCredential, StepControlPlane, StepReady,
		StepFirstOwner,
	}
}

// Injection is one entry of an injection list (C-58): a step name and the
// attempt that fails.
type Injection struct {
	// Point is one of the step names.
	Point string
	// Hit is the attempt that fails, counting from one.
	Hit int
}

// ParseInjections reads the textual form an installer accepts:
// "control-plane=1,definitions=2". An empty text selects no injection.
func ParseInjections(text string) ([]Injection, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, nil
	}
	injections := make([]Injection, 0, 2)
	for _, entry := range strings.Split(text, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		point, hitText, found := strings.Cut(entry, "=")
		if !found {
			return nil, fmt.Errorf("injection %q names no hit count", entry)
		}
		hit, err := strconv.Atoi(strings.TrimSpace(hitText))
		if err != nil || hit < 1 {
			return nil, fmt.Errorf("injection %q needs a hit count of at least one", entry)
		}
		injections = append(injections, Injection{Point: strings.TrimSpace(point), Hit: hit})
	}
	return injections, ValidateInjections(injections)
}

// ValidateInjections refuses an entry that names a step the flow never reports,
// because such an entry would silently inject nothing.
func ValidateInjections(injections []Injection) error {
	known := make(map[string]bool, len(Steps()))
	for _, step := range Steps() {
		known[step] = true
	}
	seen := make(map[string]bool, len(injections))
	for _, injection := range injections {
		if !known[injection.Point] {
			return fmt.Errorf("injection point %q is not an install step", injection.Point)
		}
		if injection.Hit < 1 {
			return fmt.Errorf("injection point %q has hit %d; it must be at least one", injection.Point, injection.Hit)
		}
		if seen[injection.Point] {
			return fmt.Errorf("injection list names step %q more than once", injection.Point)
		}
		seen[injection.Point] = true
	}
	return nil
}

// Injector counts the attempts at each step and fails the configured one. The
// same list always fails at the same step because the step order is fixed
// (C-58).
type Injector struct {
	hit    map[string]int
	counts map[string]int
}

// NewInjector builds the counter of one installation run.
func NewInjector(injections []Injection) (*Injector, error) {
	if err := ValidateInjections(injections); err != nil {
		return nil, err
	}
	hit := make(map[string]int, len(injections))
	for _, injection := range injections {
		hit[injection.Point] = injection.Hit
	}
	return &Injector{hit: hit, counts: map[string]int{}}, nil
}

// At records one attempt at step and reports the injected failure.
func (i *Injector) At(step string) error {
	if i == nil {
		return nil
	}
	i.counts[step]++
	if hit, injected := i.hit[step]; injected && i.counts[step] == hit {
		return &InjectedFailure{Point: step, Attempt: hit}
	}
	return nil
}

// InjectedFailure is the deterministic failure of an injection list (C-58).
type InjectedFailure struct {
	// Point is the step that failed.
	Point string
	// Attempt is the attempt number that failed.
	Attempt int
}

// Error implements error.
func (f *InjectedFailure) Error() string {
	return fmt.Sprintf("injected failure at %q on attempt %d", f.Point, f.Attempt)
}
