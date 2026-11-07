// Package install implements the installation forms of C-54: installing into an
// existing cluster, bootstrapping a bare host, and the external-control-plane
// form that runs the control plane outside the cluster it reconciles. It also
// carries what follows an installation: the uninstall with its six-category
// leftover manifest, the read-only rescue reader and the read-only status
// report.
//
// The C-15 install invariant and the C-58 injection list are measured by the
// install run itself: snapshots around the window plus the API server audit
// log. A window whose audit cannot be read is reported unmeasured, never
// passed (ADR-017).
package install
