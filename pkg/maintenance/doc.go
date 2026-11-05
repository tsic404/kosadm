// Package maintenance drives the node maintenance flow of C-65: preflight,
// cordon, stop-and-drain and restore.
//
// The state the flow moves is the run kernel's own node object — the
// unschedulable field — so no kernel object and no metadata contract is added.
// The control-plane API process is the writer of that field on an Owner's
// action; the applications bound to the node stop themselves when their
// reconciliation observes it, and the drain only removes what the run kernel
// may still move.
//
// kosadm owns this flow and the control-plane API serves the same steps; kosd
// keeps the reconciler side: an application bound to an unschedulable node
// stops itself and returns once the node is schedulable again (C-65).
package maintenance
