package main

import (
	"bytes"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/tsic404/kosadm/pkg/install"
	"github.com/tsic404/kosd/api/v1beta1"
)

// TestUninstallRefusesToDeleteData pins the command's one user-visible refusal:
// the uninstall never offers a one-step wipe, and it names the volume path that
// is allowed to delete a claim. The refusal happens before any cluster access,
// so the case needs no API server.
func TestUninstallRefusesToDeleteData(t *testing.T) {
	err := runUninstall([]string{"--data", "delete"}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("the uninstall accepted --data delete")
	}
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 2 {
		t.Fatalf("error = %v, want a usage failure", err)
	}
	if !strings.Contains(coded.message, "volume path") {
		t.Fatalf("message = %q, want the volume path named", coded.message)
	}
	if strings.Contains(coded.message, "retain or delete") {
		t.Fatalf("message = %q still offers delete", coded.message)
	}
	// --help must not advertise the value the command refuses.
	set := flag.NewFlagSet("uninstall", flag.ContinueOnError)
	declareUninstallFlags(set)
	data := set.Lookup("data")
	if data == nil {
		t.Fatal("the uninstall declares no --data flag")
	}
	if !strings.Contains(data.Usage, "retain") || !strings.Contains(data.Usage, "volume path") {
		t.Fatalf("--data help = %q, want the retention option and the volume path", data.Usage)
	}
	if strings.Contains(data.Usage, "delete") {
		t.Fatalf("--data help = %q still advertises delete", data.Usage)
	}
}

// TestUninstallRejectsAnUnknownRetentionOption pins the accepted values: only
// the retention option is offered, so the hint cannot promise a value the
// command refuses.
func TestUninstallRejectsAnUnknownRetentionOption(t *testing.T) {
	err := runUninstall([]string{"--data", "wipe"}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("the uninstall accepted an unknown data option")
	}
	var coded exitError
	if !errors.As(err, &coded) || coded.code != 2 {
		t.Fatalf("error = %v, want a usage failure", err)
	}
	if !strings.Contains(coded.message, "use retain") || strings.Contains(coded.message, "or delete") {
		t.Fatalf("message = %q, want only the offered value", coded.message)
	}
}

// TestUninstallManifestNeverClaimsAFinishedRunItDidNotReach pins the two
// reports apart: a finished run prints its finish time, a stopped run says so
// and still lists what it had collected.
func TestUninstallManifestNeverClaimsAFinishedRunItDidNotReach(t *testing.T) {
	manifest := install.Manifest{
		GeneratedAt: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC),
		Entries: []install.Leftover{{
			Category:    install.LeftoverReleasedClaim,
			Object:      v1beta1.ObjectRef{Version: "v1", Kind: "PersistentVolumeClaim", Namespace: "default", Name: "data"},
			Disposition: install.DispositionRetained,
		}},
	}

	var finished bytes.Buffer
	printManifest(&finished, manifest)
	if !strings.HasPrefix(finished.String(), "Finished: ") {
		t.Fatalf("finished manifest = %q, want a finish time", finished.String())
	}
	if !strings.Contains(finished.String(), "default/data") {
		t.Fatalf("finished manifest = %q, want the collected entry", finished.String())
	}

	var stopped bytes.Buffer
	printIncomplete(&stopped, manifest)
	if strings.Contains(stopped.String(), "Finished:") {
		t.Fatalf("stopped manifest = %q, want no finish time", stopped.String())
	}
	if !strings.Contains(stopped.String(), "Incomplete") {
		t.Fatalf("stopped manifest = %q, want the run marked incomplete", stopped.String())
	}
	if !strings.Contains(stopped.String(), "default/data") {
		t.Fatalf("stopped manifest = %q, want the entries collected before the stop", stopped.String())
	}
}
