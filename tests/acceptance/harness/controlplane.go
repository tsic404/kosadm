package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/kosd/pkg/controlplane"
	"github.com/tsic404/kosd/pkg/markers"
)

// ControlPlane is the API under test, served over the acceptance cluster.
type ControlPlane struct {
	// Server is the API the requests reach.
	Server *controlplane.Server
	// URL is the base address of the running server.
	URL string
}

// StartControlPlane serves the API over the cluster. configure may replace the
// projection source, the authorizer or the clock before the server starts.
func (c *Cluster) StartControlPlane(t *testing.T, configure func(*controlplane.Config)) *ControlPlane {
	t.Helper()
	config := controlplane.Config{Dynamic: c.Dynamic}
	if configure != nil {
		configure(&config)
	}
	server, err := controlplane.New(config)
	if err != nil {
		t.Fatalf("build control plane: %v", err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(func() {
		httpServer.Close()
		server.Close()
	})
	return &ControlPlane{Server: server, URL: httpServer.URL}
}

// Call sends one JSON request and returns the status and decoded body.
func (p *ControlPlane) Call(t *testing.T, method, path string, body any, headers map[string]string) (int, map[string]any) {
	t.Helper()
	status, payload, err := p.Raw(method, path, body, headers)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	decoded := map[string]any{}
	if len(bytes.TrimSpace(payload)) > 0 {
		if err := json.Unmarshal(payload, &decoded); err != nil {
			t.Fatalf("%s %s answered %q: %v", method, path, payload, err)
		}
	}
	return status, decoded
}

// Raw sends one JSON request and returns the status and the body bytes.
func (p *ControlPlane) Raw(method, path string, body any, headers map[string]string) (int, []byte, error) {
	status, _, payload, err := p.RawWithHeaders(method, path, body, headers)
	return status, payload, err
}

// RawWithHeaders sends one JSON request and returns the status, the response
// headers and the body bytes, so a case can assert a download header.
func (p *ControlPlane) RawWithHeaders(method, path string, body any, headers map[string]string) (int, http.Header, []byte, error) {
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, nil, err
		}
		payload = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, p.URL+path, payload)
	if err != nil {
		return 0, nil, nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, nil, nil, err
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return response.StatusCode, response.Header, answer, nil
}

// Eventually waits until check holds, failing the case when the deadline passes.
func Eventually(t *testing.T, timeout time.Duration, description string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, description)
}

// AuditRecorder keeps the audit records the control plane writes (C-14).
type AuditRecorder struct {
	mu      sync.Mutex
	records []controlplane.AuditRecord
}

// Record implements controlplane.Auditor.
func (r *AuditRecorder) Record(_ context.Context, record controlplane.AuditRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, record)
	return nil
}

// Verbs returns the recorded verbs in order.
func (r *AuditRecorder) Verbs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	verbs := make([]string, 0, len(r.records))
	for _, record := range r.records {
		verbs = append(verbs, record.Verb)
	}
	return verbs
}

// Actors returns the recorded actors in order.
func (r *AuditRecorder) Actors() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	actors := make([]string, 0, len(r.records))
	for _, record := range r.records {
		actors = append(actors, record.Actor)
	}
	return actors
}

// Manifest builds a writable object of a kernel kind: identity, attribution and
// an empty spec, with no status and no server-assigned metadata.
func Manifest(kind, namespace, name string, labels, annotations map[string]string) map[string]any {
	metadata := map[string]any{"name": name}
	if namespace != "" {
		metadata["namespace"] = namespace
	}
	if len(labels) > 0 {
		annotated := make(map[string]any, len(labels))
		for key, value := range labels {
			annotated[key] = value
		}
		metadata["labels"] = annotated
	}
	if len(annotations) > 0 {
		annotated := make(map[string]any, len(annotations))
		for key, value := range annotations {
			annotated[key] = value
		}
		metadata["annotations"] = annotated
	}
	return map[string]any{
		"apiVersion": "kos.tsic.top/v1beta1",
		"kind":       kind,
		"metadata":   metadata,
		"spec":       map[string]any{},
	}
}

// Component builds a system component install request naming its package.
func Component(name, reference, digest string) map[string]any {
	manifest := Manifest("SystemComponent", "", name, map[string]string{
		markers.ManagedBy: "kosd",
	}, nil)
	manifest["spec"] = map[string]any{
		"package": map[string]any{"reference": reference, "digest": digest},
	}
	return manifest
}

// Items reads the items of a list response.
func Items(t *testing.T, body map[string]any) []map[string]any {
	t.Helper()
	raw, ok := body["items"].([]any)
	if !ok {
		t.Fatalf("response has no items: %v", body)
	}
	items := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		parsed, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("item is not an object: %v", item)
		}
		items = append(items, parsed)
	}
	return items
}

// Nested reads a value below a chain of map keys.
func Nested(object map[string]any, path ...string) any {
	var value any = object
	for _, key := range path {
		parent, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value = parent[key]
	}
	return value
}

// String reads a nested string, treating a missing value as empty.
func String(object map[string]any, path ...string) string {
	parsed, _ := Nested(object, path...).(string)
	return parsed
}

// Reference renders the identity of a response reference for comparison.
func Reference(object map[string]any) string {
	return fmt.Sprintf("%s/%s/%s/%s",
		String(object, "group"), String(object, "kind"),
		String(object, "namespace"), String(object, "name"))
}
