package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tsic404/kosd/pkg/identity"
)

// Clock is a movable clock for cases about locks and session bounds.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock starts at a fixed instant.
func NewClock(at time.Time) *Clock { return &Clock{now: at} }

// Now reads the clock.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward.
func (c *Clock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

// Identity is the account and session surface under test.
type Identity struct {
	// Store is the control-plane database.
	Store *identity.Store
	// Service is the account service.
	Service *identity.Service
	// API is the HTTP surface.
	API *identity.API
	// HTTP calls the running server.
	HTTP *HTTP
	// Clock is the service clock.
	Clock *Clock
	// Trail is the audit stream every write of the composed shell appends to
	// (C-14); it keeps the records in the account database.
	Trail identity.AuditTrail
	// DatabasePath is the database file the case may export and restore.
	DatabasePath string
}

// StartIdentity opens a private database and serves the account surface over
// next. next may be nil when the case needs only the identity routes.
func StartIdentity(t *testing.T, next http.Handler, owned identity.OwnedObjects) *Identity {
	t.Helper()
	surface := startIdentity(t, owned, nil)
	surface.Serve(t, next)
	return surface
}

// OpenIdentity opens a private database without serving it, so a composed
// surface can mount the account middleware in front of its own routes.
func OpenIdentity(t *testing.T, owned identity.OwnedObjects, configure func(*identity.Config)) *Identity {
	t.Helper()
	return startIdentity(t, owned, configure)
}

// startIdentity opens the private database and builds the account service.
func startIdentity(t *testing.T, owned identity.OwnedObjects, configure func(*identity.Config)) *Identity {
	t.Helper()
	clock := NewClock(time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC))
	databasePath := filepath.Join(t.TempDir(), "kos.db")
	store, err := identity.Open(context.Background(), identity.OpenOptions{Path: databasePath, Now: clock.Now})
	if err != nil {
		t.Fatalf("open identity database: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	trail := identity.AuditTrail{Store: store}
	config := identity.Config{Now: clock.Now, Owned: owned, Auditor: trail}
	if configure != nil {
		configure(&config)
	}
	service := identity.New(store, config)
	return &Identity{
		Store:        store,
		Service:      service,
		API:          identity.NewAPI(service),
		Clock:        clock,
		Trail:        trail,
		DatabasePath: databasePath,
	}
}

// Serve mounts the account surface in front of next.
func (i *Identity) Serve(t *testing.T, next http.Handler) {
	t.Helper()
	server := httptest.NewServer(i.API.Handler(next))
	t.Cleanup(server.Close)
	i.HTTP = &HTTP{URL: server.URL}
}

// BootstrapOwner creates the first Owner and returns its one-time password.
func (i *Identity) BootstrapOwner(t *testing.T, username string) string {
	t.Helper()
	bootstrap, err := i.Service.BootstrapOwner(context.Background(), username)
	if err != nil {
		t.Fatalf("bootstrap owner: %v", err)
	}
	if !bootstrap.Created || bootstrap.OneTimePassword == "" {
		t.Fatalf("bootstrap = %+v, want a created owner with a one-time password", bootstrap)
	}
	return bootstrap.OneTimePassword
}

// SignIn logs in and returns the bearer token and the decoded answer.
func (i *Identity) SignIn(t *testing.T, username, password string) (string, map[string]any) {
	t.Helper()
	status, body := i.HTTP.Call(t, http.MethodPost, "/api/v1/auth/login", map[string]any{
		"username": username, "password": password,
	}, nil)
	if status != http.StatusOK {
		t.Fatalf("login %s = %d %v", username, status, body)
	}
	token, _ := body["token"].(string)
	if token == "" {
		t.Fatalf("login %s returned no token: %v", username, body)
	}
	return token, body
}

// ChangePassword replaces the password behind a session.
func (i *Identity) ChangePassword(t *testing.T, token, current, next string) {
	t.Helper()
	status, body := i.HTTP.Call(t, http.MethodPut, "/api/v1/auth/password", map[string]any{
		"currentPassword": current, "newPassword": next,
	}, map[string]string{"Authorization": "Bearer " + token})
	if status != http.StatusNoContent {
		t.Fatalf("change password = %d %v", status, body)
	}
}

// HTTP is a running HTTP surface the cases call.
type HTTP struct {
	// URL is the base address.
	URL string
}

// Call sends one JSON request and returns the status and decoded body.
func (h *HTTP) Call(t *testing.T, method, path string, body any, headers map[string]string) (int, map[string]any) {
	t.Helper()
	status, payload, err := h.Raw(method, path, body, headers)
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

// Raw sends one JSON request and returns the status and body bytes.
func (h *HTTP) Raw(method, path string, body any, headers map[string]string) (int, []byte, error) {
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		payload = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, h.URL+path, payload)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, nil, err
	}
	return response.StatusCode, answer, nil
}

// ServeEverywhere serves the account surface on every interface and returns the
// address another host reaches it at. The loopback URL stays in HTTP: the claim
// route tells the control-plane host from every other source by the transport
// address, so a case needs both (C-68).
func (i *Identity) ServeEverywhere(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen on every interface: %v", err)
	}
	server := httptest.NewUnstartedServer(i.API.Handler(nil))
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	port := listener.Addr().(*net.TCPAddr).Port
	i.HTTP = &HTTP{URL: fmt.Sprintf("http://127.0.0.1:%d", port)}
	return fmt.Sprintf("http://%s:%d", NonLoopbackAddress(t), port)
}

// NonLoopbackAddress is this host's first usable IPv4 address, the address a
// request from another host appears to come from.
func NonLoopbackAddress(t *testing.T) string {
	t.Helper()
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Fatalf("list interfaces: %v", err)
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}
		addresses, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, address := range addresses {
			ip, _, err := net.ParseCIDR(address.String())
			if err != nil || ip.IsLoopback() || ip.To4() == nil {
				continue
			}
			return ip.String()
		}
	}
	// The source rule is covered by the identity package's own unit test; a
	// host with only a loopback interface cannot tell a remote request from a
	// local one, so the case skips instead of failing on the host it runs on.
	t.Skip("this host has no non-loopback IPv4 address to distinguish a remote claim")
	return ""
}

// Restart reopens the account database the way a control-plane restart does:
// the recorded claim window, its deadline and the accounts survive, and every
// surface is served by a new service. The clock is the same one, so a case can
// compare the deadline across the restart (C-68).
func (i *Identity) Restart(t *testing.T) *Identity {
	t.Helper()
	if err := i.Store.Close(); err != nil {
		t.Fatalf("close the account database: %v", err)
	}
	store, err := identity.Open(context.Background(), identity.OpenOptions{Path: i.DatabasePath, Now: i.Clock.Now})
	if err != nil {
		t.Fatalf("reopen the account database: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	trail := identity.AuditTrail{Store: store}
	service := identity.New(store, identity.Config{Now: i.Clock.Now, Auditor: trail})
	return &Identity{
		Store:        store,
		Service:      service,
		API:          identity.NewAPI(service),
		HTTP:         i.HTTP,
		Clock:        i.Clock,
		Trail:        trail,
		DatabasePath: i.DatabasePath,
	}
}

// DiscardDatabase closes the control-plane database and removes it, the way an
// uninstall that keeps no control-plane database leaves the next installation
// with nothing (C-68), then reopens an empty one. The clock and the path stay
// the same, so a case can compare the reinstallation's window with the one the
// discarded database held.
func (i *Identity) DiscardDatabase(t *testing.T) *Identity {
	t.Helper()
	if err := i.Store.Close(); err != nil {
		t.Fatalf("close the account database: %v", err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(i.DatabasePath + suffix); err != nil && !os.IsNotExist(err) {
			t.Fatalf("discard the account database: %v", err)
		}
	}
	store, err := identity.Open(context.Background(), identity.OpenOptions{Path: i.DatabasePath, Now: i.Clock.Now})
	if err != nil {
		t.Fatalf("open an empty account database: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	trail := identity.AuditTrail{Store: store}
	service := identity.New(store, identity.Config{Now: i.Clock.Now, Auditor: trail})
	return &Identity{
		Store:        store,
		Service:      service,
		API:          identity.NewAPI(service),
		HTTP:         i.HTTP,
		Clock:        i.Clock,
		Trail:        trail,
		DatabasePath: i.DatabasePath,
	}
}
