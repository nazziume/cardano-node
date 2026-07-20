package ledger_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/cardano-p2p-node/internal/ledger"
)

// serveSnapshot creates a test HTTP server that returns the given snapshot JSON.
func serveSnapshot(t *testing.T, data string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(json.RawMessage(data))
	}))
}

func TestManagerFetchAndResolve(t *testing.T) {
	// Snapshot with only IP addresses (no DNS needed) for deterministic testing.
	snap := `{
	  "version": 2,
	  "slotNo": 100,
	  "bigLedgerPools": [
	    { "accumulatedStake": 0.01, "relativeStake": 0.005,
	      "relays": [{ "address": "127.0.0.1", "port": 3001 },
	                 { "address": "127.0.0.2", "port": 3002 }] },
	    { "accumulatedStake": 0.03, "relativeStake": 0.02,
	      "relays": [{ "address": "127.0.0.3", "port": 3003 }] }
	  ]
	}`

	srv := serveSnapshot(t, snap)
	defer srv.Close()

	received := make(chan []ledger.ResolvedPeer, 1)
	mgr := ledger.NewManager(ledger.Config{
		SnapshotURL:     srv.URL,
		RefreshInterval: time.Hour, // no auto-refresh during test
		MaxPeers:        100,
	}, zap.NewNop())

	mgr.OnPeers = func(peers []ledger.ResolvedPeer) {
		received <- peers
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go mgr.Run(ctx)

	select {
	case peers := <-received:
		if len(peers) != 3 {
			t.Errorf("expected 3 peers, got %d: %+v", len(peers), peers)
		}
		// Verify addresses
		addrs := make([]string, len(peers))
		for i, p := range peers {
			addrs[i] = p.Addr
		}
		sort.Strings(addrs)
		want := []string{"127.0.0.1:3001", "127.0.0.2:3002", "127.0.0.3:3003"}
		sort.Strings(want)
		for i := range want {
			if addrs[i] != want[i] {
				t.Errorf("addr[%d]: got %q, want %q", i, addrs[i], want[i])
			}
		}

	case <-ctx.Done():
		t.Fatal("timeout waiting for ledger peers")
	}
}

func TestManagerMaxPeers(t *testing.T) {
	snap := `{
	  "version": 2,
	  "slotNo": 100,
	  "bigLedgerPools": [
	    { "accumulatedStake": 0.01, "relativeStake": 0.01,
	      "relays": [{ "address": "10.0.0.1", "port": 3000 },
	                 { "address": "10.0.0.2", "port": 3000 },
	                 { "address": "10.0.0.3", "port": 3000 }] },
	    { "accumulatedStake": 0.02, "relativeStake": 0.01,
	      "relays": [{ "address": "10.0.0.4", "port": 3000 },
	                 { "address": "10.0.0.5", "port": 3000 }] }
	  ]
	}`

	srv := serveSnapshot(t, snap)
	defer srv.Close()

	received := make(chan []ledger.ResolvedPeer, 1)
	mgr := ledger.NewManager(ledger.Config{
		SnapshotURL:     srv.URL,
		RefreshInterval: time.Hour,
		MaxPeers:        2, // cap at 2
	}, zap.NewNop())
	mgr.OnPeers = func(peers []ledger.ResolvedPeer) { received <- peers }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go mgr.Run(ctx)

	select {
	case peers := <-received:
		if len(peers) != 2 {
			t.Errorf("expected 2 peers (maxPeers=2), got %d", len(peers))
		}
	case <-ctx.Done():
		t.Fatal("timeout")
	}
}

func TestManagerHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	notified := make(chan struct{}, 1)
	mgr := ledger.NewManager(ledger.Config{
		SnapshotURL:     srv.URL,
		RefreshInterval: time.Hour,
	}, zap.NewNop())
	mgr.OnPeers = func(_ []ledger.ResolvedPeer) { notified <- struct{}{} }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go mgr.Run(ctx)

	// OnPeers should NOT be called on HTTP error
	select {
	case <-notified:
		t.Error("OnPeers should not be called after HTTP error")
	case <-ctx.Done():
		// expected: timeout with no notification
	}
}

func TestManagerNoPeers(t *testing.T) {
	// Peers snapshot that resolves to empty (SRV-only relays, which we skip)
	snap := `{
	  "version": 2,
	  "slotNo": 100,
	  "bigLedgerPools": [
	    { "accumulatedStake": 0.01, "relativeStake": 0.01,
	      "relays": [{ "address": "_cardano._tcp.pool.example.com", "port": 0 }] }
	  ]
	}`

	srv := serveSnapshot(t, snap)
	defer srv.Close()

	notified := make(chan struct{}, 1)
	mgr := ledger.NewManager(ledger.Config{
		SnapshotURL:     srv.URL,
		RefreshInterval: time.Hour,
	}, zap.NewNop())
	mgr.OnPeers = func(_ []ledger.ResolvedPeer) { notified <- struct{}{} }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go mgr.Run(ctx)

	select {
	case <-notified:
		// SRV ports (port=0) are skipped, so empty result → no notification
		t.Error("OnPeers should not be called with empty result")
	case <-ctx.Done():
	}
}

func TestManagerDeduplication(t *testing.T) {
	// Two pools advertising the same relay
	snap := `{
	  "version": 2,
	  "slotNo": 100,
	  "bigLedgerPools": [
	    { "accumulatedStake": 0.01, "relativeStake": 0.01,
	      "relays": [{ "address": "10.1.1.1", "port": 3000 }] },
	    { "accumulatedStake": 0.02, "relativeStake": 0.01,
	      "relays": [{ "address": "10.1.1.1", "port": 3000 }] }
	  ]
	}`

	srv := serveSnapshot(t, snap)
	defer srv.Close()

	received := make(chan []ledger.ResolvedPeer, 1)
	mgr := ledger.NewManager(ledger.Config{
		SnapshotURL:     srv.URL,
		RefreshInterval: time.Hour,
		MaxPeers:        100,
	}, zap.NewNop())
	mgr.OnPeers = func(peers []ledger.ResolvedPeer) { received <- peers }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go mgr.Run(ctx)

	select {
	case peers := <-received:
		if len(peers) != 1 {
			t.Errorf("expected 1 unique peer (deduplication), got %d", len(peers))
		}
	case <-ctx.Done():
		t.Fatal("timeout")
	}
}

func TestManagerStakeOrdering(t *testing.T) {
	// Pool B has higher stake than Pool A; Pool B's relay should come first.
	snap := `{
	  "version": 2,
	  "slotNo": 100,
	  "bigLedgerPools": [
	    { "accumulatedStake": 0.01, "relativeStake": 0.005,
	      "relays": [{ "address": "10.0.0.1", "port": 3000 }] },
	    { "accumulatedStake": 0.05, "relativeStake": 0.040,
	      "relays": [{ "address": "10.0.0.2", "port": 3000 }] }
	  ]
	}`

	srv := serveSnapshot(t, snap)
	defer srv.Close()

	received := make(chan []ledger.ResolvedPeer, 1)
	mgr := ledger.NewManager(ledger.Config{
		SnapshotURL:     srv.URL,
		RefreshInterval: time.Hour,
		MaxPeers:        100,
	}, zap.NewNop())
	mgr.OnPeers = func(peers []ledger.ResolvedPeer) { received <- peers }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go mgr.Run(ctx)

	select {
	case peers := <-received:
		if len(peers) < 2 {
			t.Fatalf("expected 2 peers, got %d", len(peers))
		}
		// Higher stake (0.040) should be first
		if peers[0].RelativeStake < peers[1].RelativeStake {
			t.Errorf("peers not sorted by stake: [0]=%f [1]=%f",
				peers[0].RelativeStake, peers[1].RelativeStake)
		}
	case <-ctx.Done():
		t.Fatal("timeout")
	}
}

// snapshotIPOnly has only IP addresses (no DNS) for deterministic local-file tests.
const snapshotIPOnly = `{
  "version": 2,
  "slotNo": 999,
  "bigLedgerPools": [
    { "accumulatedStake": 0.01, "relativeStake": 0.005,
      "relays": [{ "address": "192.168.0.1", "port": 3001 },
                 { "address": "192.168.0.2", "port": 3002 }] },
    { "accumulatedStake": 0.02, "relativeStake": 0.01,
      "relays": [{ "address": "192.168.0.3", "port": 3003 }] }
  ]
}`

func TestManagerLocalFile(t *testing.T) {
	f, err := writeTemp(t, snapshotIPOnly)
	if err != nil {
		t.Fatal(err)
	}

	received := make(chan []ledger.ResolvedPeer, 1)
	mgr := ledger.NewManager(ledger.Config{
		SnapshotFile:    f,
		RefreshInterval: time.Hour,
		MaxPeers:        100,
	}, zap.NewNop())
	mgr.OnPeers = func(peers []ledger.ResolvedPeer) { received <- peers }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go mgr.Run(ctx)

	select {
	case peers := <-received:
		if len(peers) != 3 {
			t.Errorf("expected 3 peers from local file, got %d: %+v", len(peers), peers)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for peers from local file")
	}
}

func writeTemp(t *testing.T, content string) (string, error) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "snapshot*.json")
	if err != nil {
		return "", err
	}
	defer f.Close()
	_, err = f.WriteString(content)
	return f.Name(), err
}
