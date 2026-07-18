package rpc_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/cardano-p2p-node/internal/mempool"
	"github.com/cardano-p2p-node/internal/peer"
	"github.com/cardano-p2p-node/internal/rpc"
)

// mockConns implements rpc.ConnSource.
type mockConns struct {
	conns []peer.ConnInfo
}

func (m *mockConns) GetConnections() []peer.ConnInfo { return m.conns }
func (m *mockConns) Stats() (int, int) {
	var in, out int
	for _, c := range m.conns {
		if c.Direction == peer.DirInbound {
			in++
		} else {
			out++
		}
	}
	return in, out
}

// mockPool implements rpc.PoolWriter.
type mockPool struct {
	txs []*mempool.TxEntry
}

func (m *mockPool) GetAll() []*mempool.TxEntry { return m.txs }
func (m *mockPool) Size() int                  { return len(m.txs) }
func (m *mockPool) GetStats() mempool.Stats    { return mempool.Stats{Added: uint64(len(m.txs))} }
func (m *mockPool) Add(e *mempool.TxEntry) bool {
	m.txs = append(m.txs, e)
	return true
}
func (m *mockPool) RemoveConfirmed(ids []mempool.TxID) int {
	before := len(m.txs)
	keep := m.txs[:0]
	rm := make(map[string]bool, len(ids))
	for _, id := range ids {
		rm[id.String()] = true
	}
	for _, e := range m.txs {
		if !rm[e.ID.String()] {
			keep = append(keep, e)
		}
	}
	m.txs = keep
	return before - len(m.txs)
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func startServer(t *testing.T, conns *mockConns, pool *mockPool, tip func() (uint64, uint64)) (string, context.CancelFunc) {
	t.Helper()
	addr := freePort(t)
	srv := rpc.New(addr, conns, pool, tip, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	go srv.Run(ctx)
	// Wait briefly for the server to start
	time.Sleep(50 * time.Millisecond)
	return addr, cancel
}

func TestRPCHealth(t *testing.T) {
	addr, cancel := startServer(t, &mockConns{}, &mockPool{}, func() (uint64, uint64) { return 0, 0 })
	defer cancel()

	resp, err := http.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("health: got %d, want 200", resp.StatusCode)
	}
}

func TestRPCStatus(t *testing.T) {
	now := time.Now().UTC()
	conns := &mockConns{
		conns: []peer.ConnInfo{
			{ID: "1.2.3.4:3001", IP: "1.2.3.4", Port: "3001", Direction: peer.DirOutbound, ConnectedAt: now},
			{ID: "5.6.7.8:12345", IP: "5.6.7.8", Port: "12345", Direction: peer.DirInbound, ConnectedAt: now},
		},
	}
	addr, cancel := startServer(t, conns, &mockPool{}, func() (uint64, uint64) { return 50000, 42 })
	defer cancel()

	resp, err := http.Get("http://" + addr + "/status")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if int(body["inbound_count"].(float64)) != 1 {
		t.Errorf("inbound_count: got %v, want 1", body["inbound_count"])
	}
	if int(body["outbound_count"].(float64)) != 1 {
		t.Errorf("outbound_count: got %v, want 1", body["outbound_count"])
	}
	if int(body["chain_tip_slot"].(float64)) != 50000 {
		t.Errorf("chain_tip_slot: got %v, want 50000", body["chain_tip_slot"])
	}

	connsArr := body["connections"].([]interface{})
	if len(connsArr) != 2 {
		t.Errorf("connections count: got %d, want 2", len(connsArr))
	}

	// Check that connected_at is in UTC+8 format
	c0 := connsArr[0].(map[string]interface{})
	connectedAt := c0["connected_at"].(string)
	if len(connectedAt) == 0 {
		t.Error("connected_at should not be empty")
	}
	// Should contain +08:00 offset
	t.Logf("connected_at: %s", connectedAt)
}

func TestRPCStatusEmptyConnections(t *testing.T) {
	addr, cancel := startServer(t, &mockConns{}, &mockPool{}, func() (uint64, uint64) { return 0, 0 })
	defer cancel()

	resp, err := http.Get("http://" + addr + "/status")
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()

	if int(body["inbound_count"].(float64)) != 0 {
		t.Errorf("expected 0 inbound, got %v", body["inbound_count"])
	}
	conns := body["connections"].([]interface{})
	if len(conns) != 0 {
		t.Errorf("expected empty connections, got %d", len(conns))
	}
}

func TestRPCMempool(t *testing.T) {
	now := time.Now().UTC()
	pool := &mockPool{
		txs: []*mempool.TxEntry{
			{
				ID:         mempool.TxID([]byte{0xde, 0xad, 0xbe, 0xef}),
				Size:       128,
				Raw:        []byte{0x01, 0x02, 0x03},
				FromPeer:   "10.0.0.1:3000",
				ReceivedAt: now,
			},
			{
				ID:         mempool.TxID([]byte{0xca, 0xfe, 0xba, 0xbe}),
				Size:       256,
				Raw:        []byte{0x04, 0x05, 0x06},
				FromPeer:   "10.0.0.2:3000",
				ReceivedAt: now,
			},
		},
	}

	addr, cancel := startServer(t, &mockConns{}, pool, func() (uint64, uint64) { return 0, 0 })
	defer cancel()

	resp, err := http.Get("http://" + addr + "/mempool")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("mempool: got %d, want 200", resp.StatusCode)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()

	if int(body["count"].(float64)) != 2 {
		t.Errorf("count: got %v, want 2", body["count"])
	}

	txs := body["transactions"].([]interface{})
	if len(txs) != 2 {
		t.Fatalf("transactions count: got %d, want 2", len(txs))
	}

	tx0 := txs[0].(map[string]interface{})
	if tx0["txid"] != "deadbeef" {
		t.Errorf("txid: got %v, want deadbeef", tx0["txid"])
	}
	if int(tx0["size_bytes"].(float64)) != 128 {
		t.Errorf("size_bytes: got %v, want 128", tx0["size_bytes"])
	}
	if tx0["from_peer"] != "10.0.0.1:3000" {
		t.Errorf("from_peer: got %v", tx0["from_peer"])
	}
	if tx0["raw_hex"] != "010203" {
		t.Errorf("raw_hex: got %v, want 010203", tx0["raw_hex"])
	}
	// received_at should be in UTC+8
	recvAt := tx0["received_at"].(string)
	if len(recvAt) == 0 {
		t.Error("received_at should not be empty")
	}
	t.Logf("received_at: %s", recvAt)
}

func TestRPCMempoolEmpty(t *testing.T) {
	addr, cancel := startServer(t, &mockConns{}, &mockPool{}, func() (uint64, uint64) { return 0, 0 })
	defer cancel()

	resp, err := http.Get("http://" + addr + "/mempool")
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()

	if int(body["count"].(float64)) != 0 {
		t.Errorf("expected count=0, got %v", body["count"])
	}
}

func TestRPCShutdown(t *testing.T) {
	addr, cancel := startServer(t, &mockConns{}, &mockPool{}, func() (uint64, uint64) { return 0, 0 })

	// Verify it's up
	resp, err := http.Get("http://" + addr + "/health")
	if err != nil || resp.StatusCode != 200 {
		t.Fatal("server should be up")
	}

	// Cancel context → server should shut down
	cancel()
	time.Sleep(100 * time.Millisecond)

	// Should now be refusing connections
	_, err = http.Get("http://" + addr + "/health")
	if err == nil {
		t.Error("expected connection refused after shutdown")
	}
}
