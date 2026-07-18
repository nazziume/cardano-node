package mempool_test

import (
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"

	"github.com/cardano-p2p-node/internal/cardano"
	"github.com/cardano-p2p-node/internal/mempool"
)

// buildTxWithTTL creates a TxEntry whose parsed TTL is set to ttlSlot.
func buildTxWithTTL(id string, ttl uint64) *mempool.TxEntry {
	body := map[interface{}]interface{}{
		0: []interface{}{},
		1: []interface{}{},
		2: uint64(170_000),
		3: ttl, // TTL
	}
	bodyBytes, _ := cbor.Marshal(body)
	ws, _ := cbor.Marshal(map[interface{}]interface{}{})
	rawTx, _ := cbor.Marshal([]interface{}{
		cbor.RawMessage(bodyBytes),
		cbor.RawMessage(ws),
		true,
		nil,
	})
	return &mempool.TxEntry{
		ID:         mempool.TxID([]byte(id)),
		Size:       uint32(len(rawTx)),
		Raw:        cbor.RawMessage(rawTx),
		ReceivedAt: time.Now().UTC(),
		Parsed:     cardano.Parse(rawTx),
	}
}

func TestRemoveConfirmed(t *testing.T) {
	pool := mempool.New()

	ids := []string{"tx1", "tx2", "tx3"}
	for _, id := range ids {
		pool.Add(&mempool.TxEntry{ID: mempool.TxID([]byte(id)), Size: 1, Raw: []byte{1}})
	}
	if pool.Size() != 3 {
		t.Fatalf("expected 3 txs, got %d", pool.Size())
	}

	// Remove tx1 and tx3 (as if they appeared in a block)
	removed := pool.RemoveConfirmed([]mempool.TxID{
		mempool.TxID([]byte("tx1")),
		mempool.TxID([]byte("tx3")),
	})
	if removed != 2 {
		t.Errorf("expected 2 removed, got %d", removed)
	}
	if pool.Size() != 1 {
		t.Errorf("expected 1 remaining, got %d", pool.Size())
	}
	if !pool.Has(mempool.TxID([]byte("tx2"))) {
		t.Error("tx2 should still be in pool")
	}
	if pool.Has(mempool.TxID([]byte("tx1"))) {
		t.Error("tx1 should have been removed")
	}
}

func TestRemoveConfirmedIdempotent(t *testing.T) {
	pool := mempool.New()
	pool.Add(&mempool.TxEntry{ID: mempool.TxID([]byte("tx")), Size: 1, Raw: []byte{1}})

	// Remove twice — second removal should return 0
	first := pool.RemoveConfirmed([]mempool.TxID{mempool.TxID([]byte("tx"))})
	second := pool.RemoveConfirmed([]mempool.TxID{mempool.TxID([]byte("tx"))})
	if first != 1 {
		t.Errorf("first removal: got %d, want 1", first)
	}
	if second != 0 {
		t.Errorf("second removal: got %d, want 0", second)
	}
}

func TestPruneTTL(t *testing.T) {
	pool := mempool.New()

	// TTL=100 → expired when slot > 100
	pool.Add(buildTxWithTTL("ttl-expired", 100))
	// TTL=9999 → still valid
	pool.Add(buildTxWithTTL("ttl-valid", 9999))
	// No TTL → never pruned
	pool.Add(&mempool.TxEntry{ID: mempool.TxID([]byte("no-ttl")), Size: 1, Raw: []byte{1}})

	if pool.Size() != 3 {
		t.Fatalf("pre-prune: got %d, want 3", pool.Size())
	}

	pruned := pool.PruneTTL(101) // slot 101 > TTL 100
	if pruned != 1 {
		t.Errorf("pruned: got %d, want 1", pruned)
	}
	if pool.Size() != 2 {
		t.Errorf("post-prune size: got %d, want 2", pool.Size())
	}
	if pool.Has(mempool.TxID([]byte("ttl-expired"))) {
		t.Error("ttl-expired should have been pruned")
	}
	if !pool.Has(mempool.TxID([]byte("ttl-valid"))) {
		t.Error("ttl-valid should still be in pool")
	}
	if !pool.Has(mempool.TxID([]byte("no-ttl"))) {
		t.Error("no-ttl should still be in pool (no TTL field)")
	}
}

func TestPruneTTLZeroSlot(t *testing.T) {
	pool := mempool.New()
	pool.Add(buildTxWithTTL("tx", 0))

	// PruneTTL(0) should be a no-op (slot 0 = unknown)
	pruned := pool.PruneTTL(0)
	if pruned != 0 {
		t.Errorf("expected 0 pruned at slot 0, got %d", pruned)
	}
}

func TestStats(t *testing.T) {
	pool := mempool.New()

	for i := 0; i < 5; i++ {
		pool.Add(&mempool.TxEntry{ID: mempool.TxID([]byte{byte(i)}), Size: 1, Raw: []byte{1}})
	}

	// Remove 2 as confirmed
	pool.RemoveConfirmed([]mempool.TxID{
		mempool.TxID([]byte{0}),
		mempool.TxID([]byte{1}),
	})

	// Prune 1 with TTL
	pool.Add(buildTxWithTTL("expired", 10))
	pool.PruneTTL(20)

	st := pool.GetStats()
	if st.Added != 6 {
		t.Errorf("Added: got %d, want 6", st.Added)
	}
	if st.RemovedConfirmed != 2 {
		t.Errorf("RemovedConfirmed: got %d, want 2", st.RemovedConfirmed)
	}
	if st.RemovedTTL != 1 {
		t.Errorf("RemovedTTL: got %d, want 1", st.RemovedTTL)
	}
}
