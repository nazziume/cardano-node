package mempool_test

import (
	"testing"
	"time"

	"github.com/cardano-p2p-node/internal/mempool"
)

func TestMempoolAdd(t *testing.T) {
	pool := mempool.New()

	tx := &mempool.TxEntry{
		ID:   mempool.TxID("txid1"),
		Size: 100,
		Raw:  []byte("tx data"),
	}

	if !pool.Add(tx) {
		t.Error("first add should return true")
	}
	if pool.Add(tx) {
		t.Error("duplicate add should return false")
	}

	if pool.Size() != 1 {
		t.Errorf("size: got %d, want 1", pool.Size())
	}
}

func TestMempoolHas(t *testing.T) {
	pool := mempool.New()

	id := mempool.TxID("mytxid")
	if pool.Has(id) {
		t.Error("has should return false for missing tx")
	}

	pool.Add(&mempool.TxEntry{ID: id, Size: 50, Raw: []byte("tx")})

	if !pool.Has(id) {
		t.Error("has should return true after add")
	}
}

func TestMempoolGet(t *testing.T) {
	pool := mempool.New()
	id := mempool.TxID("gettxid")
	raw := []byte("getdata")

	pool.Add(&mempool.TxEntry{ID: id, Size: uint32(len(raw)), Raw: raw})

	e, ok := pool.Get(id)
	if !ok {
		t.Fatal("get should find existing tx")
	}
	if string(e.Raw) != string(raw) {
		t.Errorf("raw: got %q, want %q", e.Raw, raw)
	}
}

func TestMempoolTxIDsAfter(t *testing.T) {
	pool := mempool.New()

	ids := []string{"tx1", "tx2", "tx3", "tx4", "tx5"}
	for _, id := range ids {
		pool.Add(&mempool.TxEntry{
			ID:   mempool.TxID(id),
			Size: 10,
			Raw:  []byte(id),
		})
	}

	// Get first 3
	entries, newIdx := pool.TxIDsAfter(0, 3)
	if len(entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(entries))
	}
	if newIdx != 3 {
		t.Errorf("newIdx: got %d, want 3", newIdx)
	}

	// Get next 2
	entries2, _ := pool.TxIDsAfter(newIdx, 10)
	if len(entries2) != 2 {
		t.Errorf("expected 2 remaining entries, got %d", len(entries2))
	}
}

func TestMempoolSubscribe(t *testing.T) {
	pool := mempool.New()
	ch := pool.Subscribe()
	defer pool.Unsubscribe(ch)

	done := make(chan struct{})
	go func() {
		select {
		case <-ch:
			close(done)
		case <-time.After(2 * time.Second):
		}
	}()

	pool.Add(&mempool.TxEntry{ID: mempool.TxID("notify"), Size: 1, Raw: []byte("x")})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("expected notification within 2 seconds")
	}
}

func TestMempoolEviction(t *testing.T) {
	// The max size is 10000; create a pool that gets full
	pool := mempool.New()

	// Add 10001 transactions
	for i := 0; i < 10001; i++ {
		id := make([]byte, 8)
		id[0] = byte(i >> 24)
		id[1] = byte(i >> 16)
		id[2] = byte(i >> 8)
		id[3] = byte(i)
		pool.Add(&mempool.TxEntry{
			ID:   mempool.TxID(id),
			Size: 100,
			Raw:  []byte{byte(i)},
		})
	}

	// Pool should not exceed max size
	if pool.Size() > 10000 {
		t.Errorf("pool size exceeded max: %d", pool.Size())
	}
}
