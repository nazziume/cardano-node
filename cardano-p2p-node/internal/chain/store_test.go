package chain_test

import (
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"

	"github.com/cardano-p2p-node/internal/chain"
)

func makeHeader(slot uint64, hash []byte) chain.Header {
	raw, _ := cbor.Marshal(map[string]interface{}{"slot": slot})
	tip, _ := cbor.Marshal([]interface{}{[]interface{}{slot, hash}, slot / 100})
	return chain.Header{
		Point:  chain.Point{SlotNo: slot, Hash: hash},
		Raw:    cbor.RawMessage(raw),
		TipRaw: cbor.RawMessage(tip),
		No:     slot / 100,
	}
}

func TestStoreAddAndTip(t *testing.T) {
	s := chain.New()

	tip, _ := s.Tip()
	if !tip.IsOrigin() {
		t.Error("expected origin tip")
	}

	h1 := makeHeader(1000, []byte("hash1"))
	s.AddHeader(h1)

	tip, blockNo := s.Tip()
	if tip.SlotNo != 1000 {
		t.Errorf("tip slot: got %d, want 1000", tip.SlotNo)
	}
	if blockNo != 10 {
		t.Errorf("block no: got %d, want 10", blockNo)
	}
}

func TestStoreFindIntersect(t *testing.T) {
	s := chain.New()

	hashes := [][]byte{
		[]byte("hash-a"),
		[]byte("hash-b"),
		[]byte("hash-c"),
	}

	for i, h := range hashes {
		s.AddHeader(makeHeader(uint64(1000+i*100), h))
	}

	// Find exact point
	candidates := []chain.Point{
		{SlotNo: 1100, Hash: []byte("hash-b")},
	}
	found, after := s.FindIntersect(candidates)
	if found == nil {
		t.Fatal("expected intersection to be found")
	}
	if found.SlotNo != 1100 {
		t.Errorf("found slot: got %d, want 1100", found.SlotNo)
	}
	if len(after) != 1 {
		t.Errorf("headers after: got %d, want 1", len(after))
	}
	if after[0].Point.SlotNo != 1200 {
		t.Errorf("after[0] slot: got %d, want 1200", after[0].Point.SlotNo)
	}
}

func TestStoreFindIntersectOrigin(t *testing.T) {
	s := chain.New()
	s.AddHeader(makeHeader(1000, []byte("hash1")))

	// Origin should always match
	candidates := []chain.Point{{}} // zero value = origin
	found, after := s.FindIntersect(candidates)
	if found == nil {
		t.Fatal("expected origin to match")
	}
	if len(after) != 1 {
		t.Errorf("expected 1 header after origin, got %d", len(after))
	}
}

func TestStoreFindIntersectNotFound(t *testing.T) {
	s := chain.New()
	s.AddHeader(makeHeader(1000, []byte("hash1")))

	candidates := []chain.Point{
		{SlotNo: 9999, Hash: []byte("unknown")},
	}
	found, _ := s.FindIntersect(candidates)
	if found != nil {
		t.Error("expected no intersection found")
	}
}

func TestStoreSubscribeNotification(t *testing.T) {
	s := chain.New()
	ch := s.Subscribe()
	defer s.Unsubscribe(ch)

	done := make(chan struct{})
	go func() {
		select {
		case <-ch:
			close(done)
		case <-time.After(2 * time.Second):
		}
	}()

	s.AddHeader(makeHeader(1000, []byte("hash1")))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("expected notification within 2 seconds")
	}
}

func TestStoreMultipleSubscribers(t *testing.T) {
	s := chain.New()
	n := 10
	channels := make([]chan struct{}, n)
	for i := range channels {
		channels[i] = s.Subscribe()
	}

	s.AddHeader(makeHeader(2000, []byte("newhash")))

	for i, ch := range channels {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Errorf("subscriber %d not notified", i)
		}
		s.Unsubscribe(ch)
	}
}

func TestStoreRingBuffer(t *testing.T) {
	s := chain.New()

	// Add more headers than the ring size (256)
	for i := 0; i < 300; i++ {
		h := makeHeader(uint64(i*100), []byte{byte(i >> 8), byte(i)})
		s.AddHeader(h)
	}

	// The ring should have exactly 256 entries
	all := s.AllHeaders()
	if len(all) > 256 {
		t.Errorf("ring buffer overflow: got %d headers, want <= 256", len(all))
	}
	// Most recent headers should be present
	last := all[len(all)-1]
	if last.Point.SlotNo != 299*100 {
		t.Errorf("last slot: got %d, want %d", last.Point.SlotNo, 299*100)
	}
}

func TestStoreBlock(t *testing.T) {
	s := chain.New()
	hash := []byte("blockhash")
	raw, _ := cbor.Marshal("block data")

	s.AddBlock(chain.Block{
		Point: chain.Point{SlotNo: 5000, Hash: hash},
		Raw:   cbor.RawMessage(raw),
	})

	b, ok := s.GetBlock(hash)
	if !ok {
		t.Fatal("expected block to be found")
	}
	if b.Point.SlotNo != 5000 {
		t.Errorf("block slot: got %d, want 5000", b.Point.SlotNo)
	}
}
