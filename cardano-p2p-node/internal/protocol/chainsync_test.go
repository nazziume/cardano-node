package protocol_test

import (
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"

	"github.com/cardano-p2p-node/internal/chain"
	"github.com/cardano-p2p-node/internal/protocol"
)

// TestChainSyncClientServer tests the end-to-end chain sync protocol:
// - Server has some headers in its store
// - Client connects and retrieves them
// - When new headers arrive, client receives them immediately
func TestChainSyncClientServer(t *testing.T) {
	// Set up server store with some initial headers
	serverStore := chain.New()
	for i := 0; i < 5; i++ {
		h := makeTestHeader(uint64(1000+i*100), []byte{byte(i)})
		serverStore.AddHeader(h)
	}

	// Client store starts empty
	clientStore := chain.New()

	ini, resp := makeMuxPair(t)
	go ini.ReadLoop()
	go resp.ReadLoop()

	ctx, cancel := makeCtx(2 * time.Second)
	defer cancel()

	serverLog := nopLogger()
	clientLog := nopLogger()

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- protocol.ChainSyncServer(resp, serverStore, serverLog, ctx.Done())
	}()

	// Client should receive the headers
	clientDone := make(chan error, 1)
	go func() {
		clientDone <- protocol.ChainSyncClient(ini, clientStore, clientLog, ctx.Done())
	}()

	// Wait for client to sync the headers
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()

	for {
		headers := clientStore.AllHeaders()
		if len(headers) >= 5 {
			t.Logf("client synced %d headers", len(headers))
			break
		}
		select {
		case <-deadline.C:
			t.Errorf("timeout: client only synced %d/5 headers", len(clientStore.AllHeaders()))
			cancel()
			return
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Add a new header to server, client should receive it quickly
	newHeader := makeTestHeader(1500, []byte{99})
	serverStore.AddHeader(newHeader)

	deadline2 := time.NewTimer(2 * time.Second)
	defer deadline2.Stop()

	for {
		headers := clientStore.AllHeaders()
		if len(headers) >= 6 {
			t.Log("client received live header")
			break
		}
		select {
		case <-deadline2.C:
			t.Errorf("timeout: client didn't receive live header, got %d headers", len(clientStore.AllHeaders()))
			cancel()
			return
		case <-time.After(50 * time.Millisecond):
		}
	}

	cancel()
	ini.Close()
	resp.Close()
}

func makeTestHeader(slot uint64, hash []byte) chain.Header {
	raw, _ := cbor.Marshal(map[string]interface{}{"slot": slot, "hash": hash})
	tip, _ := cbor.Marshal([]interface{}{
		[]interface{}{slot, hash},
		slot / 100,
	})
	return chain.Header{
		Point:  chain.Point{SlotNo: slot, Hash: hash},
		Raw:    cbor.RawMessage(raw),
		TipRaw: cbor.RawMessage(tip),
		No:     slot / 100,
	}
}
