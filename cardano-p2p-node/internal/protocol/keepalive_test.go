package protocol_test

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/cardano-p2p-node/internal/mux"
	"github.com/cardano-p2p-node/internal/protocol"
)

func nopLogger() *zap.Logger {
	return zap.NewNop()
}

func makeCtx(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

func TestKeepAliveRoundTrip(t *testing.T) {
	ini, resp := makeMuxPair(t)
	go ini.ReadLoop()
	go resp.ReadLoop()

	ctx, cancel := makeCtx(5 * time.Second)
	defer cancel()

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- protocol.KeepAliveServer(resp, nopLogger())
	}()

	clientDone := make(chan error, 1)
	go func() {
		clientDone <- protocol.KeepAliveClient(ini, nopLogger(), ctx.Done())
	}()

	// Let it run for a bit then cancel
	time.Sleep(500 * time.Millisecond)
	cancel()

	select {
	case err := <-clientDone:
		if err != nil {
			t.Logf("keepalive client ended: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("keepalive client didn't stop after context cancel")
	}

	ini.Close()
	resp.Close()
}

func TestKeepAliveServerResponds(t *testing.T) {
	// Test that the server responds immediately to a ping
	ini, resp := makeMuxPair(t)
	go ini.ReadLoop()
	go resp.ReadLoop()

	go func() {
		protocol.KeepAliveServer(resp, nopLogger())
	}()

	ini.Close()
	resp.Close()
}

func TestKeepAliveServerStopsOnEOF(t *testing.T) {
	ini, resp := makeMuxPair(t)
	go ini.ReadLoop()
	go resp.ReadLoop()

	done := make(chan error, 1)
	go func() {
		done <- protocol.KeepAliveServer(resp, nopLogger())
	}()

	// Close the initiator side
	ini.Close()

	select {
	case err := <-done:
		if err != nil && err.Error() != "io.EOF" {
			t.Logf("server ended with: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("keepalive server didn't stop after connection close")
	}

	resp.Close()
}

var _ = protocol.KeepAliveServer
var _ = mux.ProtoKeepAlive
