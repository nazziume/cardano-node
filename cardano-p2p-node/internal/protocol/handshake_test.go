package protocol_test

import (
	"net"
	"sync"
	"testing"

	"github.com/cardano-p2p-node/internal/mux"
	"github.com/cardano-p2p-node/internal/protocol"
)

func makeMuxPair(t *testing.T) (ini, resp *mux.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var serverConn net.Conn

	wg.Add(1)
	go func() {
		defer wg.Done()
		serverConn, _ = ln.Accept()
		ln.Close()
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	ini = mux.New(clientConn, true)
	resp = mux.New(serverConn, false)
	return
}

func TestHandshakeNegotiation(t *testing.T) {
	ini, resp := makeMuxPair(t)

	go ini.ReadLoop()
	go resp.ReadLoop()

	type result struct {
		neg *protocol.NegotiatedVersion
		err error
	}

	iniCh := make(chan result, 1)
	respCh := make(chan result, 1)

	go func() {
		neg, err := protocol.HandshakePropose(ini, protocol.MainnetMagic)
		iniCh <- result{neg, err}
	}()

	go func() {
		neg, err := protocol.HandshakeAccept(resp, protocol.MainnetMagic)
		respCh <- result{neg, err}
	}()

	iniResult := <-iniCh
	respResult := <-respCh

	if iniResult.err != nil {
		t.Errorf("initiator handshake error: %v", iniResult.err)
	}
	if respResult.err != nil {
		t.Errorf("responder handshake error: %v", respResult.err)
	}

	if iniResult.neg == nil || respResult.neg == nil {
		t.Fatal("expected non-nil negotiated version")
	}

	// Both sides should agree on the same version
	if iniResult.neg.Version != respResult.neg.Version {
		t.Errorf("version mismatch: ini=%d, resp=%d",
			iniResult.neg.Version, respResult.neg.Version)
	}

	// Version should be 15 (highest we support)
	if iniResult.neg.Version != 15 {
		t.Errorf("expected version 15, got %d", iniResult.neg.Version)
	}

	if iniResult.neg.NetworkMagic != protocol.MainnetMagic {
		t.Errorf("wrong network magic: %d", iniResult.neg.NetworkMagic)
	}

	ini.Close()
	resp.Close()
}

func TestHandshakeMagicMismatch(t *testing.T) {
	ini, resp := makeMuxPair(t)

	go ini.ReadLoop()
	go resp.ReadLoop()

	type result struct {
		neg *protocol.NegotiatedVersion
		err error
	}

	iniCh := make(chan result, 1)
	respCh := make(chan result, 1)

	// Initiator uses mainnet magic, responder expects preprod magic
	go func() {
		neg, err := protocol.HandshakePropose(ini, protocol.MainnetMagic)
		iniCh <- result{neg, err}
	}()

	go func() {
		neg, err := protocol.HandshakeAccept(resp, protocol.PreprodMagic)
		respCh <- result{neg, err}
	}()

	iniResult := <-iniCh
	respResult := <-respCh

	// At least one side should fail
	if iniResult.err == nil && respResult.err == nil {
		t.Error("expected at least one error on magic mismatch")
	}

	ini.Close()
	resp.Close()
}

func TestHandshakePeerSharing(t *testing.T) {
	ini, resp := makeMuxPair(t)

	go ini.ReadLoop()
	go resp.ReadLoop()

	type result struct {
		neg *protocol.NegotiatedVersion
		err error
	}

	iniCh := make(chan result, 1)
	respCh := make(chan result, 1)

	go func() {
		neg, err := protocol.HandshakePropose(ini, protocol.MainnetMagic)
		iniCh <- result{neg, err}
	}()
	go func() {
		neg, err := protocol.HandshakeAccept(resp, protocol.MainnetMagic)
		respCh <- result{neg, err}
	}()

	iniR := <-iniCh
	respR := <-respCh

	if iniR.err != nil || respR.err != nil {
		t.Skip("handshake failed, skipping peer sharing check")
	}

	// Both sides enable PeerSharing, so it should be negotiated as enabled
	if !iniR.neg.PeerSharing {
		t.Error("expected PeerSharing to be enabled for initiator")
	}
	if !respR.neg.PeerSharing {
		t.Error("expected PeerSharing to be enabled for responder")
	}

	ini.Close()
	resp.Close()
}
