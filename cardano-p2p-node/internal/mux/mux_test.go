package mux_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cardano-p2p-node/internal/mux"
)

// makePipe creates a connected pair of mux.Conn (initiator + responder).
func makePipe(t *testing.T) (initiator, responder *mux.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var wg sync.WaitGroup
	var serverConn net.Conn

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		serverConn, err = ln.Accept()
		if err != nil {
			t.Error(err)
		}
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	initiator = mux.New(clientConn, true)
	responder = mux.New(serverConn, false)
	return
}

func TestMuxSendReceive(t *testing.T) {
	ini, resp := makePipe(t)

	// Start read loops
	go ini.ReadLoop()
	go resp.ReadLoop()

	const protoID = mux.ProtoHandshake
	testData := []byte("hello from initiator")

	// Initiator sends, responder receives
	done := make(chan struct{})
	go func() {
		defer close(done)
		r := resp.Reader(protoID)
		buf := make([]byte, len(testData))
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Errorf("read: %v", err)
			return
		}
		if !bytes.Equal(buf, testData) {
			t.Errorf("got %q, want %q", buf, testData)
		}
	}()

	if err := ini.Send(protoID, testData); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for data")
	}

	ini.Close()
	resp.Close()
}

func TestMuxMultiProtocol(t *testing.T) {
	ini, resp := makePipe(t)

	go ini.ReadLoop()
	go resp.ReadLoop()

	// Send on multiple protocols simultaneously
	messages := map[uint16][]byte{
		mux.ProtoChainSync:  []byte("chainsync data"),
		mux.ProtoKeepAlive: []byte("keepalive data"),
		mux.ProtoBlockFetch: []byte("blockfetch data"),
	}

	var wg sync.WaitGroup
	for protoID, data := range messages {
		protoID, data := protoID, data
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := resp.Reader(protoID)
			buf := make([]byte, len(data))
			if _, err := io.ReadFull(r, buf); err != nil {
				t.Errorf("proto %d read: %v", protoID, err)
				return
			}
			if !bytes.Equal(buf, data) {
				t.Errorf("proto %d: got %q, want %q", protoID, buf, data)
			}
		}()
	}

	for protoID, data := range messages {
		if err := ini.Send(protoID, data); err != nil {
			t.Fatalf("send proto %d: %v", protoID, err)
		}
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for multi-protocol data")
	}

	ini.Close()
	resp.Close()
}

func TestMuxLargePayload(t *testing.T) {
	ini, resp := makePipe(t)

	go ini.ReadLoop()
	go resp.ReadLoop()

	// Send data larger than MaxSDUSize to test segmentation
	large := make([]byte, mux.MaxSDUSize*3+100)
	for i := range large {
		large[i] = byte(i & 0xFF)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		r := resp.Reader(mux.ProtoBlockFetch)
		buf := make([]byte, len(large))
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Errorf("large read: %v", err)
			return
		}
		if !bytes.Equal(buf, large) {
			t.Error("large data mismatch")
		}
	}()

	if err := ini.Send(mux.ProtoBlockFetch, large); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for large data")
	}

	ini.Close()
	resp.Close()
}

func TestMuxHeaderFormat(t *testing.T) {
	// Verify the mux header is encoded correctly:
	// [0:4] = timestamp (uint32 big-endian)
	// [4:6] = mode+protoID (uint16 big-endian)
	// [6:8] = payload length (uint16 big-endian)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	received := make(chan []byte, 1)
	go func() {
		conn, _ := ln.Accept()
		defer conn.Close()
		hdr := make([]byte, 8)
		io.ReadFull(conn, hdr)
		received <- hdr
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	mc := mux.New(conn, true) // initiator
	payload := []byte("test")
	mc.Send(mux.ProtoChainSync, payload)
	conn.Close()

	hdr := <-received
	protoAndMode := binary.BigEndian.Uint16(hdr[4:6])
	length := binary.BigEndian.Uint16(hdr[6:8])

	// Initiator sends with mode bit = 0
	if protoAndMode&0x8000 != 0 {
		t.Errorf("initiator should send with mode bit = 0, got 0x%04x", protoAndMode)
	}
	gotProtoID := protoAndMode & 0x7FFF
	if gotProtoID != mux.ProtoChainSync {
		t.Errorf("protocol ID: got %d, want %d", gotProtoID, mux.ProtoChainSync)
	}
	if length != uint16(len(payload)) {
		t.Errorf("payload length: got %d, want %d", length, len(payload))
	}
}

func TestMuxResponderMode(t *testing.T) {
	// Verify that responder sends with mode bit = 1
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	received := make(chan []byte, 1)
	go func() {
		conn, _ := ln.Accept()
		defer conn.Close()
		hdr := make([]byte, 8)
		io.ReadFull(conn, hdr)
		received <- hdr
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	mc := mux.New(conn, false) // responder
	payload := []byte("response")
	mc.Send(mux.ProtoKeepAlive, payload)
	conn.Close()

	hdr := <-received
	protoAndMode := binary.BigEndian.Uint16(hdr[4:6])

	// Responder sends with mode bit = 1
	if protoAndMode&0x8000 == 0 {
		t.Errorf("responder should send with mode bit = 1, got 0x%04x", protoAndMode)
	}
	gotProtoID := protoAndMode & 0x7FFF
	if gotProtoID != mux.ProtoKeepAlive {
		t.Errorf("protocol ID: got %d, want %d", gotProtoID, mux.ProtoKeepAlive)
	}
}
