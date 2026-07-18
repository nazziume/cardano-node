package protocol

import (
	"fmt"
	"io"
	"time"

	"github.com/fxamacker/cbor/v2"
	"go.uber.org/zap"

	"github.com/cardano-p2p-node/internal/mux"
)

// KeepAliveServer responds to keep-alive pings from the remote initiator.
// Call this when we accepted the connection (we are the mux responder).
func KeepAliveServer(mc *mux.Conn, log *zap.Logger) error {
	r := mc.Reader(mux.ProtoKeepAlive)
	for {
		raw, err := readOneMessage(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("keepalive server read: %w", err)
		}

		var msg []cbor.RawMessage
		if err := cbor.Unmarshal(raw, &msg); err != nil {
			return fmt.Errorf("keepalive server decode: %w", err)
		}
		if len(msg) < 1 {
			continue
		}

		var tag uint8
		_ = cbor.Unmarshal(msg[0], &tag)

		switch tag {
		case 0: // MsgKeepAlive [0, cookie]
			if len(msg) < 2 {
				continue
			}
			// Respond immediately with MsgKeepAliveResponse [1, cookie]
			resp, _ := cbor.Marshal([]interface{}{uint8(1), msg[1]})
			if err := mc.Send(mux.ProtoKeepAlive, resp); err != nil {
				return fmt.Errorf("keepalive server send response: %w", err)
			}
		case 2: // MsgDone
			return nil
		}
	}
}

// KeepAliveClient sends periodic keep-alive pings and measures RTT.
// Call this when we initiated the connection (we are the mux initiator).
func KeepAliveClient(mc *mux.Conn, log *zap.Logger, done <-chan struct{}) error {
	r := mc.Reader(mux.ProtoKeepAlive)
	cookie := uint16(0)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Send first ping immediately
	if err := sendKeepAlivePing(mc, cookie); err != nil {
		return err
	}

	for {
		select {
		case <-done:
			// Send MsgDone = [2]
			doneMsg, _ := cbor.Marshal([]interface{}{uint8(2)})
			_ = mc.Send(mux.ProtoKeepAlive, doneMsg)
			return nil
		case <-ticker.C:
			cookie++
			start := time.Now()
			if err := sendKeepAlivePing(mc, cookie); err != nil {
				return fmt.Errorf("keepalive client ping: %w", err)
			}
			// Read the response (non-blocking with goroutine would be cleaner,
			// but for simplicity we read inline here)
			raw, err := readOneMessage(r)
			if err != nil {
				return fmt.Errorf("keepalive client read response: %w", err)
			}
			var msg []cbor.RawMessage
			if err := cbor.Unmarshal(raw, &msg); err != nil {
				continue
			}
			if len(msg) < 1 {
				continue
			}
			var tag uint8
			_ = cbor.Unmarshal(msg[0], &tag)
			if tag == 1 { // MsgKeepAliveResponse
				rtt := time.Since(start)
				log.Debug("keepalive RTT", zap.Duration("rtt", rtt))
			}
		}
	}
}

func sendKeepAlivePing(mc *mux.Conn, cookie uint16) error {
	// MsgKeepAlive = [0, cookie]
	msg, err := cbor.Marshal([]interface{}{uint8(0), cookie})
	if err != nil {
		return err
	}
	return mc.Send(mux.ProtoKeepAlive, msg)
}
