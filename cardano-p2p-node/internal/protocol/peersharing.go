package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/fxamacker/cbor/v2"
	"go.uber.org/zap"

	"github.com/cardano-p2p-node/internal/mux"
)

// PeerAddress is a shareable peer address.
type PeerAddress struct {
	IP   net.IP
	Port uint16
}

// PeerSharingServer responds to peer sharing requests.
// We share our known peers with the requester.
func PeerSharingServer(mc *mux.Conn, getPeers func() []PeerAddress, log *zap.Logger, done <-chan struct{}) error {
	r := mc.Reader(mux.ProtoPeerSharing)

	for {
		select {
		case <-done:
			return nil
		default:
		}

		raw, err := readOneMessage(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("peer sharing server read: %w", err)
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

		switch tag {
		case 0: // MsgShareRequest [0, amount]
			var amount uint8
			if len(msg) >= 2 {
				_ = cbor.Unmarshal(msg[1], &amount)
			}
			if amount == 0 {
				amount = 10
			}

			peers := getPeers()
			if int(amount) < len(peers) {
				peers = peers[:amount]
			}

			// Encode peer addresses
			peerList := make([]interface{}, 0, len(peers))
			for _, p := range peers {
				peerList = append(peerList, encodePeerAddr(p))
			}

			// MsgSharePeers = [1, peerAddresses]
			resp, _ := cbor.Marshal([]interface{}{uint8(1), peerList})
			if err := mc.Send(mux.ProtoPeerSharing, resp); err != nil {
				return fmt.Errorf("peer sharing server send: %w", err)
			}
			log.Debug("peer sharing server: shared peers", zap.Int("count", len(peers)))

		case 2: // MsgDone
			return nil
		}
	}
}

// PeerSharingClient requests peers from a remote node.
func PeerSharingClient(mc *mux.Conn, amount uint8, log *zap.Logger) ([]PeerAddress, error) {
	r := mc.Reader(mux.ProtoPeerSharing)

	// MsgShareRequest = [0, amount]
	req, _ := cbor.Marshal([]interface{}{uint8(0), amount})
	if err := mc.Send(mux.ProtoPeerSharing, req); err != nil {
		return nil, fmt.Errorf("peer sharing client request: %w", err)
	}

	raw, err := readOneMessage(r)
	if err != nil {
		return nil, fmt.Errorf("peer sharing client read: %w", err)
	}

	var msg []cbor.RawMessage
	if err := cbor.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("peer sharing client decode: %w", err)
	}
	if len(msg) < 2 {
		return nil, nil
	}

	var tag uint8
	_ = cbor.Unmarshal(msg[0], &tag)
	if tag != 1 { // MsgSharePeers
		return nil, nil
	}

	var peerList []cbor.RawMessage
	if err := cbor.Unmarshal(msg[1], &peerList); err != nil {
		return nil, nil
	}

	peers := make([]PeerAddress, 0, len(peerList))
	for _, rawPeer := range peerList {
		if p, ok := decodePeerAddr(rawPeer); ok {
			peers = append(peers, p)
		}
	}

	// Send MsgDone = [2]
	doneMsg, _ := cbor.Marshal([]interface{}{uint8(2)})
	_ = mc.Send(mux.ProtoPeerSharing, doneMsg)

	log.Debug("peer sharing client: received peers", zap.Int("count", len(peers)))
	return peers, nil
}

func encodePeerAddr(p PeerAddress) interface{} {
	if p.IP.To4() != nil {
		// IPv4: [0, uint32, port]
		ip4 := p.IP.To4()
		ipUint := binary.BigEndian.Uint32(ip4)
		return []interface{}{uint8(0), ipUint, p.Port}
	}
	// IPv6: [1, w0, w1, w2, w3, port]
	ip6 := p.IP.To16()
	w0 := binary.BigEndian.Uint32(ip6[0:4])
	w1 := binary.BigEndian.Uint32(ip6[4:8])
	w2 := binary.BigEndian.Uint32(ip6[8:12])
	w3 := binary.BigEndian.Uint32(ip6[12:16])
	return []interface{}{uint8(1), w0, w1, w2, w3, p.Port}
}

func decodePeerAddr(raw cbor.RawMessage) (PeerAddress, bool) {
	var parts []cbor.RawMessage
	if err := cbor.Unmarshal(raw, &parts); err != nil || len(parts) < 3 {
		return PeerAddress{}, false
	}

	var addrType uint8
	_ = cbor.Unmarshal(parts[0], &addrType)

	switch addrType {
	case 0: // IPv4
		if len(parts) < 3 {
			return PeerAddress{}, false
		}
		var ipUint uint32
		var port uint16
		_ = cbor.Unmarshal(parts[1], &ipUint)
		_ = cbor.Unmarshal(parts[2], &port)
		ip := make(net.IP, 4)
		binary.BigEndian.PutUint32(ip, ipUint)
		return PeerAddress{IP: ip, Port: port}, true

	case 1: // IPv6
		if len(parts) < 6 {
			return PeerAddress{}, false
		}
		var w0, w1, w2, w3 uint32
		var port uint16
		_ = cbor.Unmarshal(parts[1], &w0)
		_ = cbor.Unmarshal(parts[2], &w1)
		_ = cbor.Unmarshal(parts[3], &w2)
		_ = cbor.Unmarshal(parts[4], &w3)
		_ = cbor.Unmarshal(parts[5], &port)
		ip := make(net.IP, 16)
		binary.BigEndian.PutUint32(ip[0:4], w0)
		binary.BigEndian.PutUint32(ip[4:8], w1)
		binary.BigEndian.PutUint32(ip[8:12], w2)
		binary.BigEndian.PutUint32(ip[12:16], w3)
		return PeerAddress{IP: ip, Port: port}, true
	}

	return PeerAddress{}, false
}
