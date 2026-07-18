// Package protocol implements Ouroboros NodeToNode mini-protocols.
package protocol

import (
	"bytes"
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"

	"github.com/cardano-p2p-node/internal/mux"
)

// NodeToNode version numbers used in the handshake.
const (
	NtNV14 = 14
	NtNV15 = 15
)

// NetworkMagic identifies the Cardano network.
const (
	MainnetMagic  uint32 = 764824073
	PreviewMagic  uint32 = 2
	PreprodMagic  uint32 = 1
	SanchoMagic   uint32 = 4
)

// VersionData for NtN v14/v15: [networkMagic, initiatorOnly, peerSharing, query]
type VersionData struct {
	_              struct{} `cbor:",toarray"`
	NetworkMagic   uint32
	InitiatorOnly  bool   // false = InitiatorAndResponder (full node)
	PeerSharing    uint8  // 0=disabled, 1=enabled
	Query          bool   // false normally
}

// NegotiatedVersion is the result of a successful handshake.
type NegotiatedVersion struct {
	Version      int
	NetworkMagic uint32
	PeerSharing  bool
}

// HandshakePropose runs the initiator (client) side of the handshake.
// It proposes versions and waits for acceptance.
func HandshakePropose(mc *mux.Conn, networkMagic uint32) (*NegotiatedVersion, error) {
	vd14 := VersionData{
		NetworkMagic:  networkMagic,
		InitiatorOnly: false,
		PeerSharing:   1,
		Query:         false,
	}

	versionTable := map[uint64]interface{}{
		NtNV14: vd14,
		NtNV15: vd14,
	}

	// msgProposeVersions = [0, versionTable]
	msg, err := cbor.Marshal([]interface{}{uint8(0), versionTable})
	if err != nil {
		return nil, fmt.Errorf("handshake encode propose: %w", err)
	}

	if err := mc.Send(mux.ProtoHandshake, msg); err != nil {
		return nil, fmt.Errorf("handshake send propose: %w", err)
	}

	return receiveHandshakeResponse(mc, networkMagic)
}

// HandshakeAccept runs the responder (server) side of the handshake.
// It receives version proposals and accepts the highest supported version.
func HandshakeAccept(mc *mux.Conn, networkMagic uint32) (*NegotiatedVersion, error) {
	r := mc.Reader(mux.ProtoHandshake)
	raw, err := readOneMessage(r)
	if err != nil {
		return nil, fmt.Errorf("handshake receive propose: %w", err)
	}

	// Decode as generic array
	var msg []cbor.RawMessage
	if err := cbor.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("handshake decode propose: %w", err)
	}
	if len(msg) < 2 {
		return nil, fmt.Errorf("handshake propose too short")
	}

	var tag uint8
	if err := cbor.Unmarshal(msg[0], &tag); err != nil {
		return nil, fmt.Errorf("handshake decode tag: %w", err)
	}
	if tag != 0 {
		return nil, fmt.Errorf("handshake expected propose (tag=0), got %d", tag)
	}

	// Decode version table: map<versionNum → versionData>
	var versionTable map[uint64]cbor.RawMessage
	if err := cbor.Unmarshal(msg[1], &versionTable); err != nil {
		return nil, fmt.Errorf("handshake decode version table: %w", err)
	}

	// Find the highest version we support
	bestVersion := uint64(0)
	for v := range versionTable {
		if (v == NtNV14 || v == NtNV15) && v > bestVersion {
			bestVersion = v
		}
	}

	if bestVersion == 0 {
		// Refuse: version mismatch
		refuseMsg, _ := cbor.Marshal([]interface{}{
			uint8(2),
			[]interface{}{uint8(0), []interface{}{}},
		})
		_ = mc.Send(mux.ProtoHandshake, refuseMsg)
		return nil, fmt.Errorf("handshake: no common version with peer")
	}

	// Decode the version data to check network magic
	var vd VersionData
	if err := cbor.Unmarshal(versionTable[bestVersion], &vd); err != nil {
		return nil, fmt.Errorf("handshake decode version data: %w", err)
	}

	if vd.NetworkMagic != networkMagic {
		refuseMsg, _ := cbor.Marshal([]interface{}{
			uint8(2),
			[]interface{}{uint8(0), []interface{}{}},
		})
		_ = mc.Send(mux.ProtoHandshake, refuseMsg)
		return nil, fmt.Errorf("handshake: network magic mismatch (peer=%d, ours=%d)",
			vd.NetworkMagic, networkMagic)
	}

	// Accept: send MsgAcceptVersion = [1, versionNum, versionData]
	acceptVD := VersionData{
		NetworkMagic:  networkMagic,
		InitiatorOnly: false,
		PeerSharing:   1,
		Query:         false,
	}
	acceptMsg, err := cbor.Marshal([]interface{}{uint8(1), bestVersion, acceptVD})
	if err != nil {
		return nil, fmt.Errorf("handshake encode accept: %w", err)
	}
	if err := mc.Send(mux.ProtoHandshake, acceptMsg); err != nil {
		return nil, fmt.Errorf("handshake send accept: %w", err)
	}

	return &NegotiatedVersion{
		Version:      int(bestVersion),
		NetworkMagic: networkMagic,
		PeerSharing:  vd.PeerSharing == 1,
	}, nil
}

func receiveHandshakeResponse(mc *mux.Conn, networkMagic uint32) (*NegotiatedVersion, error) {
	r := mc.Reader(mux.ProtoHandshake)
	raw, err := readOneMessage(r)
	if err != nil {
		return nil, fmt.Errorf("handshake receive response: %w", err)
	}

	var msg []cbor.RawMessage
	if err := cbor.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("handshake decode response: %w", err)
	}
	if len(msg) < 1 {
		return nil, fmt.Errorf("handshake response empty")
	}

	var tag uint8
	if err := cbor.Unmarshal(msg[0], &tag); err != nil {
		return nil, fmt.Errorf("handshake decode response tag: %w", err)
	}

	switch tag {
	case 1: // MsgAcceptVersion
		if len(msg) < 3 {
			return nil, fmt.Errorf("handshake accept too short")
		}
		var version uint64
		if err := cbor.Unmarshal(msg[1], &version); err != nil {
			return nil, fmt.Errorf("handshake decode accepted version: %w", err)
		}
		var vd VersionData
		if err := cbor.Unmarshal(msg[2], &vd); err != nil {
			return nil, fmt.Errorf("handshake decode accepted version data: %w", err)
		}
		if vd.NetworkMagic != networkMagic {
			return nil, fmt.Errorf("handshake accepted wrong network magic: %d", vd.NetworkMagic)
		}
		return &NegotiatedVersion{
			Version:      int(version),
			NetworkMagic: networkMagic,
			PeerSharing:  vd.PeerSharing == 1,
		}, nil

	case 2: // MsgRefuse
		return nil, fmt.Errorf("handshake refused by peer")

	case 3: // MsgQueryReply - peer just querying, not connecting
		return nil, fmt.Errorf("handshake: peer sent QueryReply (query-only connection)")

	default:
		return nil, fmt.Errorf("handshake: unexpected response tag %d", tag)
	}
}

// readOneMessage reads exactly one complete CBOR message from r.
// It reads byte-by-byte using the CBOR self-delimiting property via a decoder.
func readOneMessage(r io.Reader) ([]byte, error) {
	// We use a cbor decoder which reads exactly one complete item.
	dec := cbor.NewDecoder(r)
	var raw cbor.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// readOneMessageBuf reads one CBOR message from a buffer.
// This is kept for potential future use.
func readOneMessageBuf(data []byte) (msg []byte, err error) {
	dec := cbor.NewDecoder(bytes.NewReader(data))
	var raw cbor.RawMessage
	if decErr := dec.Decode(&raw); decErr != nil {
		return nil, decErr
	}
	return raw, nil
}
