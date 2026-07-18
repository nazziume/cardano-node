package cardano

import (
	"golang.org/x/crypto/blake2b"

	"github.com/fxamacker/cbor/v2"
)

// ParseBlockTxIDs extracts the Blake2b-256 transaction IDs from a Cardano
// block received via the BlockFetch mini-protocol.
//
// Wire format (Babbage/Conway, NodeToNode):
//
//	The block bytes from MsgBlock may be:
//	  a) A CBOR byte-string wrapping the actual block bytes (Serialised newtype)
//	  b) The block CBOR directly
//
//	Once unwrapped, the era-tagged block is:
//	  [era_id, era_block_bytes]   -- era_block_bytes may again be a bstr
//
//	The era-specific block for Babbage (5) / Conway (6) is:
//	  [header, tx_bodies, tx_witnesses, auxiliary_data, invalid_tx_indices]
//
//	tx_bodies is a CBOR array of raw transaction_body maps.
//	The txid = Blake2b-256(raw_tx_body_cbor)
//
// Returns nil when the block cannot be recognised (unknown era, corrupt CBOR).
func ParseBlockTxIDs(raw []byte) [][]byte {
	// Layer 1: unwrap Serialised byte-string if present
	var inner []byte
	if err := cbor.Unmarshal(raw, &inner); err == nil && len(inner) > 2 {
		if ids := parseEraTagged(inner); ids != nil {
			return ids
		}
	}
	// Try treating raw directly as the era-tagged block
	return parseEraTagged(raw)
}

// parseEraTagged handles [era_id, block_bytes_or_cbor].
func parseEraTagged(raw []byte) [][]byte {
	var outerArr []cbor.RawMessage
	if err := cbor.Unmarshal(raw, &outerArr); err != nil || len(outerArr) < 2 {
		return nil
	}
	// outerArr[0] = era id (uint)
	// outerArr[1] = era-specific block (bstr or direct CBOR)
	var blockBytes []byte
	if err := cbor.Unmarshal(outerArr[1], &blockBytes); err == nil && len(blockBytes) > 0 {
		return parseEraBlock(blockBytes)
	}
	return parseEraBlock(outerArr[1])
}

// parseEraBlock handles the Babbage/Conway era block:
//
//	[header, tx_bodies, tx_witnesses, auxiliary_data, invalid_tx_indices]
func parseEraBlock(raw []byte) [][]byte {
	var blockArr []cbor.RawMessage
	if err := cbor.Unmarshal(raw, &blockArr); err != nil || len(blockArr) < 2 {
		return nil
	}

	// index 1 = tx_bodies = [* transaction_body]
	var txBodies []cbor.RawMessage
	if err := cbor.Unmarshal(blockArr[1], &txBodies); err != nil || len(txBodies) == 0 {
		return nil
	}

	ids := make([][]byte, 0, len(txBodies))
	for _, bodyRaw := range txBodies {
		// txid = Blake2b-256(CBOR bytes of the transaction_body map)
		h, err := blake2b.New256(nil)
		if err != nil {
			continue
		}
		h.Write(bodyRaw)
		ids = append(ids, h.Sum(nil))
	}
	return ids
}
