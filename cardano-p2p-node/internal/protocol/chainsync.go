package protocol

import (
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"
	"go.uber.org/zap"

	"github.com/cardano-p2p-node/internal/chain"
	"github.com/cardano-p2p-node/internal/mux"
)

// ChainSync message tags
const (
	csTagRequestNext       = 0
	csTagAwaitReply        = 1
	csTagRollForward       = 2
	csTagRollBackward      = 3
	csTagFindIntersect     = 4
	csTagIntersectFound    = 5
	csTagIntersectNotFound = 6
	csTagDone              = 7
)

// ChainSyncClient runs the chain-sync CLIENT on an outbound connection.
// It downloads headers from the remote and stores them in the chain store.
// When a new header arrives, it immediately becomes available to downstream peers.
func ChainSyncClient(mc *mux.Conn, store *chain.Store, log *zap.Logger, done <-chan struct{}) error {
	r := mc.Reader(mux.ProtoChainSync)

	// Start by finding the intersection point
	// Use our current tip as the candidate
	currentTip, _ := store.Tip()
	var candidates []interface{}
	if !currentTip.IsOrigin() {
		candidates = append(candidates, encodePoint(currentTip))
	}
	// Also include origin as fallback
	candidates = append(candidates, encodeOrigin())

	// Send MsgFindIntersect = [4, [points...]]
	findMsg, _ := cbor.Marshal([]interface{}{uint8(csTagFindIntersect), candidates})
	if err := mc.Send(mux.ProtoChainSync, findMsg); err != nil {
		return fmt.Errorf("chainsync client find intersect: %w", err)
	}

	// Receive intersection response
	raw, err := readOneMessage(r)
	if err != nil {
		return fmt.Errorf("chainsync client intersect response: %w", err)
	}

	var msg []cbor.RawMessage
	if err := cbor.Unmarshal(raw, &msg); err != nil {
		return fmt.Errorf("chainsync client decode intersect: %w", err)
	}
	if len(msg) < 1 {
		return fmt.Errorf("chainsync client empty intersect response")
	}

	var tag uint8
	_ = cbor.Unmarshal(msg[0], &tag)
	switch tag {
	case csTagIntersectFound:
		log.Info("chainsync client: intersection found")
	case csTagIntersectNotFound:
		log.Info("chainsync client: no intersection found, syncing from genesis")
	default:
		return fmt.Errorf("chainsync client unexpected intersect tag: %d", tag)
	}

	// Main sync loop: request headers continuously
	for {
		select {
		case <-done:
			return nil
		default:
		}

		// Send MsgRequestNext = [0]
		reqMsg, _ := cbor.Marshal([]interface{}{uint8(csTagRequestNext)})
		if err := mc.Send(mux.ProtoChainSync, reqMsg); err != nil {
			return fmt.Errorf("chainsync client request next: %w", err)
		}

		// Wait for response (may need to wait for MsgAwaitReply then next message)
		raw, err := readOneMessage(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("chainsync client read response: %w", err)
		}

		var respMsg []cbor.RawMessage
		if err := cbor.Unmarshal(raw, &respMsg); err != nil {
			return fmt.Errorf("chainsync client decode response: %w", err)
		}
		if len(respMsg) < 1 {
			continue
		}

		var respTag uint8
		_ = cbor.Unmarshal(respMsg[0], &respTag)

		switch respTag {
		case csTagAwaitReply:
			// Remote has no more blocks yet; wait for the next message
			raw, err = readOneMessage(r)
			if err != nil {
				return fmt.Errorf("chainsync client await reply: %w", err)
			}
			if err := cbor.Unmarshal(raw, &respMsg); err != nil {
				continue
			}
			if len(respMsg) < 1 {
				continue
			}
			_ = cbor.Unmarshal(respMsg[0], &respTag)
			fallthrough // process the next message below

		case csTagRollForward:
			// MsgRollForward = [2, header, tip]
			if len(respMsg) < 3 {
				continue
			}
			headerRaw := respMsg[1]
			tipRaw := respMsg[2]

			// Decode the tip to get slot/hash for our store
			pt, blockNo := decodePoint(tipRaw)

			h := chain.Header{
				Point:  pt,
				Raw:    headerRaw,
				TipRaw: tipRaw,
				No:     blockNo,
			}
			// AddHeader notifies all downstream ChainSync servers immediately
			store.AddHeader(h)
			log.Debug("chainsync client: new header",
				zap.Uint64("slot", pt.SlotNo),
				zap.Uint64("blockNo", blockNo))

		case csTagRollBackward:
			// MsgRollBackward = [3, point, tip]
			// We don't fully handle rollbacks; just update our view
			if len(respMsg) >= 3 {
				log.Info("chainsync client: rollback received")
			}

		case csTagDone:
			return nil
		}
	}
}

// ChainSyncServer runs the chain-sync SERVER on an inbound connection.
// It serves headers to the remote client from our chain store.
// This is the critical path for upstreamyness scoring:
// when a new header arrives we send it to the downstream peer immediately.
func ChainSyncServer(mc *mux.Conn, store *chain.Store, log *zap.Logger, done <-chan struct{}) error {
	r := mc.Reader(mux.ProtoChainSync)

	// Wait for MsgFindIntersect from client
	raw, err := readOneMessage(r)
	if err != nil {
		return fmt.Errorf("chainsync server read find intersect: %w", err)
	}

	var msg []cbor.RawMessage
	if err := cbor.Unmarshal(raw, &msg); err != nil {
		return fmt.Errorf("chainsync server decode find intersect: %w", err)
	}
	if len(msg) < 2 {
		return fmt.Errorf("chainsync server find intersect too short")
	}

	var tag uint8
	_ = cbor.Unmarshal(msg[0], &tag)
	if tag != csTagFindIntersect {
		return fmt.Errorf("chainsync server expected FindIntersect, got tag=%d", tag)
	}

	// Decode candidate points
	var rawPoints []cbor.RawMessage
	if err := cbor.Unmarshal(msg[1], &rawPoints); err != nil {
		return fmt.Errorf("chainsync server decode points: %w", err)
	}

	candidates := make([]chain.Point, 0, len(rawPoints))
	for _, rp := range rawPoints {
		pt, _ := decodePoint(rp)
		candidates = append(candidates, pt)
	}

	// Find intersection in our store
	currentTip, tipBlockNo := store.Tip()
	tipEncoded := encodeTip(currentTip, tipBlockNo)

	foundPoint, headersAfter := store.FindIntersect(candidates)
	if foundPoint != nil {
		// MsgIntersectFound = [5, point, tip]
		resp, _ := cbor.Marshal([]interface{}{
			uint8(csTagIntersectFound),
			encodePoint(*foundPoint),
			tipEncoded,
		})
		if err := mc.Send(mux.ProtoChainSync, resp); err != nil {
			return fmt.Errorf("chainsync server send intersect found: %w", err)
		}
		// Serve buffered headers first, then live ones
		return chainSyncServe(mc, r, store, headersAfter, log, done)
	}

	// MsgIntersectNotFound = [6, tip]
	resp, _ := cbor.Marshal([]interface{}{uint8(csTagIntersectNotFound), tipEncoded})
	if err := mc.Send(mux.ProtoChainSync, resp); err != nil {
		return fmt.Errorf("chainsync server send intersect not found: %w", err)
	}
	// Serve all headers we have, then live ones
	allHeaders := store.AllHeaders()
	return chainSyncServe(mc, r, store, allHeaders, log, done)
}

// chainSyncServe is the inner serving loop after intersection negotiation.
// It sends bufferedHeaders first, then waits for new headers from the store.
func chainSyncServe(
	mc *mux.Conn,
	r io.Reader,
	store *chain.Store,
	bufferedHeaders []chain.Header,
	log *zap.Logger,
	done <-chan struct{},
) error {
	// Subscribe to new headers BEFORE we start serving buffered ones
	// to avoid missing any that arrive between buffer snapshot and subscription
	newHeaderCh := store.Subscribe()
	defer store.Unsubscribe(newHeaderCh)

	headerQueue := make([]chain.Header, len(bufferedHeaders))
	copy(headerQueue, bufferedHeaders)

	for {
		select {
		case <-done:
			return nil
		default:
		}

		// Wait for MsgRequestNext from the client
		raw, err := readOneMessage(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("chainsync server read request: %w", err)
		}

		var reqMsg []cbor.RawMessage
		if err := cbor.Unmarshal(raw, &reqMsg); err != nil {
			continue
		}
		if len(reqMsg) < 1 {
			continue
		}
		var reqTag uint8
		_ = cbor.Unmarshal(reqMsg[0], &reqTag)

		switch reqTag {
		case csTagRequestNext:
			if len(headerQueue) > 0 {
				// Serve next buffered header immediately
				h := headerQueue[0]
				headerQueue = headerQueue[1:]
				if err := sendRollForward(mc, h); err != nil {
					return err
				}
				log.Debug("chainsync server: served buffered header", zap.Uint64("slot", h.Point.SlotNo))
			} else {
				// At tip: send MsgAwaitReply, then wait for new header
				awaitMsg, _ := cbor.Marshal([]interface{}{uint8(csTagAwaitReply)})
				if err := mc.Send(mux.ProtoChainSync, awaitMsg); err != nil {
					return fmt.Errorf("chainsync server send await: %w", err)
				}

				// Wait for a new header to arrive in the store
				// This is the CRITICAL path: we wake up immediately when new
				// headers arrive and forward them to the downstream peer.
				// Being first here is what maximizes upstreamyness score.
				for {
					select {
					case <-done:
						return nil
					case <-newHeaderCh:
						// Fetch any new headers we haven't served yet
						newHeaders := store.AllHeaders()
						// Find the last header we sent (if any)
						// For now, just send the most recent one
						if len(newHeaders) > 0 {
							h := newHeaders[len(newHeaders)-1]
							if err := sendRollForward(mc, h); err != nil {
								return err
							}
							log.Debug("chainsync server: sent live header", zap.Uint64("slot", h.Point.SlotNo))
							goto nextRequest
						}
					}
				}
			nextRequest:
			}

		case csTagDone:
			return nil
		}
	}
}

func sendRollForward(mc *mux.Conn, h chain.Header) error {
	// MsgRollForward = [2, header, tip]
	msg, err := cbor.Marshal([]interface{}{
		uint8(csTagRollForward),
		h.Raw,
		h.TipRaw,
	})
	if err != nil {
		return fmt.Errorf("chainsync server encode roll forward: %w", err)
	}
	return mc.Send(mux.ProtoChainSync, msg)
}

// encodePoint encodes a chain point as CBOR for use in protocol messages.
// Cardano encodes points as [slotNo, hash] (a 2-element array) or [] for origin.
func encodePoint(p chain.Point) interface{} {
	if p.IsOrigin() {
		return []interface{}{}
	}
	return []interface{}{p.SlotNo, p.Hash}
}

func encodeOrigin() interface{} {
	return []interface{}{}
}

// encodeTip encodes a tip as [point, blockNo].
func encodeTip(p chain.Point, blockNo uint64) interface{} {
	return []interface{}{encodePoint(p), blockNo}
}

// decodePoint extracts slot and hash from a raw CBOR point.
// Returns origin point if decoding fails.
func decodePoint(raw cbor.RawMessage) (chain.Point, uint64) {
	// Try to decode as [point, blockNo] (tip format)
	var tipArr []cbor.RawMessage
	if err := cbor.Unmarshal(raw, &tipArr); err == nil && len(tipArr) == 2 {
		var pt chain.Point
		var bn uint64
		// inner is [slotNo, hash] or []
		var ptArr []cbor.RawMessage
		if err := cbor.Unmarshal(tipArr[0], &ptArr); err == nil {
			if len(ptArr) == 2 {
				_ = cbor.Unmarshal(ptArr[0], &pt.SlotNo)
				_ = cbor.Unmarshal(ptArr[1], &pt.Hash)
			}
		}
		_ = cbor.Unmarshal(tipArr[1], &bn)
		return pt, bn
	}

	// Try to decode as [slotNo, hash] directly (point format)
	var ptArr []cbor.RawMessage
	if err := cbor.Unmarshal(raw, &ptArr); err == nil {
		if len(ptArr) == 0 {
			return chain.Point{}, 0 // origin
		}
		if len(ptArr) == 2 {
			var pt chain.Point
			_ = cbor.Unmarshal(ptArr[0], &pt.SlotNo)
			_ = cbor.Unmarshal(ptArr[1], &pt.Hash)
			return pt, 0
		}
	}

	return chain.Point{}, 0
}
