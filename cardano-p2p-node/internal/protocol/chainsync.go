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
//
// Historical headers (before our tip) are requested only to find the intersection
// point. We never store more than the last 256 headers (chain.Store ring size).
// No block bodies are downloaded here — that is the job of BlockFetchClient.
func ChainSyncClient(mc *mux.Conn, store *chain.Store, log *zap.Logger, done <-chan struct{}) error {
	r := mc.Reader(mux.ProtoChainSync)

	// Build intersection candidates from our known recent headers.
	// We propose our current tip; the upstream will find where we diverge.
	// We do NOT include origin: starting from genesis would force the upstream
	// to replay the entire blockchain history (millions of headers).
	// If no intersection is found, the upstream will tell us and we'll
	// drain forward at full speed until we reach the current tip.
	currentTip, _ := store.Tip()
	var candidates []interface{}
	if !currentTip.IsOrigin() {
		candidates = append(candidates, encodePoint(currentTip))
	} else {
		// No known tip yet. Propose origin so the upstream starts from
		// the beginning and we fast-forward to the tip.
		// Headers before our ring fills up are discarded by the store.
		candidates = append(candidates, encodeOrigin())
	}

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

	// Track whether we are in the fast-forward phase (catching up to tip)
	// or in the live phase (at the tip, receiving new blocks as they arrive).
	// During fast-forward, we suppress per-header logging to avoid spam.
	atTip := false

	switch tag {
	case csTagIntersectFound:
		log.Info("chainsync client: intersection found, already synced")
		atTip = true
	case csTagIntersectNotFound:
		// No common point — upstream will send all headers from genesis.
		// We'll fast-forward through them; the ring buffer discards old ones.
		// Headers are small (~500 bytes), so this is fast in practice.
		log.Info("chainsync client: no intersection, fast-forwarding to tip (headers only, no blocks)")
	default:
		return fmt.Errorf("chainsync client unexpected intersect tag: %d", tag)
	}

	fastForwardCount := uint64(0)

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
			// Remote has no more blocks yet; we've reached the tip.
			if !atTip {
				atTip = true
				log.Info("chainsync client: reached tip",
					zap.Uint64("fast_forwarded_headers", fastForwardCount))
			}
			// Wait for the next message (MsgRollForward when a new block arrives)
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
			fallthrough // process the live message below

		case csTagRollForward:
			// MsgRollForward = [2, header, tip]
			if len(respMsg) < 3 {
				continue
			}
			headerRaw := respMsg[1]
			tipRaw := respMsg[2]

			// Decode tip to extract slot/hash for storage
			pt, blockNo := decodePoint(tipRaw)

			h := chain.Header{
				Point:  pt,
				Raw:    headerRaw,
				TipRaw: tipRaw,
				No:     blockNo,
			}

			// AddHeader wakes all downstream ChainSync servers immediately.
			// This is the critical latency path for upstreamyness scoring.
			store.AddHeader(h)

			if atTip {
				log.Debug("chainsync client: new live header",
					zap.Uint64("slot", pt.SlotNo),
					zap.Uint64("blockNo", blockNo))
			} else {
				fastForwardCount++
				// Log progress every 10 000 headers during fast-forward
				if fastForwardCount%10000 == 0 {
					log.Info("chainsync client: fast-forwarding",
						zap.Uint64("headers_received", fastForwardCount),
						zap.Uint64("current_slot", pt.SlotNo))
				}
			}

		case csTagRollBackward:
			// MsgRollBackward = [3, point, tip]
			// A rollback happens during chain reorganisations.
			// We clear our store back to the rollback point.
			if len(respMsg) >= 3 {
				pt, _ := decodePoint(respMsg[1])
				log.Info("chainsync client: rollback", zap.Uint64("to_slot", pt.SlotNo))
				// The store's ring buffer will naturally handle this:
				// new headers from the fork will overwrite old ones.
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
//
// Concurrency design:
//   - A dedicated "reader" goroutine pulls MsgRequestNext from the peer
//     and sends tokens into a channel. This decouples reading from sending
//     so we never block waiting for the client when a new header is ready.
//   - The main goroutine consumes request tokens and sends headers.
//   - When a new header arrives from upstream we wake immediately via the
//     store subscription channel and push it without waiting for the reader.
//
// This design is what makes us fast enough to win the upstreamyness race:
// the moment a new header lands in the store, all downstream ChainSync
// servers are already unblocked and push the header concurrently.
func chainSyncServe(
	mc *mux.Conn,
	r io.Reader,
	store *chain.Store,
	bufferedHeaders []chain.Header,
	log *zap.Logger,
	done <-chan struct{},
) error {
	// Subscribe before copying buffered headers to avoid missing any
	// headers that arrive while we are serving the backlog.
	newHeaderCh := store.Subscribe()
	defer store.Unsubscribe(newHeaderCh)

	// requestCh receives a token for every MsgRequestNext we get.
	// Buffered so the reader goroutine is never blocked by a slow sender.
	requestCh := make(chan struct{}, 64)
	readErrCh := make(chan error, 1)

	// Reader goroutine: just reads requests, never sends anything.
	go func() {
		for {
			raw, err := readOneMessage(r)
			if err != nil {
				readErrCh <- err
				return
			}
			var req []cbor.RawMessage
			if err := cbor.Unmarshal(raw, &req); err != nil || len(req) == 0 {
				continue
			}
			var tag uint8
			_ = cbor.Unmarshal(req[0], &tag)
			switch tag {
			case csTagRequestNext:
				select {
				case requestCh <- struct{}{}:
				case <-done:
					return
				}
			case csTagDone:
				readErrCh <- io.EOF
				return
			}
		}
	}()

	// headerQ holds headers we have queued to send but haven't yet received
	// a MsgRequestNext for. We track by position in the store's ring.
	headerQ := make([]chain.Header, len(bufferedHeaders))
	copy(headerQ, bufferedHeaders)

	// lastSentSlot is used to deduplicate: the store subscription may fire
	// multiple times before we drain, but we track position via allHeaders index.
	allHeaders := store.AllHeaders()
	sentUpTo := len(allHeaders) - len(bufferedHeaders) // index into allHeaders

	// pendingRequests counts how many MsgRequestNext we've received but not
	// yet answered. When > 0 we can send immediately without waiting.
	pendingRequests := 0

	for {
		// Drain any pending requests from the channel (non-blocking)
	drainRequests:
		for {
			select {
			case <-requestCh:
				pendingRequests++
			default:
				break drainRequests
			}
		}

		if pendingRequests > 0 && len(headerQ) > 0 {
			// Serve as many headers as we have requests for, all at once.
			toSend := pendingRequests
			if toSend > len(headerQ) {
				toSend = len(headerQ)
			}
			for i := 0; i < toSend; i++ {
				if err := sendRollForward(mc, headerQ[i]); err != nil {
					return err
				}
				log.Debug("chainsync server: served header", zap.Uint64("slot", headerQ[i].Point.SlotNo))
			}
			headerQ = headerQ[toSend:]
			pendingRequests -= toSend
			continue
		}

		if pendingRequests > 0 && len(headerQ) == 0 {
			// At tip: send MsgAwaitReply once, then wait for new headers.
			awaitMsg, _ := cbor.Marshal([]interface{}{uint8(csTagAwaitReply)})
			if err := mc.Send(mux.ProtoChainSync, awaitMsg); err != nil {
				return fmt.Errorf("chainsync server send await: %w", err)
			}
			// Wait for store notification or more requests (reader goroutine continues)
		}

		// Block until something interesting happens:
		// - a new header arrives in the store, OR
		// - the client sends another MsgRequestNext, OR
		// - shutdown/error
		select {
		case <-done:
			return nil
		case err := <-readErrCh:
			if err == io.EOF {
				return nil
			}
			return err
		case <-requestCh:
			pendingRequests++
		case <-newHeaderCh:
			// Fetch newly added headers since we last checked.
			// This is O(n) over new headers only, not the whole ring.
			current := store.AllHeaders()
			if len(current) > sentUpTo {
				newOnes := current[sentUpTo:]
				headerQ = append(headerQ, newOnes...)
				sentUpTo = len(current)
			}
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
