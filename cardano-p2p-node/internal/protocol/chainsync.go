package protocol

import (
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"
	"go.uber.org/zap"

	"github.com/cardano-p2p-node/internal/chain"
	"github.com/cardano-p2p-node/internal/mempool"
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
//
// pool is optional: if non-nil, TTL-expired transactions are pruned from the
// mempool whenever the chain tip advances to a new slot.
func ChainSyncClient(mc *mux.Conn, store *chain.Store, pool *mempool.Mempool, log *zap.Logger, done <-chan struct{}) error {
	r := mc.Reader(mux.ProtoChainSync)

	// Build intersection candidates using exponential-backoff sampling of
	// our known recent headers. Sending only the tip is fragile: if the tip
	// block arrived after the peer's latest view (e.g. a 1-block micro-fork),
	// the intersection fails and the peer rolls us ALL THE WAY back to origin,
	// forcing a full re-sync from genesis.
	//
	// Sending candidates at depths 0, 1, 2, 4, 8, 16, 32, 64, 128 gives the
	// peer many chances to find a common ancestor within the last 256 blocks.
	// Cardano clients may send up to 96 candidates; we use at most ~10.
	headers := store.AllHeaders()
	var candidates []interface{}
	if len(headers) > 0 {
		seen := make(map[int]bool)
		depth := 0
		for depth < len(headers) {
			idx := len(headers) - 1 - depth
			if !seen[idx] {
				seen[idx] = true
				candidates = append(candidates, encodePoint(headers[idx].Point))
			}
			if depth == 0 {
				depth = 1
			} else {
				depth *= 2
			}
		}
		// Always include the oldest available header as a final fallback.
		if !seen[0] {
			candidates = append(candidates, encodePoint(headers[0].Point))
		}
	} else {
		// No known headers yet: propose origin so the upstream starts from
		// genesis and we fast-forward. Headers are small (~500 B each) so
		// this is fast even for a chain with millions of blocks.
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

			// Prune TTL-expired transactions now that the slot has advanced.
			if pool != nil && atTip {
				if pruned := pool.PruneTTL(pt.SlotNo); pruned > 0 {
					log.Info("chainsync client: pruned TTL-expired txs",
						zap.Int("pruned", pruned),
						zap.Uint64("slot", pt.SlotNo))
				}
			}

			if atTip {
				log.Debug("chainsync client: new live header",
					zap.Uint64("slot", pt.SlotNo),
					zap.Uint64("blockNo", blockNo))
			} else {
				fastForwardCount++
				if fastForwardCount%10000 == 0 {
					log.Info("chainsync client: fast-forwarding",
						zap.Uint64("headers_received", fastForwardCount),
						zap.Uint64("current_slot", pt.SlotNo))
				}
			}

		case csTagRollBackward:
			// MsgRollBackward = [3, rollback_point, tip]
			// respMsg[1] = rollback_point = [slotNo, hash]  (no blockNo)
			// respMsg[2] = tip            = [[slotNo, hash], blockNo]
			if len(respMsg) >= 3 {
				rollPt, _ := decodePoint(respMsg[1])
				_, tipBlockNo := decodePoint(respMsg[2])
				tipRaw := respMsg[2]

				// Guard against deep rollbacks that wipe significant chain state.
				//
				// When a new peer connects with an intersection at origin, it asks
				// us to roll back to genesis so it can replay the whole chain.
				// With 10 concurrent peers all doing this simultaneously, each one
				// wipes the headers built by the others, causing an infinite loop
				// of genesis re-syncs.
				//
				// Cardano's finality depth (k=2160 on mainnet, similar on preview)
				// guarantees that honest nodes never roll back more than ~2160 blocks.
				// Any rollback to genesis from a peer while our store is already well
				// ahead indicates a stale/slow peer.  Close this peer's session so
				// the peer manager can reconnect and find a proper intersection.
				currentTip, currentBlock := store.Tip()
				const deepRollbackThreshold = uint64(1000)
				const blockLeadThreshold = uint64(10) // peer must be >10 blocks ahead to justify genesis rollback
				if rollPt.IsOrigin() && currentTip.SlotNo > deepRollbackThreshold {
					if tipBlockNo > currentBlock+blockLeadThreshold {
						// Peer is significantly ahead → we're likely on an abandoned fork.
						// Accept rollback to resync on the canonical chain.
						log.Warn("chainsync client: accepting genesis rollback (fork recovery)",
							zap.Uint64("store_tip_slot", currentTip.SlotNo),
							zap.Uint64("store_block", currentBlock),
							zap.Uint64("peer_tip_block", tipBlockNo))
					} else {
						// Peer is at roughly the same height → concurrent genesis rollback.
						// Close session; reconnect will find intersection without wiping store.
						log.Warn("chainsync client: refusing genesis rollback, closing session",
							zap.Uint64("store_tip_slot", currentTip.SlotNo),
							zap.Uint64("peer_tip_block", tipBlockNo))
						return nil
					}
				}

				log.Info("chainsync client: rollback",
					zap.Uint64("to_slot", rollPt.SlotNo),
					zap.Uint64("tip_block_no", tipBlockNo))
				store.Rollback(rollPt, tipRaw, tipBlockNo)
				if pool != nil {
					pool.PruneTTL(rollPt.SlotNo)
				}
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

	rollbackCh := store.SubscribeRollback()
	defer store.UnsubscribeRollback(rollbackCh)

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
		case evt := <-rollbackCh:
			// A chain rollback occurred. Send MsgRollBackward to our downstream peer.
			// This tells them to discard headers beyond the rollback point.
			rollMsg, _ := cbor.Marshal([]interface{}{
				uint8(csTagRollBackward),
				encodePoint(evt.Point),
				evt.TipRaw,
			})
			if err := mc.Send(mux.ProtoChainSync, rollMsg); err != nil {
				return fmt.Errorf("chainsync server: send rollback: %w", err)
			}
			// Reset our position tracking to the rollback point.
			current := store.AllHeaders()
			headerQ = nil
			sentUpTo = len(current)
			pendingRequests = 0 // client will re-request after processing rollback
			log.Info("chainsync server: forwarded rollback", zap.Uint64("to_slot", evt.Point.SlotNo))
		case <-newHeaderCh:
			// Fetch newly added headers since we last checked.
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
