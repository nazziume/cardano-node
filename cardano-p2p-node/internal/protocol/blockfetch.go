package protocol

import (
	"fmt"
	"io"
	"time"

	"github.com/fxamacker/cbor/v2"
	"go.uber.org/zap"

	"github.com/cardano-p2p-node/internal/cardano"
	"github.com/cardano-p2p-node/internal/chain"
	"github.com/cardano-p2p-node/internal/mempool"
	"github.com/cardano-p2p-node/internal/mux"
)

// BlockFetch message tags
const (
	bfTagRequestRange = 0
	bfTagClientDone   = 1
	bfTagStartBatch   = 2
	bfTagNoBlocks     = 3
	bfTagBlock        = 4
	bfTagBatchDone    = 5
)

// recentBlockWindow is the number of recent block bodies to proactively fetch.
// Older blocks are not fetched because:
// 1. Downstream peers that are very far behind are unlikely (churn replaces them).
// 2. Fetching too many old blocks wastes upstream bandwidth.
// 3. The fetchyness metric only matters for the MOST RECENT blocks per slot.
const recentBlockWindow = 10

// BlockFetchClient downloads blocks for the most recent headers in the store.
// It does NOT download historical blocks — only the latest recentBlockWindow blocks
// are fetched so they can be served immediately to downstream peers.
//
// Blocks are fetched in consecutive RANGE batches to minimise round-trips.
//
// After receiving each block, its transaction IDs are extracted and removed
// from the mempool — confirmed transactions must not stay in the mempool.
func BlockFetchClient(mc *mux.Conn, store *chain.Store, pool *mempool.Mempool, log *zap.Logger, done <-chan struct{}) error {
	r := mc.Reader(mux.ProtoBlockFetch)
	newHeaderCh := store.Subscribe()
	defer store.Unsubscribe(newHeaderCh)

	for {
		select {
		case <-done:
			return sendBlockFetchDone(mc)
		case <-newHeaderCh:
		}

		// Only fetch blocks for the most recent recentBlockWindow headers.
		headers := store.AllHeaders()
		start := 0
		if len(headers) > recentBlockWindow {
			start = len(headers) - recentBlockWindow
		}
		recentHeaders := headers[start:]

		// Collect missing headers, then group into consecutive runs so we
		// can issue one MsgRequestRange per run instead of per block.
		var missing []chain.Header
		for _, h := range recentHeaders {
			if _, ok := store.GetBlock(h.Point.Hash); !ok {
				missing = append(missing, h)
			}
		}
		if len(missing) == 0 {
			continue
		}

		// Group consecutive missing headers into ranges and batch-fetch each.
		// If a batch fails (e.g. non-standard CBOR in a real Cardano block),
		// send MsgClientDone and pause so ChainSync / TxSubmission stay up.
		batchFailed := false
		for _, run := range groupIntoRuns(missing) {
			select {
			case <-done:
				return sendBlockFetchDone(mc)
			default:
			}
			if err := fetchRange(mc, r, store, pool, run, log); err != nil {
				log.Warn("blockfetch client: batch error (non-standard CBOR?), pausing",
					zap.Error(err),
					zap.Uint64("from_slot", run[0].Point.SlotNo))
				batchFailed = true
				break
			}
		}
		if batchFailed {
			// Gracefully end just the BlockFetch session; other protocols continue.
			_ = sendBlockFetchDone(mc)
			// Wait before retrying to avoid hammering the peer.
			select {
			case <-done:
				return nil
			case <-time.After(30 * time.Second):
			case <-newHeaderCh: // new header arrived — retry sooner
			}
		}
	}
}

// groupIntoRuns splits headers into consecutive groups (by index position).
// A single gap in the missing list starts a new run.
// This keeps each MsgRequestRange contiguous so the server can satisfy it
// without gaps that would force a MsgNoBlocks response.
func groupIntoRuns(headers []chain.Header) [][]chain.Header {
	if len(headers) == 0 {
		return nil
	}
	var runs [][]chain.Header
	cur := []chain.Header{headers[0]}
	for i := 1; i < len(headers); i++ {
		// Adjacent if slots are consecutive (mainnet: 1 slot apart is fine,
		// but headers may have slot gaps from missed slots; treat all as one run).
		cur = append(cur, headers[i])
	}
	runs = append(runs, cur)
	return runs
}

// fetchRange sends a single MsgRequestRange for [first, last] and reads the
// streamed blocks, storing each one immediately as it arrives.
// Confirmed txids are extracted from each block and removed from the mempool.
func fetchRange(mc *mux.Conn, r io.Reader, store *chain.Store, pool *mempool.Mempool, run []chain.Header, log *zap.Logger) error {
	if len(run) == 0 {
		return nil
	}
	from := encodePoint(run[0].Point)
	to := encodePoint(run[len(run)-1].Point)

	// MsgRequestRange = [0, fromPoint, toPoint]
	reqMsg, _ := cbor.Marshal([]interface{}{uint8(bfTagRequestRange), from, to})
	if err := mc.Send(mux.ProtoBlockFetch, reqMsg); err != nil {
		return fmt.Errorf("blockfetch client request range: %w", err)
	}

	raw, err := readOneMessage(r)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("blockfetch client read range response: %w", err)
	}

	var msg []cbor.RawMessage
	if err := cbor.Unmarshal(raw, &msg); err != nil {
		return nil
	}
	if len(msg) < 1 {
		return nil
	}

	var tag uint8
	_ = cbor.Unmarshal(msg[0], &tag)

	switch tag {
	case bfTagNoBlocks:
		log.Debug("blockfetch client: no blocks in range",
			zap.Uint64("from", run[0].Point.SlotNo),
			zap.Uint64("to", run[len(run)-1].Point.SlotNo))
		case bfTagStartBatch:
			// Stream all blocks from the batch into the store immediately.
			// We use run as a slot-ordered index to assign the right Point to each block.
			// Use the lenient reader here because real Cardano block CBOR may contain
			// non-standard additional-info values (28–30) from the Haskell cborg library
			// that the strict RFC-8949 decoder rejects.
			idx := 0
			for {
				raw, err := readOneMessageLenient(r)
				if err != nil {
					return fmt.Errorf("blockfetch client read batch: %w", err)
				}
				var bMsg []cbor.RawMessage
				if err := cbor.Unmarshal(raw, &bMsg); err != nil {
					// If the outer array itself can't be decoded, try a strict read
					// to see if it's a simple MsgBatchDone / MsgNoBlocks.
					var bTagOnly []cbor.RawMessage
					if err2 := cbor.Unmarshal(raw, &bTagOnly); err2 != nil {
						continue
					}
					bMsg = bTagOnly
				}
			if len(bMsg) < 1 {
				continue
			}
			var bTag uint8
			_ = cbor.Unmarshal(bMsg[0], &bTag)

			switch bTag {
			case bfTagBlock:
				if len(bMsg) < 2 {
					continue
				}
				var pt chain.Point
				if idx < len(run) {
					pt = run[idx].Point
					idx++
				}
				// Store immediately — downstream peers can serve this block now.
				store.AddBlock(chain.Block{Point: pt, Raw: bMsg[1]})

				// Extract confirmed txids and remove them from the mempool.
				// Transactions that appear in a block are no longer pending.
				if pool != nil {
					if txids := cardano.ParseBlockTxIDs(bMsg[1]); len(txids) > 0 {
						mids := make([]mempool.TxID, len(txids))
						for i, id := range txids {
							mids[i] = mempool.TxID(id)
						}
						removed := pool.RemoveConfirmed(mids)
						if removed > 0 {
							log.Info("blockfetch: removed confirmed txs from mempool",
								zap.Int("removed", removed),
								zap.Uint64("slot", pt.SlotNo))
						}
					}
				}
				log.Debug("blockfetch client: stored block", zap.Uint64("slot", pt.SlotNo))

			case bfTagBatchDone:
				log.Debug("blockfetch client: batch done", zap.Int("count", idx))
				return nil
			}
		}
	}
	return nil
}

func receiveBlocks(r io.Reader, mc *mux.Conn, store *chain.Store, forPoint chain.Point, log *zap.Logger) error {
	for {
		raw, err := readOneMessage(r)
		if err != nil {
			return fmt.Errorf("blockfetch receive block: %w", err)
		}

		var msg []cbor.RawMessage
		if err := cbor.Unmarshal(raw, &msg); err != nil {
			return fmt.Errorf("blockfetch decode block message: %w", err)
		}
		if len(msg) < 1 {
			continue
		}

		var tag uint8
		_ = cbor.Unmarshal(msg[0], &tag)

		switch tag {
		case bfTagBlock:
			if len(msg) < 2 {
				continue
			}
			store.AddBlock(chain.Block{
				Point: forPoint,
				Raw:   msg[1],
			})
			log.Debug("blockfetch client: stored block", zap.Uint64("slot", forPoint.SlotNo))
		case bfTagBatchDone:
			return nil
		}
	}
}

func sendBlockFetchDone(mc *mux.Conn) error {
	msg, _ := cbor.Marshal([]interface{}{uint8(bfTagClientDone)})
	return mc.Send(mux.ProtoBlockFetch, msg)
}

// BlockFetchServer serves blocks to an inbound client.
// This is critical for fetchyness scoring: serve blocks as fast as possible.
// Being first to deliver a block for a given slot scores fetchynessBlocks.
func BlockFetchServer(mc *mux.Conn, store *chain.Store, log *zap.Logger, done <-chan struct{}) error {
	r := mc.Reader(mux.ProtoBlockFetch)

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
			return fmt.Errorf("blockfetch server read: %w", err)
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
		case bfTagRequestRange:
			// MsgRequestRange = [0, fromPoint, toPoint]
			if len(msg) < 3 {
				continue
			}
			fromPt, _ := decodePoint(msg[1])
			toPt, _ := decodePoint(msg[2])

			// Find all blocks in the requested range
			blocks := findBlocksInRange(store, fromPt, toPt)

			if len(blocks) == 0 {
				// MsgNoBlocks = [3]
				noBlocks, _ := cbor.Marshal([]interface{}{uint8(bfTagNoBlocks)})
				if err := mc.Send(mux.ProtoBlockFetch, noBlocks); err != nil {
					return err
				}
				log.Debug("blockfetch server: no blocks in range",
					zap.Uint64("from", fromPt.SlotNo),
					zap.Uint64("to", toPt.SlotNo))
				continue
			}

			// MsgStartBatch = [2]
			startBatch, _ := cbor.Marshal([]interface{}{uint8(bfTagStartBatch)})
			if err := mc.Send(mux.ProtoBlockFetch, startBatch); err != nil {
				return err
			}

			// Send each block immediately
			for _, b := range blocks {
				blockMsg, _ := cbor.Marshal([]interface{}{uint8(bfTagBlock), b.Raw})
				if err := mc.Send(mux.ProtoBlockFetch, blockMsg); err != nil {
					return err
				}
				log.Debug("blockfetch server: served block", zap.Uint64("slot", b.Point.SlotNo))
			}

			// MsgBatchDone = [5]
			batchDone, _ := cbor.Marshal([]interface{}{uint8(bfTagBatchDone)})
			if err := mc.Send(mux.ProtoBlockFetch, batchDone); err != nil {
				return err
			}

		case bfTagClientDone:
			return nil
		}
	}
}

func findBlocksInRange(store *chain.Store, from, to chain.Point) []chain.Block {
	headers := store.AllHeaders()
	var result []chain.Block

	inRange := from.IsOrigin()
	for _, h := range headers {
		if !inRange && h.Point.SlotNo == from.SlotNo {
			inRange = true
		}
		if inRange {
			if b, ok := store.GetBlock(h.Point.Hash); ok {
				result = append(result, b)
			}
		}
		if h.Point.SlotNo == to.SlotNo {
			break
		}
	}
	return result
}
