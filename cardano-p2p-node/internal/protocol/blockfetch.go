package protocol

import (
	"fmt"
	"io"

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
// Only the last N blocks matter for fetchyness scoring; older blocks are
// unlikely to be requested by downstream peers still in the hot peer set.
const recentBlockWindow = 10

// BlockFetchClient downloads blocks for the most recent headers in the store.
//
// Each missing block is requested INDIVIDUALLY (from==to in MsgRequestRange)
// rather than as a multi-block range.  Batching would be more efficient but
// introduces an index-mismatch bug: when some blocks in a range are already
// cached, the server still returns all blocks in the range, but the client's
// index only covers the missing subset — bodies end up stored under wrong Points.
//
// After receiving each block its transaction IDs are hashed and confirmed txs
// are removed from the mempool.
//
// On any protocol error the session is terminated with MsgClientDone.
// MsgClientDone is a TERMINAL state in the BlockFetch state machine — the
// server will not accept further requests on this connection, so BlockFetchClient
// returns nil (not an error) to keep the peer connection alive for ChainSync,
// TxSubmission and KeepAlive.
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

		headers := store.AllHeaders()
		start := 0
		if len(headers) > recentBlockWindow {
			start = len(headers) - recentBlockWindow
		}

		for _, h := range headers[start:] {
			select {
			case <-done:
				return sendBlockFetchDone(mc)
			default:
			}
			if _, ok := store.GetBlock(h.Point.Hash); ok {
				continue // already cached
			}
			if err := fetchOne(mc, r, store, pool, h, log); err != nil {
				log.Warn("blockfetch client: fetch error, ending session",
					zap.Error(err), zap.Uint64("slot", h.Point.SlotNo))
				_ = sendBlockFetchDone(mc)
				return nil // keep connection alive; only BlockFetch is done
			}
		}
	}
}

// fetchOne requests and receives a single block identified by its exact point.
// The server returns either MsgNoBlocks or MsgStartBatch → MsgBlock → MsgBatchDone.
// The lenient CBOR reader is used for MsgBlock bodies because real Cardano block
// CBOR may contain non-standard additional-info bytes (Haskell cborg extensions).
func fetchOne(mc *mux.Conn, r io.Reader, store *chain.Store, pool *mempool.Mempool, h chain.Header, log *zap.Logger) error {
	pt := encodePoint(h.Point)
	reqMsg, _ := cbor.Marshal([]interface{}{uint8(bfTagRequestRange), pt, pt})
	if err := mc.Send(mux.ProtoBlockFetch, reqMsg); err != nil {
		return fmt.Errorf("blockfetch request: %w", err)
	}

	raw, err := readOneMessage(r)
	if err != nil {
		return fmt.Errorf("blockfetch read response: %w", err)
	}
	var msg []cbor.RawMessage
	if err := cbor.Unmarshal(raw, &msg); err != nil || len(msg) < 1 {
		return fmt.Errorf("blockfetch decode response")
	}
	var tag uint8
	_ = cbor.Unmarshal(msg[0], &tag)

	switch tag {
	case bfTagNoBlocks:
		log.Debug("blockfetch: server has no block", zap.Uint64("slot", h.Point.SlotNo))
		return nil

	case bfTagStartBatch:
		for {
			raw, err := readOneMessageLenient(r)
			if err != nil {
				return fmt.Errorf("blockfetch read batch: %w", err)
			}
			var bMsg []cbor.RawMessage
			if err := cbor.Unmarshal(raw, &bMsg); err != nil || len(bMsg) < 1 {
				return fmt.Errorf("blockfetch decode batch message")
			}
			var bTag uint8
			_ = cbor.Unmarshal(bMsg[0], &bTag)

			switch bTag {
			case bfTagBlock:
				if len(bMsg) < 2 {
					continue
				}
				store.AddBlock(chain.Block{Point: h.Point, Raw: bMsg[1]})
				if pool != nil {
					if txids := cardano.ParseBlockTxIDs(bMsg[1]); len(txids) > 0 {
						mids := make([]mempool.TxID, len(txids))
						for i, id := range txids {
							mids[i] = mempool.TxID(id)
						}
						if removed := pool.RemoveConfirmed(mids); removed > 0 {
							log.Info("blockfetch: removed confirmed txs",
								zap.Int("count", removed),
								zap.Uint64("slot", h.Point.SlotNo))
						}
					}
				}
				log.Debug("blockfetch client: stored block", zap.Uint64("slot", h.Point.SlotNo))

			case bfTagBatchDone:
				return nil
			}
		}

	default:
		return fmt.Errorf("blockfetch: unexpected response tag %d for slot %d", tag, h.Point.SlotNo)
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
			if len(msg) < 3 {
				continue
			}
			fromPt, _ := decodePoint(msg[1])
			toPt, _ := decodePoint(msg[2])
			blocks := findBlocksInRange(store, fromPt, toPt)

			if len(blocks) == 0 {
				noBlocks, _ := cbor.Marshal([]interface{}{uint8(bfTagNoBlocks)})
				if err := mc.Send(mux.ProtoBlockFetch, noBlocks); err != nil {
					return err
				}
				log.Debug("blockfetch server: no blocks",
					zap.Uint64("from", fromPt.SlotNo), zap.Uint64("to", toPt.SlotNo))
				continue
			}

			startBatch, _ := cbor.Marshal([]interface{}{uint8(bfTagStartBatch)})
			if err := mc.Send(mux.ProtoBlockFetch, startBatch); err != nil {
				return err
			}
			for _, b := range blocks {
				blockMsg, _ := cbor.Marshal([]interface{}{uint8(bfTagBlock), b.Raw})
				if err := mc.Send(mux.ProtoBlockFetch, blockMsg); err != nil {
					return err
				}
				log.Debug("blockfetch server: served block", zap.Uint64("slot", b.Point.SlotNo))
			}
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
