package protocol

import (
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"
	"go.uber.org/zap"

	"github.com/cardano-p2p-node/internal/chain"
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

// BlockFetchClient downloads blocks for headers in the store.
// It connects to an upstream node and pulls blocks for recent headers.
func BlockFetchClient(mc *mux.Conn, store *chain.Store, log *zap.Logger, done <-chan struct{}) error {
	r := mc.Reader(mux.ProtoBlockFetch)
	newHeaderCh := store.Subscribe()
	defer store.Unsubscribe(newHeaderCh)

	for {
		select {
		case <-done:
			return sendBlockFetchDone(mc)
		case <-newHeaderCh:
		}

		// Get headers we don't have blocks for yet
		headers := store.AllHeaders()
		var toFetch []chain.Header
		for _, h := range headers {
			if _, ok := store.GetBlock(h.Point.Hash); !ok {
				toFetch = append(toFetch, h)
			}
		}
		if len(toFetch) == 0 {
			continue
		}

		// Fetch blocks for headers we're missing
		// Request one range at a time (from → to point)
		// For simplicity, fetch each block individually
		for _, h := range toFetch {
			select {
			case <-done:
				return sendBlockFetchDone(mc)
			default:
			}

			from := encodePoint(h.Point)
			to := encodePoint(h.Point)

			// MsgRequestRange = [0, fromPoint, toPoint]
			reqMsg, _ := cbor.Marshal([]interface{}{uint8(bfTagRequestRange), from, to})
			if err := mc.Send(mux.ProtoBlockFetch, reqMsg); err != nil {
				return fmt.Errorf("blockfetch client request: %w", err)
			}

			// Receive response
			raw, err := readOneMessage(r)
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("blockfetch client read response: %w", err)
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
			case bfTagNoBlocks:
				log.Debug("blockfetch client: no blocks for point", zap.Uint64("slot", h.Point.SlotNo))
				continue
			case bfTagStartBatch:
				// Receive blocks until BatchDone
				if err := receiveBlocks(r, mc, store, h.Point, log); err != nil {
					return err
				}
			default:
				log.Warn("blockfetch client: unexpected tag", zap.Uint8("tag", tag))
			}
		}
	}
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
